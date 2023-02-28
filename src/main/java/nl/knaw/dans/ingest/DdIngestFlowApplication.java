/*
 * Copyright (C) 2022 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.knaw.dans.ingest;

import gov.loc.repository.bagit.reader.BagReader;
import io.dropwizard.Application;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.forms.MultiPartBundle;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import nl.knaw.dans.ingest.core.AutoIngestArea;
import nl.knaw.dans.ingest.core.BlockedTarget;
import nl.knaw.dans.ingest.core.CsvMessageBodyWriter;
import nl.knaw.dans.ingest.core.ImportArea;
import nl.knaw.dans.ingest.core.TaskEvent;
import nl.knaw.dans.ingest.core.config.IngestAreaConfig;
import nl.knaw.dans.ingest.core.deposit.BagDirResolverImpl;
import nl.knaw.dans.ingest.core.deposit.DepositLocationReaderImpl;
import nl.knaw.dans.ingest.core.deposit.DepositManagerImpl;
import nl.knaw.dans.ingest.core.deposit.DepositReaderImpl;
import nl.knaw.dans.ingest.core.deposit.DepositWriterImpl;
import nl.knaw.dans.ingest.core.io.BagDataManagerImpl;
import nl.knaw.dans.ingest.core.io.FileServiceImpl;
import nl.knaw.dans.ingest.core.sequencing.TargetedTaskSequenceManager;
import nl.knaw.dans.ingest.core.service.BlockedTargetService;
import nl.knaw.dans.ingest.core.service.BlockedTargetServiceImpl;
import nl.knaw.dans.ingest.core.service.DansBagValidator;
import nl.knaw.dans.ingest.core.service.DansBagValidatorImpl;
import nl.knaw.dans.ingest.core.service.DepositIngestTaskFactoryBuilder;
import nl.knaw.dans.ingest.core.service.EnqueuingService;
import nl.knaw.dans.ingest.core.service.EnqueuingServiceImpl;
import nl.knaw.dans.ingest.core.service.TaskEventService;
import nl.knaw.dans.ingest.core.service.TaskEventServiceImpl;
import nl.knaw.dans.ingest.core.service.XmlReaderImpl;
import nl.knaw.dans.ingest.core.service.ZipFileHandler;
import nl.knaw.dans.ingest.core.service.mapper.DepositToDvDatasetMetadataMapperFactory;
import nl.knaw.dans.ingest.db.BlockedTargetDAO;
import nl.knaw.dans.ingest.db.TaskEventDAO;
import nl.knaw.dans.ingest.health.DansBagValidatorHealthCheck;
import nl.knaw.dans.ingest.health.DataverseHealthCheck;
import nl.knaw.dans.ingest.resources.BlockedTargetsResource;
import nl.knaw.dans.ingest.resources.EventsResource;
import nl.knaw.dans.ingest.resources.ImportsResource;
import nl.knaw.dans.ingest.resources.MigrationsResource;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import java.io.IOException;
import java.net.URISyntaxException;

public class DdIngestFlowApplication extends Application<DdIngestFlowConfiguration> {

    private final HibernateBundle<DdIngestFlowConfiguration> hibernateBundle = new HibernateBundle<>(TaskEvent.class, BlockedTarget.class) {

        @Override
        public PooledDataSourceFactory getDataSourceFactory(DdIngestFlowConfiguration configuration) {
            return configuration.getTaskEventDatabase();
        }
    };

    public static void main(final String[] args) throws Exception {
        new DdIngestFlowApplication().run(args);
    }

    @Override
    public String getName() {
        return "DD Ingest Flow";
    }

    @Override
    public void initialize(final Bootstrap<DdIngestFlowConfiguration> bootstrap) {
        bootstrap.addBundle(hibernateBundle);
        bootstrap.addBundle(new MultiPartBundle());
    }

    @Override
    public void run(final DdIngestFlowConfiguration configuration, final Environment environment) throws IOException, URISyntaxException {
        final var taskExecutor = configuration.getIngestFlow().getTaskQueue().build(environment);
        final var targetedTaskSequenceManager = new TargetedTaskSequenceManager(taskExecutor);
        final var dataverseClient = configuration.getDataverse().build();

        IngestFlowConfigReader.readIngestFlowConfiguration(configuration.getIngestFlow());

        final var xmlReader = new XmlReaderImpl();

        final var fileService = new FileServiceImpl();

        // the parts responsible for reading and writing deposits to disk
        final var bagReader = new BagReader();
        final var bagDataManager = new BagDataManagerImpl(bagReader);
        final var bagDirResolver = new BagDirResolverImpl(fileService);
        final var depositReader = new DepositReaderImpl(xmlReader, bagDirResolver, fileService, bagDataManager);
        final var depositLocationReader = new DepositLocationReaderImpl(bagDirResolver, bagDataManager);
        final var depositWriter = new DepositWriterImpl(bagDataManager);
        final var depositManager = new DepositManagerImpl(depositReader, depositLocationReader, depositWriter);

        final var depositToDvDatasetMetadataMapperFactory = new DepositToDvDatasetMetadataMapperFactory(
            configuration.getIngestFlow().getIso1ToDataverseLanguage(),
            configuration.getIngestFlow().getIso2ToDataverseLanguage(),
            configuration.getIngestFlow().getSpatialCoverageCountryTerms(),
            dataverseClient
        );

        final var zipFileHandler = new ZipFileHandler(configuration.getIngestFlow().getZipWrappingTempDir());

        final var dansBagValidatorClient = new JerseyClientBuilder(environment)
            .withProvider(MultiPartFeature.class)
            .using(configuration.getValidateDansBag().getHttpClient())
            .build(getName());

        final DansBagValidator validator = new DansBagValidatorImpl(
            dansBagValidatorClient,
            configuration.getValidateDansBag().getBaseUrl(),
            configuration.getValidateDansBag().getPingUrl());

        final BlockedTargetDAO blockedTargetDAO = new BlockedTargetDAO(hibernateBundle.getSessionFactory());
        final BlockedTargetService blockedTargetService = new UnitOfWorkAwareProxyFactory(hibernateBundle)
            .create(BlockedTargetServiceImpl.class, BlockedTargetDAO.class, blockedTargetDAO);

        final DepositIngestTaskFactoryBuilder builder = new DepositIngestTaskFactoryBuilder(
            dataverseClient,
            validator,
            configuration.getIngestFlow(),
            configuration.getDataverseExtra(),
            depositManager,
            depositToDvDatasetMetadataMapperFactory,
            zipFileHandler,
            blockedTargetService,
            depositReader
        );

        final EnqueuingService enqueuingService = new EnqueuingServiceImpl(targetedTaskSequenceManager, 3 /* Must support importArea, migrationArea and autoIngestArea */);
        final TaskEventDAO taskEventDAO = new TaskEventDAO(hibernateBundle.getSessionFactory());
        final TaskEventService taskEventService = new UnitOfWorkAwareProxyFactory(hibernateBundle).create(TaskEventServiceImpl.class, TaskEventDAO.class, taskEventDAO);

        final IngestAreaConfig importConfig = configuration.getIngestFlow().getImportConfig();
        final ImportArea importArea = new ImportArea(
            importConfig.getInbox(),
            importConfig.getOutbox(),
            builder.createTaskFactory(false, importConfig.getDepositorRole()),
            taskEventService,
            enqueuingService);

        // Can be phased out after migration.
        final IngestAreaConfig migrationConfig = configuration.getIngestFlow().getMigration();
        final ImportArea migrationArea = new ImportArea(
            migrationConfig.getInbox(),
            migrationConfig.getOutbox(),
            builder.createTaskFactory(true, migrationConfig.getDepositorRole()),
            taskEventService,
            enqueuingService);

        final IngestAreaConfig autoIngestConfig = configuration.getIngestFlow().getAutoIngest();
        final AutoIngestArea autoIngestArea = new AutoIngestArea(
            autoIngestConfig.getInbox(),
            autoIngestConfig.getOutbox(),
            builder.createTaskFactory(false, autoIngestConfig.getDepositorRole()),
            taskEventService,
            enqueuingService
        );

        environment.healthChecks().register("Dataverse", new DataverseHealthCheck(dataverseClient));
        environment.healthChecks().register("DansBagValidator", new DansBagValidatorHealthCheck(validator));

        environment.lifecycle().manage(autoIngestArea);
        environment.jersey().register(new ImportsResource(importArea));
        environment.jersey().register(new MigrationsResource(migrationArea));
        environment.jersey().register(new EventsResource(taskEventDAO));
        environment.jersey().register(new BlockedTargetsResource(blockedTargetService));
        environment.jersey().register(new CsvMessageBodyWriter());
    }

}
