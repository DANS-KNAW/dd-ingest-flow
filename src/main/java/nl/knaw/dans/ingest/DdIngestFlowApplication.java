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
import nl.knaw.dans.ingest.core.config.IngestFlowConfig;
import nl.knaw.dans.ingest.core.dataverse.DatasetService;
import nl.knaw.dans.ingest.core.dataverse.DataverseServiceImpl;
import nl.knaw.dans.ingest.core.deposit.BagDirResolverImpl;
import nl.knaw.dans.ingest.core.deposit.DepositFileListerImpl;
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
import nl.knaw.dans.ingest.core.validation.DepositorAuthorizationValidator;
import nl.knaw.dans.ingest.core.validation.DepositorAuthorizationValidatorImpl;
import nl.knaw.dans.ingest.db.BlockedTargetDAO;
import nl.knaw.dans.ingest.db.TaskEventDAO;
import nl.knaw.dans.ingest.health.DansBagValidatorHealthCheck;
import nl.knaw.dans.ingest.health.DataverseHealthCheck;
import nl.knaw.dans.ingest.resources.BlockedTargetsResource;
import nl.knaw.dans.ingest.resources.EventsResource;
import nl.knaw.dans.ingest.resources.ImportsResource;
import nl.knaw.dans.ingest.resources.MigrationsResource;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.util.DataverseClientFactory;
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
        IngestFlowConfig ingestFlowConfig = configuration.getIngestFlow();
        final var taskExecutor = ingestFlowConfig.getTaskQueue().build(environment);
        final var targetedTaskSequenceManager = new TargetedTaskSequenceManager(taskExecutor);

        IngestFlowConfigReader.readIngestFlowConfiguration(ingestFlowConfig);

        final var xmlReader = new XmlReaderImpl();

        final var fileService = new FileServiceImpl();
        final var depositFileLister = new DepositFileListerImpl();

        // the parts responsible for reading and writing deposits to disk
        final var bagReader = new BagReader();
        final var bagDataManager = new BagDataManagerImpl(bagReader);
        final var bagDirResolver = new BagDirResolverImpl(fileService);
        final var depositReader = new DepositReaderImpl(xmlReader, bagDirResolver, fileService, bagDataManager, depositFileLister);
        final var depositLocationReader = new DepositLocationReaderImpl(bagDirResolver, bagDataManager);
        final var depositWriter = new DepositWriterImpl(bagDataManager);
        final var depositManager = new DepositManagerImpl(depositReader, depositLocationReader, depositWriter);

        final var zipFileHandler = new ZipFileHandler(ingestFlowConfig.getZipWrappingTempDir());

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

        // validate depositors
        final EnqueuingService enqueuingService = new EnqueuingServiceImpl(targetedTaskSequenceManager, 3 /* Must support importArea, migrationArea and autoIngestArea */);
        final TaskEventDAO taskEventDAO = new TaskEventDAO(hibernateBundle.getSessionFactory());
        final TaskEventService taskEventService = new UnitOfWorkAwareProxyFactory(hibernateBundle).create(TaskEventServiceImpl.class, TaskEventDAO.class, taskEventDAO);

        final var importAreaConfig = ingestFlowConfig.getImportConfig();
        final var migrationAreaConfig = ingestFlowConfig.getMigration();
        final var autoIngestAreaConfig = ingestFlowConfig.getAutoIngest();

        final var dataverseClientImportArea = getDataverseClient(configuration, autoIngestAreaConfig);
        final var dataverseClientMigrationArea = getDataverseClient(configuration, autoIngestAreaConfig);
        final var dataverseClientAutoIngestArea = getDataverseClient(configuration, autoIngestAreaConfig);

        final ImportArea importArea = new ImportArea(
            importAreaConfig.getInbox(),
            importAreaConfig.getOutbox(),
            new DepositIngestTaskFactoryBuilder(
                validator,
                ingestFlowConfig,
                depositManager,
                getMetadataMapperFactory(configuration, dataverseClientImportArea),
                zipFileHandler,
                getDataverseService(configuration, dataverseClientImportArea),
                blockedTargetService
            ).createTaskFactory(
                false,
                importAreaConfig.getDepositorRole(),
                getDepositorAuthorizationValidator(configuration, importAreaConfig, dataverseClientImportArea)
            ),
            taskEventService,
            enqueuingService);

        // Can be phased out after migration.
        final ImportArea migrationArea = new ImportArea(
            migrationAreaConfig.getInbox(),
            migrationAreaConfig.getOutbox(),
            new DepositIngestTaskFactoryBuilder(
                validator,
                ingestFlowConfig,
                depositManager,
                getMetadataMapperFactory(configuration, dataverseClientMigrationArea),
                zipFileHandler,
                getDataverseService(configuration, dataverseClientMigrationArea),
                blockedTargetService
            ).createTaskFactory(
                true,
                migrationAreaConfig.getDepositorRole(),
                getDepositorAuthorizationValidator(configuration, migrationAreaConfig, dataverseClientMigrationArea)
            ),
            taskEventService,
            enqueuingService);

        final AutoIngestArea autoIngestArea = new AutoIngestArea(
            autoIngestAreaConfig.getInbox(),
            autoIngestAreaConfig.getOutbox(),
            new DepositIngestTaskFactoryBuilder(
                validator,
                ingestFlowConfig,
                depositManager,
                getMetadataMapperFactory(configuration, dataverseClientAutoIngestArea),
                zipFileHandler,
                getDataverseService(configuration, dataverseClientAutoIngestArea),
                blockedTargetService
            ).createTaskFactory(
                false,
                autoIngestAreaConfig.getDepositorRole(),
                getDepositorAuthorizationValidator(configuration, autoIngestAreaConfig, dataverseClientAutoIngestArea)
            ),
            taskEventService,
            enqueuingService
        );

        environment.healthChecks().register("DataverseAutoIngestArea", new DataverseHealthCheck(dataverseClientAutoIngestArea));
        environment.healthChecks().register("DataverseMigrationArea", new DataverseHealthCheck(dataverseClientMigrationArea));
        environment.healthChecks().register("DataverseImportArea", new DataverseHealthCheck(dataverseClientImportArea));
        environment.healthChecks().register("DansBagValidator", new DansBagValidatorHealthCheck(validator));

        environment.lifecycle().manage(autoIngestArea);
        environment.jersey().register(new ImportsResource(importArea));
        environment.jersey().register(new MigrationsResource(migrationArea));
        environment.jersey().register(new EventsResource(taskEventDAO));
        environment.jersey().register(new BlockedTargetsResource(blockedTargetService));
        environment.jersey().register(new CsvMessageBodyWriter());
    }

    private static DepositToDvDatasetMetadataMapperFactory getMetadataMapperFactory(DdIngestFlowConfiguration configuration, DataverseClient dataverseClient) {
        final var ingestFlowConfig = configuration.getIngestFlow();
        return new DepositToDvDatasetMetadataMapperFactory(
            ingestFlowConfig.getIso1ToDataverseLanguage(),
            ingestFlowConfig.getIso2ToDataverseLanguage(),
            ingestFlowConfig.getSpatialCoverageCountryTerms(),
            dataverseClient
        );
    }

    private DepositorAuthorizationValidator getDepositorAuthorizationValidator(DdIngestFlowConfiguration configuration, IngestAreaConfig autoIngestAreaConfig, DataverseClient dataverseClient) {
        return buildDepositorAuthorizationValidator(
            getDataverseService(configuration, dataverseClient),
            configuration.getIngestFlow(),
            autoIngestAreaConfig);
    }

    private static DataverseServiceImpl getDataverseService(DdIngestFlowConfiguration configuration, DataverseClient dataverseClient) {
        return new DataverseServiceImpl(
            dataverseClient,
            configuration.getDataverseExtra().getPublishAwaitUnlockWaitTimeMs(),
            configuration.getDataverseExtra().getPublishAwaitUnlockMaxRetries()
        );
    }

    private static DataverseClient getDataverseClient(DdIngestFlowConfiguration configuration, IngestAreaConfig ingestAreaConfig) {
        DataverseClientFactory dataverseClientFactory = configuration.getDataverse();
        dataverseClientFactory.setApiKey(getOverridable(ingestAreaConfig.getApiKey(), configuration.getDataverse().getApiKey()));
        return dataverseClientFactory.build();
    }

    DepositorAuthorizationValidator buildDepositorAuthorizationValidator(DatasetService datasetService, IngestFlowConfig ingestFlowConfig, IngestAreaConfig ingestAreaConfig) {
        var creator = getDatasetCreatorRole(ingestFlowConfig, ingestAreaConfig);
        var updater = getDatasetUpdaterRole(ingestFlowConfig, ingestAreaConfig);

        return new DepositorAuthorizationValidatorImpl(datasetService, creator, updater);
    }

    private static String getOverridable(String value, String defaultValue) {
        // TODO like dataset-creator/updater-role this is another approach that for depositorRole,
        //  which convention is preferred?
        if (value != null) {
            return value;
        }

        return defaultValue;
    }

    private static String getDatasetCreatorRole(IngestFlowConfig ingestFlowConfig, IngestAreaConfig ingestAreaConfig) {
        if (ingestAreaConfig.getAuthorization() != null && ingestAreaConfig.getAuthorization().getDatasetCreator() != null) {
            return ingestAreaConfig.getAuthorization().getDatasetCreator();
        }

        return ingestFlowConfig.getAuthorization().getDatasetCreator();
    }

    String getDatasetUpdaterRole(IngestFlowConfig ingestFlowConfig, IngestAreaConfig ingestAreaConfig) {
        if (ingestAreaConfig.getAuthorization() != null && ingestAreaConfig.getAuthorization().getDatasetUpdater() != null) {
            return ingestAreaConfig.getAuthorization().getDatasetUpdater();
        }

        return ingestFlowConfig.getAuthorization().getDatasetUpdater();
    }
}
