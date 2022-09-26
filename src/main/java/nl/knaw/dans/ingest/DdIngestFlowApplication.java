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

import io.dropwizard.Application;
import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import nl.knaw.dans.easy.dd2d.DepositIngestTaskFactory;
import nl.knaw.dans.easy.dd2d.dansbag.DansBagValidator;
import nl.knaw.dans.ingest.core.AutoIngestArea;
import nl.knaw.dans.ingest.core.BlockedTarget;
import nl.knaw.dans.ingest.core.CsvMessageBodyWriter;
import nl.knaw.dans.ingest.core.ImportArea;
import nl.knaw.dans.ingest.core.TaskEvent;
import nl.knaw.dans.ingest.core.service.BlockedTargetService;
import nl.knaw.dans.ingest.core.service.BlockedTargetServiceImpl;
import nl.knaw.dans.ingest.db.BlockedTargetDAO;
import nl.knaw.dans.ingest.health.DansBagValidatorHealthCheck;
import nl.knaw.dans.ingest.health.DataverseHealthCheck;
import nl.knaw.dans.ingest.core.legacy.DepositIngestTaskFactoryWrapper;
import nl.knaw.dans.ingest.core.sequencing.TargetedTaskSequenceManager;
import nl.knaw.dans.ingest.core.service.EnqueuingService;
import nl.knaw.dans.ingest.core.service.EnqueuingServiceImpl;
import nl.knaw.dans.ingest.core.service.TaskEventService;
import nl.knaw.dans.ingest.core.service.TaskEventServiceImpl;
import nl.knaw.dans.ingest.db.TaskEventDAO;
import nl.knaw.dans.ingest.resources.BlockedTargetResource;
import nl.knaw.dans.ingest.resources.EventsResource;
import nl.knaw.dans.ingest.resources.ImportsResource;
import nl.knaw.dans.ingest.resources.MigrationsResource;
import nl.knaw.dans.lib.dataverse.DataverseClient;

import java.util.concurrent.ExecutorService;

public class DdIngestFlowApplication extends Application<DdIngestFlowConfiguration> {

    public static void main(final String[] args) throws Exception {
        new DdIngestFlowApplication().run(args);
    }

    private final HibernateBundle<DdIngestFlowConfiguration> hibernateBundle = new HibernateBundle<DdIngestFlowConfiguration>(TaskEvent.class, BlockedTarget.class) {

        @Override
        public PooledDataSourceFactory getDataSourceFactory(DdIngestFlowConfiguration configuration) {
            return configuration.getTaskEventDatabase();
        }
    };

    @Override
    public String getName() {
        return "DD Ingest Flow";
    }

    @Override
    public void initialize(final Bootstrap<DdIngestFlowConfiguration> bootstrap) {
        bootstrap.addBundle(hibernateBundle);
    }

    @Override
    public void run(final DdIngestFlowConfiguration configuration, final Environment environment) {
        final ExecutorService taskExecutor = configuration.getIngestFlow().getTaskQueue().build(environment);
        final TargetedTaskSequenceManager targetedTaskSequenceManager = new TargetedTaskSequenceManager(taskExecutor);
        final DataverseClient dataverseClient = configuration.getDataverse().build();

        final DansBagValidator validator =  new DansBagValidator(
            DepositIngestTaskFactory.appendSlash(configuration.getValidateDansBag().getBaseUrl()),
            configuration.getValidateDansBag().getPingUrl(),
            configuration.getValidateDansBag().getConnectionTimeoutMs(),
            configuration.getValidateDansBag().getReadTimeoutMs());
        final DepositIngestTaskFactoryWrapper ingestTaskFactoryWrapper = new DepositIngestTaskFactoryWrapper(
            false,
            configuration.getIngestFlow(),
            dataverseClient,
            configuration.getDataverseExtra(),
            validator);
        final DepositIngestTaskFactoryWrapper migrationTaskFactoryWrapper = new DepositIngestTaskFactoryWrapper(
            true,
            configuration.getIngestFlow(),
            dataverseClient,
            configuration.getDataverseExtra(),
            validator);

        final EnqueuingService enqueuingService = new EnqueuingServiceImpl(targetedTaskSequenceManager, 3 /* Must support importArea, migrationArea and autoIngestArea */);
        final TaskEventDAO taskEventDAO = new TaskEventDAO(hibernateBundle.getSessionFactory());
        final BlockedTargetDAO blockedTargetDAO = new BlockedTargetDAO(hibernateBundle.getSessionFactory());
        final TaskEventService taskEventService = new UnitOfWorkAwareProxyFactory(hibernateBundle).create(TaskEventServiceImpl.class, TaskEventDAO.class, taskEventDAO);
        final BlockedTargetService blockedTargetService = new UnitOfWorkAwareProxyFactory(hibernateBundle).create(BlockedTargetServiceImpl.class, BlockedTargetDAO.class, blockedTargetDAO);

        final ImportArea importArea = new ImportArea(
            configuration.getIngestFlow().getImportConfig().getInbox(),
            configuration.getIngestFlow().getImportConfig().getOutbox(),
            ingestTaskFactoryWrapper,
            migrationTaskFactoryWrapper, // Only necessary during migration. Can be phased out after that.
            taskEventService,
            blockedTargetService,
            enqueuingService);

        final ImportArea migrationArea = new ImportArea(
            configuration.getIngestFlow().getMigration().getInbox(),
            configuration.getIngestFlow().getMigration().getOutbox(),
            ingestTaskFactoryWrapper,
            migrationTaskFactoryWrapper, // Only necessary during migration. Can be phased out after that.
            taskEventService,
            blockedTargetService,
            enqueuingService);

        final AutoIngestArea autoIngestArea = new AutoIngestArea(
            configuration.getIngestFlow().getAutoIngest().getInbox(),
            configuration.getIngestFlow().getAutoIngest().getOutbox(),
            ingestTaskFactoryWrapper,
            taskEventService,
            blockedTargetService,
            enqueuingService
        );

        environment.healthChecks().register("Dataverse", new DataverseHealthCheck(dataverseClient));
        environment.healthChecks().register("DansBagValidator", new DansBagValidatorHealthCheck(validator));

        environment.lifecycle().manage(autoIngestArea);
        environment.jersey().register(new ImportsResource(importArea));
        environment.jersey().register(new MigrationsResource(migrationArea));
        environment.jersey().register(new EventsResource(taskEventDAO));
        environment.jersey().register(new BlockedTargetResource(blockedTargetDAO));
        environment.jersey().register(new CsvMessageBodyWriter());
    }
}
