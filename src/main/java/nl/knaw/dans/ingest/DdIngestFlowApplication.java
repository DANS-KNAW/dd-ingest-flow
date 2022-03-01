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
import nl.knaw.dans.ingest.core.AutoIngestInbox;
import nl.knaw.dans.ingest.core.ImportInbox;
import nl.knaw.dans.ingest.core.TaskEvent;
import nl.knaw.dans.ingest.core.legacy.DepositIngestTaskFactoryWrapper;
import nl.knaw.dans.ingest.core.sequencing.TargettedTaskSequenceManager;
import nl.knaw.dans.ingest.core.service.EnqueuingService;
import nl.knaw.dans.ingest.core.service.EnqueuingServiceImpl;
import nl.knaw.dans.ingest.core.service.TaskEventService;
import nl.knaw.dans.ingest.core.service.TaskEventServiceImpl;
import nl.knaw.dans.ingest.db.TaskEventDAO;
import nl.knaw.dans.ingest.resources.ImportResource;

import java.util.concurrent.ExecutorService;

public class DdIngestFlowApplication extends Application<DdIngestFlowConfiguration> {

    public static void main(final String[] args) throws Exception {
        new DdIngestFlowApplication().run(args);
    }

    private final HibernateBundle<DdIngestFlowConfiguration> hibernateBundle = new HibernateBundle<DdIngestFlowConfiguration>(TaskEvent.class) {

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
        final TargettedTaskSequenceManager targettedTaskSequenceManager = new TargettedTaskSequenceManager(taskExecutor);
        final DepositIngestTaskFactoryWrapper ingestTaskFactoryWrapper = new DepositIngestTaskFactoryWrapper(
            false,
            configuration.getIngestFlow(),
            configuration.getDataverse(),
            configuration.getManagePrestaging(),
            configuration.getValidateDansBag());
        final DepositIngestTaskFactoryWrapper migrationTaskFactoryWrapper = new DepositIngestTaskFactoryWrapper(
            true,
            configuration.getIngestFlow(),
            configuration.getDataverse(),
            configuration.getManagePrestaging(),
            configuration.getValidateDansBag());

        final EnqueuingService enqueuingService = new EnqueuingServiceImpl(targettedTaskSequenceManager, 2 /* Must support both importInbox and autoIngestInbox */);
        final TaskEventDAO taskEventDAO = new TaskEventDAO(hibernateBundle.getSessionFactory());
        final TaskEventService taskEventService = new UnitOfWorkAwareProxyFactory(hibernateBundle).create(TaskEventServiceImpl.class, TaskEventDAO.class, taskEventDAO);

        final ImportInbox importInbox = new ImportInbox(
            configuration.getIngestFlow().getImportConfig().getInbox(),
            configuration.getIngestFlow().getImportConfig().getOutbox(),
            ingestTaskFactoryWrapper,
            migrationTaskFactoryWrapper, // Only necessary during migration. Can be phased out after that.
            taskEventService,
            enqueuingService);

        final AutoIngestInbox autoIngestInbox = new AutoIngestInbox(
            configuration.getIngestFlow().getAutoIngest().getInbox(),
            configuration.getIngestFlow().getAutoIngest().getOutbox(),
            ingestTaskFactoryWrapper,
            taskEventService,
            enqueuingService
        );

        environment.lifecycle().manage(autoIngestInbox);
        environment.jersey().register(new ImportResource(importInbox));
    }
}
