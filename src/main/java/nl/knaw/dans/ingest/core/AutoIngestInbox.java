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
package nl.knaw.dans.ingest.core;

import io.dropwizard.lifecycle.Managed;
import nl.knaw.dans.ingest.core.legacy.DepositImportTaskWrapper;
import nl.knaw.dans.ingest.core.legacy.DepositIngestTaskFactoryWrapper;
import nl.knaw.dans.ingest.core.service.ContinuousTargettedTaskSource;
import nl.knaw.dans.ingest.core.service.EnqueuingService;
import nl.knaw.dans.ingest.core.service.TargettedTaskSource;
import nl.knaw.dans.ingest.core.service.TargettedTaskSourceImpl;
import nl.knaw.dans.ingest.core.service.TaskEventService;

import java.nio.file.Path;

public class AutoIngestInbox extends AbstractInbox implements Managed {
    private ContinuousTargettedTaskSource taskSource;

    public AutoIngestInbox(Path inboxDir, Path outboxDir, DepositIngestTaskFactoryWrapper taskFactory,
        TaskEventService taskEventService, EnqueuingService enqueuingService) {
        super(inboxDir, outboxDir, taskFactory, taskEventService, enqueuingService);
    }

    @Override
    public void start() throws Exception {
        validateInDir(inboxDir);
        initOutbox(outboxDir, true);
        TargettedTaskSource<DepositImportTaskWrapper> taskSource = new ContinuousTargettedTaskSource("auto-ingest", inboxDir, outboxDir, taskEventService, taskFactory);
        enqueuingService.executeEnqueue(taskSource);
    }

    @Override
    public void stop() throws Exception {
        taskSource.stopWatching();
    }
}
