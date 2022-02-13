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
package nl.knaw.dans.ingest.core.service;

import nl.knaw.dans.ingest.core.TaskEvent;
import nl.knaw.dans.ingest.core.legacy.DepositImportTaskWrapper;
import nl.knaw.dans.ingest.core.sequencing.TargettedTaskSequenceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EnqueuingServiceImpl implements EnqueuingService {
    private static final Logger log = LoggerFactory.getLogger(EnqueuingServiceImpl.class);

    private final ExecutorService enqueingExecutor = Executors.newSingleThreadExecutor();
    private final TargettedTaskSequenceManager targettedTaskSequenceManager;

    public EnqueuingServiceImpl(TargettedTaskSequenceManager targettedTaskSequenceManager) {
        this.targettedTaskSequenceManager = targettedTaskSequenceManager;
    }

    @Override
    public void executeEnqueue(Batch source) {
        enqueingExecutor.execute(() -> {
            source.getTasks().forEach(t -> enqueue(t, source.getEventWriter()));
        });
    }

    private void enqueue(DepositImportTaskWrapper w, EventWriter eventWriter) {
        log.trace("Enqueuing {}", w);
        try {
            targettedTaskSequenceManager.scheduleTask(w);
            eventWriter.write(w.getDepositId(), TaskEvent.EventType.ENQUEUE, TaskEvent.Result.OK, null);
        }
        catch (Exception e) {
            log.error("Enqueing of {} failed", w, e);
            eventWriter.write(w.getDepositId(), TaskEvent.EventType.ENQUEUE, TaskEvent.Result.FAILED, e.getMessage());
        }
    }
}
