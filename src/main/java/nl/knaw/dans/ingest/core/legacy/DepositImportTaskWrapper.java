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
package nl.knaw.dans.ingest.core.legacy;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.TaskEvent;
import nl.knaw.dans.ingest.core.sequencing.TargetedTask;
import nl.knaw.dans.ingest.core.service.DepositIngestTask;
import nl.knaw.dans.ingest.core.service.EventWriter;
import nl.knaw.dans.ingest.core.service.exception.RejectedDepositException;

import java.time.Instant;
import java.util.UUID;

@Slf4j
public class DepositImportTaskWrapper implements TargetedTask, Comparable<DepositImportTaskWrapper> {

    private final DepositIngestTask task;
    private final Instant created;
    private final EventWriter eventWriter;

    public DepositImportTaskWrapper(DepositIngestTask task, EventWriter eventWriter) {
        this.task = task;
        this.created = getCreatedInstant(task);
        this.eventWriter = eventWriter;
    }

    private static Instant getCreatedInstant(DepositIngestTask t) {
        return t.getDeposit().getBagCreated();
    }

    @Override
    public String getTarget() {
        return task.getDeposit().getDoi();
    }

    public UUID getDepositId() {
        return UUID.fromString(task.getDeposit().getDepositId());
    }

    @Override
    public void writeEvent(TaskEvent.EventType eventType, TaskEvent.Result result, String message) {
        eventWriter.write(getDepositId(), eventType, result, message);
    }

    @Override
    public void run() {
        writeEvent(TaskEvent.EventType.START_PROCESSING, TaskEvent.Result.OK, null);
        try {
            task.run();
            writeEvent(TaskEvent.EventType.END_PROCESSING, TaskEvent.Result.OK, null);
        }
        catch (RejectedDepositException e) {
            writeEvent(TaskEvent.EventType.END_PROCESSING, TaskEvent.Result.REJECTED, e.getMessage());
        }
        catch (Exception e) { // Not necessarily a FailedDepositException !
            writeEvent(TaskEvent.EventType.END_PROCESSING, TaskEvent.Result.FAILED, e.getMessage());
        }
    }

    @Override
    public int compareTo(DepositImportTaskWrapper o) {
        return created.compareTo(o.created);
    }

    @Override
    public String toString() {
        return "DepositImportTaskWrapper{" +
            "task=" + task +
            '}';
    }
}
