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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Stream;

public abstract class AbstractDepositsImportTaskIterator implements Iterator<DepositIngestTask> {
    private static final Logger log = LoggerFactory.getLogger(AbstractDepositsImportTaskIterator.class);
    private final LinkedBlockingDeque<DepositIngestTask> deque = new LinkedBlockingDeque<>();
    private final Path inboxDir;
    private final Path outBox;
    private final DepositIngestTaskFactory taskFactory;
    private final EventWriter eventWriter;

    public AbstractDepositsImportTaskIterator(
        Path inboxDir, Path outBox, DepositIngestTaskFactory taskFactory, EventWriter eventWriter) {
        this.inboxDir = inboxDir;
        this.outBox = outBox;
        this.taskFactory = taskFactory;
        this.eventWriter = eventWriter;
    }

    protected boolean readAllDepositsFromInbox() {
        try (Stream<Path> paths = Files.list(inboxDir)) {
            paths.map(d -> taskFactory.createIngestTask(d, outBox, eventWriter))
                .sorted().forEach(deque::add);
            return !deque.isEmpty();
        }
        catch (IOException e) {
            throw new IllegalStateException("Could not read deposits from inbox", e);
        }
    }

    protected void addTaskForDeposit(Path dir) {
        deque.add(taskFactory.createIngestTask(dir, outBox, eventWriter));
    }

    @Override
    public boolean hasNext() {
        return deque.peekFirst() != null;
    }

    @Override
    public DepositIngestTask next() {
        try {
            return deque.take();
        }
        catch (InterruptedException e) {
            log.warn("Deque threw error", e);
        }
        return null;
    }
}
