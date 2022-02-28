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

import nl.knaw.dans.ingest.core.legacy.DepositImportTaskWrapper;
import nl.knaw.dans.ingest.core.legacy.DepositIngestTaskFactoryWrapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;

public class DepositsImportTaskIterator implements Iterator<DepositImportTaskWrapper>  {
    private final LinkedBlockingDeque<DepositImportTaskWrapper> deque = new LinkedBlockingDeque<>();
    private final Path inboxDir;
    private final Path outBox;
    private final DepositIngestTaskFactoryWrapper taskFactory;
    private final EventWriter eventWriter;

    public DepositsImportTaskIterator(
        Path inboxDir, Path outBox, DepositIngestTaskFactoryWrapper taskFactory, EventWriter eventWriter) {
        this.inboxDir = inboxDir;
        this.outBox = outBox;
        this.taskFactory = taskFactory;
        this.eventWriter = eventWriter;
    }

    protected void readAddDepositsFromInbox() {
        try {
            Files.list(inboxDir)
                .map(d -> taskFactory.createIngestTask(d, outBox, eventWriter))
                .sorted().forEach(deque::add);
        }
        catch (IOException e) {
            e.printStackTrace();
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
    public DepositImportTaskWrapper next() {
        try {
            return deque.take();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

}
