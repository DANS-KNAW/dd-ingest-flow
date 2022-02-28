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

import nl.knaw.dans.ingest.core.legacy.DepositIngestTaskFactoryWrapper;
import nl.knaw.dans.ingest.core.service.EnqueuingService;
import nl.knaw.dans.ingest.core.service.TaskEventService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class AbstractInbox {
    protected final Path inboxDir;
    protected final Path outboxDir;
    protected final DepositIngestTaskFactoryWrapper taskFactory;
    protected final TaskEventService taskEventService;
    protected final EnqueuingService enqueuingService;

    public AbstractInbox(Path inboxDir, Path outboxDir,
        DepositIngestTaskFactoryWrapper taskFactory, TaskEventService taskEventService, EnqueuingService enqueuingService) {
        this.inboxDir = inboxDir.toAbsolutePath();
        this.outboxDir = outboxDir.toAbsolutePath();
        this.taskFactory = taskFactory;
        this.taskEventService = taskEventService;
        this.enqueuingService = enqueuingService;
    }

    protected static boolean nonEmpty(Path p) throws IOException {
        return Files.list(p).findAny().isPresent();
    }

    protected void validateInDir(Path inDir) {
        if (Files.isRegularFile(inDir))
            throw new IllegalArgumentException("Input directory is a regular file: " + inDir);
        if (!Files.exists(inDir))
            throw new IllegalArgumentException("Input directory does not exist: " + inDir);
    }

    protected void initOutbox(Path outbox, boolean allowNonEmpty) {
        try {
            Path processedDir = outbox.resolve("processed");
            Path failedDir = outbox.resolve("failed");
            Path rejectedDir = outbox.resolve("rejected");

            Files.createDirectories(outbox);
            Files.createDirectories(processedDir);
            Files.createDirectories(failedDir);
            Files.createDirectories(rejectedDir);

            if (!allowNonEmpty && (AbstractInbox.nonEmpty(processedDir) || AbstractInbox.nonEmpty(failedDir) || AbstractInbox.nonEmpty(rejectedDir))) {
                throw new IllegalArgumentException("outbox is not empty; start with empty outbox at " + outbox + ", or use the 'continue' option");
            }
        }
        catch (IOException e) {
            throw new IllegalArgumentException("cannot initialize outbox for batch at " + outbox, e);
        }
    }
}
