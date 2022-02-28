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

import nl.knaw.dans.ingest.core.legacy.DepositImportTaskWrapper;
import nl.knaw.dans.ingest.core.legacy.DepositIngestTaskFactoryWrapper;
import nl.knaw.dans.ingest.core.service.TargettedTaskSource;
import nl.knaw.dans.ingest.core.service.ImportTargettedTaskSource;
import nl.knaw.dans.ingest.core.service.EnqueuingService;
import nl.knaw.dans.ingest.core.service.TaskEventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ImportInbox {
    private static final Logger log = LoggerFactory.getLogger(ImportInbox.class);
    private final Path inboxDir;
    private final Path outboxDir;
    private final DepositIngestTaskFactoryWrapper taskFactory;
    private final TaskEventService taskEventService;
    private final EnqueuingService enqueuingService;

    public ImportInbox(Path inboxDir, Path outboxDir, DepositIngestTaskFactoryWrapper taskFactory, TaskEventService taskEventService, EnqueuingService enqueuingService) {
        this.inboxDir = inboxDir.toAbsolutePath();
        this.outboxDir = outboxDir.toAbsolutePath();
        this.taskFactory = taskFactory;
        this.taskEventService = taskEventService;
        this.enqueuingService = enqueuingService;
    }

    public void importBatch(Path batchPath, boolean continuePrevious) {
        Path relativeBatchDir;
        if (batchPath.isAbsolute()) {
            relativeBatchDir = inboxDir.relativize(batchPath);
            if (relativeBatchDir.startsWith(Paths.get(".."))) {
                throw new IllegalArgumentException("Batch directory must be subdirectory of" + inboxDir
                    + ". Provide correct absolute path or a path relative to this directory");
            }
        }
        else {
            relativeBatchDir = batchPath;
        }
        Path inDir = inboxDir.resolve(relativeBatchDir);
        Path outDir = outboxDir.resolve(relativeBatchDir);

        validateBatchDir(inDir);
        initOutbox(outDir, continuePrevious);
        TargettedTaskSource<DepositImportTaskWrapper> taskSource = new ImportTargettedTaskSource(relativeBatchDir.toString(), inDir, outDir, taskEventService, taskFactory);
        enqueuingService.executeEnqueue(taskSource);
    }

    private void validateBatchDir(Path batchDir) {
        if (Files.isRegularFile(batchDir))
            throw new IllegalArgumentException("Batch directory is a regular file: " + batchDir);
        if (!Files.exists(batchDir))
            throw new IllegalArgumentException("Batch directory does not exist: " + batchDir);
    }

    private void initOutbox(Path outbox, boolean continuePrevious) {
        try {
            Path processedDir = outbox.resolve("processed");
            Path failedDir = outbox.resolve("failed");
            Path rejectedDir = outbox.resolve("rejected");

            Files.createDirectories(outbox);
            Files.createDirectories(processedDir);
            Files.createDirectories(failedDir);
            Files.createDirectories(rejectedDir);

            if (!continuePrevious && (nonEmpty(processedDir) || nonEmpty(failedDir) || nonEmpty(rejectedDir))) {
                throw new IllegalArgumentException("outbox is not empty; start with empty outbox at " + outbox + ", or use the 'continue' option");
            }
        }
        catch (IOException e) {
            throw new IllegalArgumentException("cannot initialize outbox for batch at " + outbox, e);
        }
    }

    private boolean nonEmpty(Path p) throws IOException {
        return Files.list(p).findAny().isPresent();
    }
}
