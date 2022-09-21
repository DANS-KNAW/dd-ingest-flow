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
import nl.knaw.dans.ingest.core.service.EnqueuingService;
import nl.knaw.dans.ingest.core.service.SingleDepositTargetedTaskSourceImpl;
import nl.knaw.dans.ingest.core.service.TargetedTaskSource;
import nl.knaw.dans.ingest.core.service.TargetedTaskSourceImpl;
import nl.knaw.dans.ingest.core.service.TaskEventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class ImportArea extends AbstractIngestArea {
    private static final Logger log = LoggerFactory.getLogger(ImportArea.class);
    private final DepositIngestTaskFactoryWrapper migrationTaskFactory;
    private final Map<String, TargetedTaskSource<DepositImportTaskWrapper>> batches = new HashMap<>();

    public ImportArea(Path inboxDir, Path outboxDir, DepositIngestTaskFactoryWrapper taskFactory, DepositIngestTaskFactoryWrapper migrationTaskFactory,
        TaskEventService taskEventService, EnqueuingService enqueuingService) {
        super(inboxDir, outboxDir, taskFactory, taskEventService, enqueuingService);
        this.migrationTaskFactory = migrationTaskFactory;
    }

    public String startImport(Path inputPath, boolean isBatch, boolean continuePrevious, boolean isMigration) {
        log.trace("startBatch({}, {}, {})", inputPath, continuePrevious, isMigration);
        Path relativeInputDir;
        if (inputPath.isAbsolute()) {
            relativeInputDir = inboxDir.relativize(inputPath);
            if (relativeInputDir.startsWith(Paths.get(".."))) {
                throw new IllegalArgumentException(
                    String.format("Input directory must be subdirectory of %s. Provide correct absolute path or a path relative to this directory.", inboxDir));
            }
        }
        else {
            relativeInputDir = inputPath;
        }

        Path batchInDir;
        Path batchOutDir;

        if (isBatch) {
            batchInDir = inboxDir.resolve(relativeInputDir);
            batchOutDir = outboxDir.resolve(relativeInputDir);
        }
        else {
            batchInDir = inboxDir.resolve(relativeInputDir.getParent());
            batchOutDir = outboxDir.resolve(relativeInputDir.getParent());
        }
        log.debug("relativeInputDir = {}, batchInDir = {}, batchOutDir = {}", relativeInputDir, batchInDir, batchOutDir);
        validateInDir(batchInDir);
        initOutbox(batchOutDir, continuePrevious || !isBatch);
        String taskName = relativeInputDir.toString();
        TargetedTaskSource<DepositImportTaskWrapper> taskSource;
        if (isBatch) {
            taskSource = new TargetedTaskSourceImpl(taskName, batchInDir, batchOutDir, taskEventService,
                isMigration ? migrationTaskFactory : taskFactory);
        }
        else {
            taskSource = new SingleDepositTargetedTaskSourceImpl(taskName, inboxDir.resolve(relativeInputDir), batchOutDir, taskEventService,
                isMigration ? migrationTaskFactory : taskFactory);
        }
        batches.put(taskName, taskSource);
        enqueuingService.executeEnqueue(taskSource);
        return relativeInputDir.toString();
    }

}
