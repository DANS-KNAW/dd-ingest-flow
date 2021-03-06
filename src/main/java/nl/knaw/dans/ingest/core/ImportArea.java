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

    public String startBatch(Path batchPath, boolean continuePrevious, boolean isMigration) {
        log.trace("startBatch({}, {}, {})", batchPath, continuePrevious, isMigration);
        Path relativeBatchDir;
        if (batchPath.isAbsolute()) {
            relativeBatchDir = inboxDir.relativize(batchPath);
            if (relativeBatchDir.startsWith(Paths.get(".."))) {
                throw new IllegalArgumentException(
                    String.format("Batch directory must be subdirectory of %s. Provide correct absolute path or a path relative to this directory.", inboxDir));
            }
        }
        else {
            relativeBatchDir = batchPath;
        }
        Path inDir = inboxDir.resolve(relativeBatchDir);
        Path outDir = outboxDir.resolve(relativeBatchDir);
        log.debug("inDir = {}, outDir = {}", inDir, outDir);
        validateInDir(inDir);
        initOutbox(outDir, continuePrevious);
        String batchName = relativeBatchDir.toString();
        TargetedTaskSource<DepositImportTaskWrapper> taskSource = new TargetedTaskSourceImpl(batchName, inDir, outDir, taskEventService,
            isMigration ? migrationTaskFactory : taskFactory);
        batches.put(batchName, taskSource);
        enqueuingService.executeEnqueue(taskSource);
        return  relativeBatchDir.toString();
    }
}
