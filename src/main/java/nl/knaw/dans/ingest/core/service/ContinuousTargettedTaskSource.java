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

import java.nio.file.Path;
import java.util.Iterator;

public class ContinuousTargettedTaskSource extends TargettedTaskSourceImpl{
    private ContinuousDepositsImportTaskIterator iterator;

    public ContinuousTargettedTaskSource(String name, Path inDir, Path outDir, TaskEventService taskEventService,
        DepositIngestTaskFactoryWrapper taskFactory) {
        super(name, inDir, outDir, taskEventService, taskFactory);
    }

    @Override
    protected Iterator<DepositImportTaskWrapper> createIterator(Path inDir, Path outDir, DepositIngestTaskFactoryWrapper taskFactory, EventWriter eventWriter) {
        iterator =  new ContinuousDepositsImportTaskIterator(inDir, outDir, 500, taskFactory, eventWriter);
        return iterator;
    }

    public void stopWatching() {
        iterator.stopWatching();
    }
}
