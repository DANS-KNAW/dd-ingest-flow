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

import nl.knaw.dans.ingest.core.legacy.DepositIngestTaskFactoryWrapper;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;
import java.nio.file.Path;

public class ContinuousDepositsImportTaskIterator extends DepositsImportTaskIterator {
    private boolean initialized = false;
    private boolean initializationTriggered = false;
    private boolean keepWatching = true;

    private class EventHandler extends FileAlterationListenerAdaptor {
        @Override
        public void onStart(FileAlterationObserver observer) {
            if (!initialized) {
                readAddDepositsFromInbox();
                initialized = true;
                initializationTriggered = true;
            }
        }

        @Override
        public void onDirectoryCreate(File file) {
            if (initializationTriggered) {
                initializationTriggered = false;
                return; // file already added to queue by readFromBackLog
            }
            addTaskForDeposit(file.toPath());
        }
    }

    public ContinuousDepositsImportTaskIterator(Path inboxDir, Path outBox, int pollingInterval, DepositIngestTaskFactoryWrapper taskFactory, EventWriter eventWriter) {
        super(inboxDir, outBox, taskFactory, eventWriter);
        FileAlterationObserver observer = new FileAlterationObserver(inboxDir.toFile(), f -> f.isDirectory() && f.getParentFile().equals(inboxDir.toFile()));
        observer.addListener(new EventHandler());
        FileAlterationMonitor monitor = new FileAlterationMonitor(pollingInterval);
        monitor.addObserver(observer);
        try {
            monitor.start();
        }
        catch (Exception e) {
            throw new IllegalStateException(String.format("Could not start monitoring %s", inboxDir), e);
        }
    }

    @Override
    public boolean hasNext() {
        return keepWatching || super.hasNext();
    }

    public void stopWatching() {
        keepWatching = false;
    }
}
