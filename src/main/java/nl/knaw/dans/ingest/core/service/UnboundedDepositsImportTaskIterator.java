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

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;

public class UnboundedDepositsImportTaskIterator extends AbstractDepositsImportTaskIterator {
    private static final Logger log = LoggerFactory.getLogger(UnboundedDepositsImportTaskIterator.class);
    private boolean initialized = false;
    private boolean depositsReadInInitialization = false;
    private boolean keepRunning = true;

    public UnboundedDepositsImportTaskIterator(Path inboxDir, Path outBox, int pollingInterval, DepositIngestTaskFactory taskFactory, EventWriter eventWriter) {
        super(inboxDir, outBox, taskFactory, eventWriter);
        var observer = new FileAlterationObserver(inboxDir.toFile(), f -> f.isDirectory() && f.getParentFile().equals(inboxDir.toFile()));
        observer.addListener(new EventHandler());
        var monitor = new FileAlterationMonitor(pollingInterval);
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
        // Assuming that eventually a new item will arrive, unless we are explicitly stopping watching the inbox
        return keepRunning;
    }

    public void stop() {
        keepRunning = false;
    }

    private class EventHandler extends FileAlterationListenerAdaptor {
        @Override
        public void onStart(FileAlterationObserver observer) {
            log.trace("onStart called");
            if (!initialized) {
                initialized = true;
                depositsReadInInitialization = readAllDepositsFromInbox();
            }
        }

        @Override
        public void onDirectoryCreate(File file) {
            // TODO bug: this will not trigger if a new file appears in the inbox and
            // the initial state also had one or more deposits in it
            // to reproduce: put deposit in inbox, start process and then add another deposit to inbox
            // possible solution: just remove this logic, it doesnt seem to be triggered on initial start anyway?
            log.trace("onDirectoryCreate: {}", file);
            if (depositsReadInInitialization) {
                depositsReadInInitialization = false;
                return; // file already added to queue by onStart
            }
            addTaskForDeposit(file.toPath());
        }
    }
}
