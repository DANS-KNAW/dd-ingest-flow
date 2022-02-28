package nl.knaw.dans.ingest.core;

import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingDeque;

public class InboxWatcher extends FileAlterationListenerAdaptor implements Iterator<File> {
    private final LinkedBlockingDeque<File> deque = new LinkedBlockingDeque<>();
    private final File inboxDir;

    private boolean initialized = false;
    private boolean initializationTriggered = false;
    private boolean keepWatching = true;

    public InboxWatcher(File inboxDir, FileFilter fileFilter, int intverval) throws Exception {
        this.inboxDir = inboxDir;
        FileAlterationObserver observer = new FileAlterationObserver(inboxDir, fileFilter);
        observer.addListener(this);
        FileAlterationMonitor monitor = new FileAlterationMonitor(intverval);
        monitor.addObserver(observer);
        monitor.start();
    }

    @Override
    public void onStart(FileAlterationObserver observer) {
        if (!initialized) {
            readBacklog();
            initialized = true;
            initializationTriggered = true;
        }
    }

    private void readBacklog() {
        Optional.ofNullable(inboxDir.listFiles((FileFilter) FileFilterUtils.directoryFileFilter())).ifPresent(
            files -> Arrays.stream(files).forEach(deque::add) // TODO: sort by Created timestamp
        );
    }

    @Override
    public void onDirectoryCreate(File file) {
        if (initializationTriggered) {
            initializationTriggered = false;
            return; // file already added to queue by readFromBackLog
        }
        deque.add(file);
    }

    @Override
    public boolean hasNext() {
        return keepWatching || deque.peekFirst() != null;
    }

    @Override
    public File next() {
        try {
            return deque.take();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
