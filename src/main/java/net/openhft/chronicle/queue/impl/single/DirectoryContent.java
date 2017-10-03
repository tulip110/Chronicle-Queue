package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.ReferenceCounted;
import net.openhft.chronicle.core.ReferenceCounter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.ToIntFunction;

public final class DirectoryContent implements ReferenceCounted {
    private static final int RECORD_COUNT = 64;
    private static final int RECORD_SIZE = 64;
    private final Bytes bytes;
    private final ReferenceCounter rc;
    private final Path queuePath;
    private final ToIntFunction<File> fileToCycleFunction;

    DirectoryContent(final Path queuePath, final ToIntFunction<File> fileToCycleFunction) {
        this.queuePath = queuePath;
        this.fileToCycleFunction = fileToCycleFunction;
        final File file = queuePath.resolve("directory-contents.idx").toFile();
        try {
            Files.createDirectories(queuePath);
            file.createNewFile();
            final RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.setLength(RECORD_COUNT * RECORD_SIZE);
            raf.close();
            bytes = MappedBytes.mappedBytes(file, RECORD_SIZE);
            rc = ReferenceCounter.onReleased(bytes::release);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void refresh() {
        final File[] queueFiles = queuePath.toFile().listFiles(SingleChronicleQueue.QUEUE_FILE_FILTER);
        for (File queueFile : queueFiles) {
            onFileCreated(fileToCycleFunction.applyAsInt(queueFile));
        }
    }

    boolean containsFileForCycle(final int cycle) {
        for (int i = 0; i < RECORD_COUNT; i++) {
            if (cycle == bytes.readVolatileInt(i * RECORD_SIZE)) {
                return true;
            }
        }
        return false;
    }

    int getMostRecentRollCycle()
    {
        int maxRollCycle = 0;
        for (int i = 0; i < RECORD_COUNT; i++) {
            maxRollCycle = Math.max(maxRollCycle, bytes.readVolatileInt(i * RECORD_SIZE));
        }

        return maxRollCycle;
    }

    void onFileCreated(final int rollCycle)
    {
        final int index = rollCycle & (RECORD_COUNT - 1);
        while (true) {
            final int currentCycle = bytes.readVolatileInt(index * RECORD_SIZE);
            if (rollCycle > currentCycle) {
                if (bytes.compareAndSwapInt(index * RECORD_SIZE, currentCycle, rollCycle)) {
                    return;
                }
            } else {
                return;
            }
        }
    }

    void onFileDeleted(final int rollCycle)
    {
        final int index = rollCycle & (RECORD_COUNT - 1);
        while (true) {
            final int currentCycle = bytes.readVolatileInt(index * RECORD_SIZE);
            if (rollCycle == currentCycle) {
                if (bytes.compareAndSwapInt(index * RECORD_SIZE, rollCycle, 0)) {
                    return;
                }
            } else {
                return;
            }
        }
    }

    @Override
    public void reserve() throws IllegalStateException {
        rc.reserve();
    }

    @Override
    public void release() throws IllegalStateException {
        rc.release();
    }

    @Override
    public long refCount() {
        return rc.get();
    }
}