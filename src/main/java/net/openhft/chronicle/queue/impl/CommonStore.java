package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.ReferenceCounted;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.File;

public interface CommonStore extends ReferenceCounted, Demarshallable, WriteMarshallable, Closeable {
    /**
     * @return the file associated with this store.
     */
    @Nullable
    File file();

    @NotNull
    MappedBytes bytes();

    @NotNull
    String dump();

    long writeHeader(@NotNull Wire wire, int length, int safeLength, long timeoutMS) throws EOFException, UnrecoverableTimeoutException;

    long tryWriteHeader(@NotNull Wire wire, int length, int safeLength);

    /**
     * @return the type of wire used
     */
    @NotNull
    WireType wireType();
}
