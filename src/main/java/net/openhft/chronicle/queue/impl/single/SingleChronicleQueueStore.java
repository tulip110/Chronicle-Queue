/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.ReferenceCounter;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.StreamCorruptedException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SingleChronicleQueueStore implements WireStore {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(SCQIndexing.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(SCQRoll.class, "Roll");
    }

    @NotNull
    final SCQIndexing indexing;
    @NotNull
    private final WireType wireType;
    @NotNull
    private final SCQRoll roll;
    @NotNull
    private final LongValue writePosition;
    @NotNull

    private final LongValue seqAndPosition;
    private Sequence sequence;
    @NotNull
    private final MappedBytes mappedBytes;
    @NotNull
    private final MappedFile mappedFile;
    @NotNull
    private final ReferenceCounter refCount;
    @Nullable
    private final StoreRecovery recovery;

    private int deltaCheckpointInterval;
    @Nullable
    private LongValue lastAcknowledgedIndexReplicated;

    /**
     * used by {@link net.openhft.chronicle.wire.Demarshallable}
     *
     * @param wire a wire
     */
    @UsedViaReflection
    private SingleChronicleQueueStore(@NotNull WireIn wire) {
        assert wire.startUse();
        try {
            this.wireType = wire.read(MetaDataField.wireType).object(WireType.class);
            assert wireType != null;
            this.writePosition = wire.newLongReference();
            this.seqAndPosition = wire.newLongReference();
            wire.read(MetaDataField.writePosition).int64(writePosition);
            this.roll = wire.read(MetaDataField.roll).typedMarshallable();

            this.mappedBytes = (MappedBytes) (wire.bytes());
            this.mappedFile = mappedBytes.mappedFile();
            this.refCount = ReferenceCounter.onReleased(this::onCleanup);
            this.indexing = wire.read(MetaDataField.indexing).typedMarshallable();
            assert indexing != null;
            this.indexing.writePosition = writePosition;

            this.sequence = new RollCycleEncodeSequence(seqAndPosition, rollIndexCount(), rollIndexSpacing());


            if (wire.bytes().readRemaining() > 0) {
                this.lastAcknowledgedIndexReplicated = wire.read(MetaDataField.lastAcknowledgedIndexReplicated)
                        .int64ForBinding(null);
            } else {
                this.lastAcknowledgedIndexReplicated = null; // disabled.
            }
            if (wire.bytes().readRemaining() > 0) {
                this.recovery = wire.read(MetaDataField.recovery)
                        .typedMarshallable();
            } else {
                this.recovery = new SimpleStoreRecovery(); // disabled.
            }

            if (wire.bytes().readRemaining() > 0) {
                this.deltaCheckpointInterval = wire.read(MetaDataField.deltaCheckpointInterval)
                        .int32();
            } else {
                this.deltaCheckpointInterval = -1; // disabled.
            }
            if (wire.bytes().readRemaining() > 0) {
                wire.read(MetaDataField.sequence).int64(seqAndPosition);
            }
        } finally {
            assert wire.endUse();
        }
    }

    /**
     * @param rollCycle               the current rollCycle
     * @param wireType                the wire type that is being used
     * @param mappedBytes             used to mapped the data store file
     * @param epoch                   sets an epoch offset as the number of number of milliseconds
     *                                since
     * @param indexCount              the number of entries in each index.
     * @param indexSpacing            the spacing between indexed entries.
     * @param recovery
     * @param deltaCheckpointInterval
     */
    public SingleChronicleQueueStore(@Nullable RollCycle rollCycle,
                                     @NotNull final WireType wireType,
                                     @NotNull MappedBytes mappedBytes,
                                     long epoch,
                                     int indexCount,
                                     int indexSpacing,
                                     StoreRecovery recovery,
                                     int deltaCheckpointInterval) {
        this.recovery = recovery;
        this.roll = new SCQRoll(rollCycle, epoch);
        this.wireType = wireType;
        this.mappedBytes = mappedBytes;
        this.mappedFile = mappedBytes.mappedFile();
        this.refCount = ReferenceCounter.onReleased(this::onCleanup);

        indexCount = Maths.nextPower2(indexCount, 8);
        indexSpacing = Maths.nextPower2(indexSpacing, 1);

        this.indexing = new SCQIndexing(wireType, indexCount, indexSpacing);
        this.indexing.writePosition = this.writePosition = wireType.newLongReference().get();
        this.seqAndPosition = wireType.newLongReference().get();


        this.indexing.sequence = this.sequence = new RollCycleEncodeSequence(this.seqAndPosition, rollCycle.defaultIndexCount(), rollCycle.defaultIndexSpacing());
        this.lastAcknowledgedIndexReplicated = wireType.newLongReference().get();
        this.deltaCheckpointInterval = deltaCheckpointInterval;
    }

    public static void dumpStore(@NotNull Wire wire) {
        Bytes<?> bytes = wire.bytes();
        bytes.readPositionUnlimited(0);
        Jvm.debug().on(SingleChronicleQueueStore.class, Wires.fromSizePrefixedBlobs(wire));
    }

    @NotNull
    public static String dump(@NotNull String directoryFilePath) {
        SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(directoryFilePath).build();
        return q.dump();
    }

    /**
     * @return the type of wire used
     */
    @NotNull
    @Override
    public WireType wireType() {
        return wireType;
    }

    @Nullable
    @Override
    public File file() {
        return mappedFile == null ? null : mappedFile.file();
    }

    /**
     * when using replication to another host, this is the last index that has been confirmed to *
     * have been read by the remote host.
     */
    @Override
    public long lastAcknowledgedIndexReplicated() {
        return lastAcknowledgedIndexReplicated == null ? -1 : lastAcknowledgedIndexReplicated.getVolatileValue();
    }

    @Override
    public void lastAcknowledgedIndexReplicated(long newValue) {
        if (lastAcknowledgedIndexReplicated != null)
            lastAcknowledgedIndexReplicated.setMaxValue(newValue);
    }

    @NotNull
    @Override
    public String dump() {

        MappedBytes bytes = MappedBytes.mappedBytes(mappedFile);
        try {
            bytes.readLimit(bytes.realCapacity());
            return Wires.fromSizePrefixedBlobs(bytes);
        } finally {
            bytes.release();
        }
    }

    @Override
    public long writePosition() {
        return this.writePosition.getVolatileValue();
    }

    @NotNull
    @Override
    public WireStore writePosition(long position) {

        assert writePosition.getVolatileValue() + mappedFile.chunkSize() > position;
        int header = mappedBytes.readVolatileInt(position);
        if (Wires.isReadyData(header)) {
            writePosition.setMaxValue(position);
            sequence.sequence(header, position);
        } else
            throw new AssertionError();
        return this;
    }

    /**
     * @return an epoch offset as the number of number of milliseconds since January 1, 1970,
     * 00:00:00 GMT
     */
    @Override
    public long epoch() {
        return this.roll.epoch();
    }

    /**
     * Moves the position to the index
     *
     * @param ec    the data structure we are navigating
     * @param index the index we wish to move to
     * @return whether the index was found for reading.
     */
    @Nullable
    @Override
    public ScanResult moveToIndexForRead(@NotNull ExcerptContext ec, long index) {
        try {
            return indexing.moveToIndex(recovery, ec, index);
        } catch (@NotNull UnrecoverableTimeoutException | StreamCorruptedException e) {
            return ScanResult.NOT_REACHED;
        }
    }

    @Override
    public void reserve() throws IllegalStateException {
        this.refCount.reserve();
    }

    @Override
    public void release() throws IllegalStateException {
        this.refCount.release();
    }

    @Override
    public long refCount() {
        return this.refCount.get();
    }

    @Override
    public void close() {
        while (refCount.get() > 0) {
            refCount.release();
        }
    }

    /**
     * @return creates a new instance of mapped bytes, because, for example the tailer and appender
     * can be at different locations.
     */
    @NotNull
    @Override
    public MappedBytes bytes() {
        return MappedBytes.mappedBytes(mappedFile);
    }

    @Override
    public long sequenceForPosition(@NotNull final ExcerptContext ec, final long position, boolean inclusive) throws
            UnrecoverableTimeoutException, StreamCorruptedException {
        return indexing.sequenceForPosition(recovery, ec, position, inclusive);
    }

    @Override
    public long lastSequenceNumber(@NotNull ExcerptContext ec) throws StreamCorruptedException {
        return indexing.lastSequenceNumber(recovery, ec);
    }

    @NotNull
    @Override
    public String toString() {
        return "SingleChronicleQueueStore{" +
                "indexing=" + indexing +
                ", wireType=" + wireType +
                ", checkpointInterval=" + this.deltaCheckpointInterval +
                ", roll=" + roll +
                ", writePosition=" + writePosition +
                ", mappedFile=" + mappedFile +
                ", refCount=" + refCount +
                ", lastAcknowledgedIndexReplicated=" + lastAcknowledgedIndexReplicated +
                '}';
    }

    private void onCleanup() {
        mappedBytes.release();
    }

    // *************************************************************************
    // Marshalling
    // *************************************************************************

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        if (lastAcknowledgedIndexReplicated == null)
            lastAcknowledgedIndexReplicated = wire.newLongReference();

        wire.write(MetaDataField.wireType).object(wireType)
                .writeAlignTo(8, 0).write(MetaDataField.writePosition).int64forBinding(0L, writePosition)
                .write(MetaDataField.roll).typedMarshallable(this.roll)
                .write(MetaDataField.indexing).typedMarshallable(this.indexing)
                .write(MetaDataField.lastAcknowledgedIndexReplicated)
                .int64forBinding(-1L, lastAcknowledgedIndexReplicated);
        wire.write(MetaDataField.recovery).typedMarshallable(recovery);
        wire.write(MetaDataField.deltaCheckpointInterval).int32(this.deltaCheckpointInterval);
        wire.padToCacheAlign();
    }

    @Override
    public boolean indexable(long index) {
        return indexing.indexable(index);
    }

    @Override
    public void setPositionForSequenceNumber(@NotNull final ExcerptContext ec, long sequenceNumber,
                                             long position)
            throws UnrecoverableTimeoutException, StreamCorruptedException {
        long nextSequence = indexing.nextEntryToBeIndexed();
        if (nextSequence > sequenceNumber)
            return;

        try {
            indexing.setPositionForSequenceNumber(recovery, ec,
                    sequenceNumber, position);

        } catch (EOFException ignored) {
            // todo unable to add an index to a rolled store.
        }
    }

    @Override
    public ScanResult linearScanTo(final long index, final long knownIndex, final ExcerptContext ec, final long knownAddress) {
        return indexing.linearScanTo(index, knownIndex, ec, knownAddress);
    }

    @Override
    public long writeHeader(@NotNull Wire wire, int length, int safeLength, long timeoutMS) throws EOFException, UnrecoverableTimeoutException {
        return recovery.writeHeader(wire, length, safeLength, timeoutMS, writePosition, sequence);
    }

    @Override
    public long tryWriteHeader(Wire wire, int length, int safeLength) {
        return recovery.tryWriteHeader(wire, length, safeLength);
    }

    @Override
    public void writeEOF(@NotNull Wire wire, long timeoutMS) throws TimeoutException {
        // just in case we are about to release this
        if (wire.bytes().tryReserve()) {
            wire.writeEndOfWire(timeoutMS, TimeUnit.MILLISECONDS, writePosition());
            wire.bytes().release();
        } else {
            Jvm.debug().on(getClass(), "Tried to writeEOF to as it was being closed");
        }
    }

    @Override
    public int deltaCheckpointInterval() {
        return deltaCheckpointInterval;
    }

    int rollCycleLength() {
        return roll.length();
    }

    int rollIndexCount() {
        return indexing.indexCount();
    }

    int rollIndexSpacing() {
        return indexing.indexSpacing();
    }

}

