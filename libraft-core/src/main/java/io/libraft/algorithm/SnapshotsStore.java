/*
 * Copyright (c) 2013 - 2014, Allen A. George <allen dot george at gmail dot com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of libraft nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.libraft.algorithm;

import io.libraft.Snapshot;
import io.libraft.SnapshotWriter;

import javax.annotation.Nullable;

/**
 * {@link Snapshot} storage service.
 * <p/>
 * Defined methods and classes to persist serialized state
 * as a snapshot to, and retrieve the last stored snapshot from,
 * a backing store. Implementations must store the
 * following information at minimum:
 * <ul>
 *     <li>Serialized state.</li>
 *     <li>Last log index captured in the snapshot.</li>
 *     <li>Term of the last log index captured in the snapshot.</li>
 * </ul>
 * Snapshots are serialized using an instance of an
 * {@link ExtendedSnapshotWriter} and deserialized using an instance
 * of an {@link ExtendedSnapshot}, both of which are constructed by
 * the {@code SnapshotsStore} implementation.
 * <p/>
 * Implementations need only guarantee that each
 * <strong>individual</strong> {@code SnapshotsStore} operation
 * is atomic. They <strong>do not</strong> have to guarantee
 * that {@link SnapshotWriter} or {@link Snapshot} methods
 * are thread-safe. They <strong>must</strong> wrap all
 * implementation-specific checked exceptions in a {@link StorageException}
 * and rethrow them.
 */
public interface SnapshotsStore {

    /**
     * Object with which snapshot state can be written.
     * The written snapshot is not persisted to durable storage until
     * a subsequent call to
     * {@link SnapshotsStore#storeSnapshot(ExtendedSnapshotWriter)}.
     * <p/>
     * Implementations <strong>do not</strong> have to be thread safe.
     * Behavior of this object when used simultaneously from
     * multiple threads is <strong>undefined</strong>.
     */
    interface ExtendedSnapshotWriter extends SnapshotWriter {

        /**
         * Set the term of the last committed log entry contained in the snapshot.
         *
         * @param term term >= 0 in the Raft log of last committed log entry contained in the snapshot
         */
        void setTerm(long term);
    }

    /**
     * Object from which snapshot state can be loaded.
     * <p/>
     * Implementations <strong>do not</strong> have to be thread safe.
     * Behavior of this object when used simultaneously from
     * multiple threads is <strong>undefined</strong>.
     */
    interface ExtendedSnapshot extends Snapshot {

        /**
         * Get the term of the last committed log entry contained in the snapshot.
         *
         * @return term >= 0 in the Raft log of last committed log entry contained in the snapshot
         */
        long getTerm();
    }

    /**
     * Create an instance of {@code ExtendedSnapshotWriter}.
     *
     * @return a valid instance of {@code ExtendedSnapshotWriter}
     * @throws StorageException if an instance of {@code ExtendedSnapshotWriter} cannot be created
     */
    ExtendedSnapshotWriter newSnapshotWriter() throws StorageException;

    /**
     * Persist the serialized state captured by an {@code ExtendedSnapshotWriter}
     * into the {@code SnapshotsStore}. A call to {@link SnapshotsStore#getLatestSnapshot()} after
     * successful completion of this method <strong>will</strong> return the serialized
     * state contained in {@code snapshotWriter}.
     *
     * @param snapshotWriter instance of {@code ExtendedSnapshotWriter} containing the
     *                       snapshot data to be persisted
     * @throws StorageException if the serialized snapshot data cannot be persisted to the backing store
     */
    void storeSnapshot(ExtendedSnapshotWriter snapshotWriter) throws StorageException;

    /**
     * Get the latest snapshot persisted to the backing store.
     *
     * @return an instance of {@code ExtendedSnapshot} that can be used
     * to read the latest snapshot state, or null if there are no snapshots
     * @throws StorageException if information about the latest snapshot
     * cannot be retrieved from the backing store, or an instance of {@code ExtendedSnapshot}
     * cannot be constructed
     */
    @Nullable ExtendedSnapshot getLatestSnapshot() throws StorageException;
}
