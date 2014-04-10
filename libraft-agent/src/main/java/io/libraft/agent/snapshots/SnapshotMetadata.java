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

package io.libraft.agent.snapshots;

import com.google.common.base.Objects;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Represents metadata for a single snapshot.
 * Snapshot metadata is independent of the serialized application
 * state and consists of the following properties:
 * <ul>
 *     <li>Snapshot filename: file in which serialized application
 *         state is stored.</li>
 *     <li>Timestamp: epoch time when the snapshot metadata was stored
 *         in the database.</li>
 *     <li>Last term: term of the last log entry applied by the application
 *         before it serialized its state into the snapshot file.</li>
 *     <li>Last index: log index of the last log entry applied by the application
 *         before it serialized its state into the snapshot file.</li>
 * </ul>
 */
public final class SnapshotMetadata {

    private final String filename;
    private final long timestamp;
    private final long lastTerm;
    private final long lastIndex;

    /**
     * Constructor.
     *
     * @param filename non-null, non-empty filename of the file in which the
     *                 snapshot is stored
     * @param timestamp epoch time at which the snapshot metadata was stored in the database
     * @param lastTerm term >= 0 of the last log entry applied by the application
     *                 before it serialized its state into the snapshot file
     * @param lastIndex log index >= 0 of the last log entry applied by the application
     *                  before it serialized its state to the snapshot file
     */
    SnapshotMetadata(String filename, long timestamp, long lastTerm, long lastIndex) {
        checkArgument(!filename.isEmpty());
        checkArgument(timestamp >= 0, "timestamp:%s", timestamp);
        checkArgument(lastTerm >= 0, "lastTerm:%s", lastTerm);
        checkArgument(lastIndex >= 0, "lastIndex:%s", lastIndex);

        this.filename = filename;
        this.timestamp = timestamp;
        this.lastTerm = lastTerm;
        this.lastIndex = lastIndex;
    }

    /**
     * Get the filename of the file in which the snapshot is stored.
     * The filename is <strong>not</strong> an absolute path, and is
     * <strong>relative</strong> to the snapshots directory.
     *
     * @return filename of the file in which the snapshot is stored
     */
    public String getFilename() {
        return filename;
    }

    /**
     * Get the epoch time at which the snapshot metadata was stored in the database.
     *
     * @return epoch time >= 0 at which the snapshot metadata was stored in the database
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Get the term of the last log entry applied by the application
     * before it serialized its state to the snapshot file.
     *
     * @return term >= 0 of the last log entry applied by the application
     * before it serialized its state to the snapshot file
     */
    public long getLastTerm() {
        return lastTerm;
    }

    /**
     * Get the log index of the last log entry applied by the application
     * before it serialized its state to the snapshot file.
     *
     * @return index >= 0 of the last log entry applied by the application
     * before it serialized its state to the snapshot file
     */
    public long getLastIndex() {
        return lastIndex;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotMetadata other = (SnapshotMetadata) o;

        return filename.equals(other.filename) && timestamp == other.timestamp && lastTerm == other.lastTerm && lastIndex == other.lastIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(filename, timestamp, lastTerm, lastIndex);
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add("filename", filename)
                .add("timestamp", timestamp)
                .add("lastTerm", lastTerm)
                .add("lastIndex", lastIndex)
                .toString();
    }
}
