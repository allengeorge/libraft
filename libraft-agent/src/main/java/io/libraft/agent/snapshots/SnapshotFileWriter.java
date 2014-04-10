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
import io.libraft.algorithm.SnapshotsStore;

import javax.annotation.Nullable;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of {@code ExtendedSnapshotWriter} that returns
 * an {@code OutputStream} that can be used to write serialized application
 * state to disk.
 * <p/>
 * This class is <strong>not</strong> thread-safe and
 * <strong>should not</strong> be accessed simultaneously from multiple threads.
 */
final class SnapshotFileWriter implements SnapshotsStore.ExtendedSnapshotWriter {

    private final String snapshotDirectory;

    private long term;
    private long index;
    private @Nullable File tempSnapshotFile;
    private @Nullable OutputStream snapshotOutputStream;

    /**
     * Constructor
     *
     * @param snapshotDirectory directory in which the snapshot file is stored
     */
    SnapshotFileWriter(String snapshotDirectory) {
        this.snapshotDirectory = snapshotDirectory;
    }

    /**
     * Set the term of the last committed log entry contained in the snapshot.
     *
     * @return term >= 0 in the Raft log of last committed log entry contained in the snapshot
     */
    long getTerm() {
        return term;
    }

    @Override
    public void setTerm(long term) {
        this.term = term;
    }

    @Override
    public long getIndex() {
        return index;
    }

    @Override
    public void setIndex(long index) {
        this.index = index;
    }

    /**
     * Get whether {@link SnapshotFileWriter#getSnapshotOutputStream()} was called.
     * Failure to get an {@code OutputStream} to the snapshot file
     * indicates that the user of this instance did not attempt to
     * serialize application state.
     *
     * @return true if {@link SnapshotFileWriter#getSnapshotOutputStream()} was called, false otherwise
     */
    boolean snapshotStarted() {
        return tempSnapshotFile != null;
    }

    /**
     * Get the file to which the serialized application state was written.
     * This method <strong>does not</strong> check if the file
     * still exists or is usable by the caller.
     *
     * @return handle to the file to which the serialized application state was written
     */
    File getSnapshotFile() {
        checkState(tempSnapshotFile != null, "snapshot was not started");
        return checkNotNull(tempSnapshotFile);
    }

    @Override
    public OutputStream getSnapshotOutputStream() throws IOException {
        if (tempSnapshotFile == null) {
            tempSnapshotFile = File.createTempFile("tmp-", "snap");
            tempSnapshotFile.deleteOnExit();
            snapshotOutputStream = new BufferedOutputStream(new FileOutputStream(tempSnapshotFile));
        }

        return snapshotOutputStream;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(snapshotDirectory, tempSnapshotFile);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotFileWriter other = (SnapshotFileWriter) o;

        return snapshotDirectory.equals(other.snapshotDirectory) && (tempSnapshotFile == null ? other.tempSnapshotFile == null : tempSnapshotFile.equals(other.tempSnapshotFile));
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add("snapshotDirectory", snapshotDirectory)
                .add("snapshotFile", tempSnapshotFile)
                .toString();
    }
}
