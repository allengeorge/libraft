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
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * Implementation of {@code ExtendedSnapshot} that constructs
 * and returns an {@code InputStream} to a snapshot stored on disk.
 * <p/>
 * This class is <strong>not</strong> thread-safe and
 * <strong>should not</strong> be accessed simultaneously from multiple threads.
 */
final class SnapshotFileReader implements SnapshotsStore.ExtendedSnapshot {

    private final String snapshotDirectory;
    private final String snapshotFilename;
    private final long term;
    private final long index;

    private InputStream snapshotInputStream;

    /**
     * Constructor.
     *
     * @param snapshotDirectory directory that contains the snapshot to be read
     * @param snapshotFilename filename of the snapshot to be read
     * @param term term in which the last log entry contained in the snapshot was created
     * @param index index of the last log entry contained in the snapshot
     */
    SnapshotFileReader(String snapshotDirectory, String snapshotFilename, long term, long index) {
        this.snapshotDirectory = snapshotDirectory;
        this.snapshotFilename = snapshotFilename;
        this.term = term;
        this.index = index;
    }

    @Override
    public Type getType() {
        return Type.SNAPSHOT;
    }

    @Override
    public long getTerm() {
        return term;
    }

    @Override
    public long getIndex() {
        return index;
    }

    @Override
    public InputStream getSnapshotInputStream() throws FileNotFoundException {
        if (snapshotInputStream == null) {
            File snapshotFile = new File(snapshotDirectory, snapshotFilename);
            snapshotInputStream = new BufferedInputStream(new FileInputStream(snapshotFile));
        }

        return snapshotInputStream;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotFileReader other = (SnapshotFileReader) o;

        return snapshotDirectory.equals(other.snapshotDirectory) && snapshotFilename.equals(other.snapshotFilename) && term == other.term && index == other.index;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(snapshotDirectory, snapshotFilename, term, index);
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add("snapshotDirectory", snapshotDirectory)
                .add("snapshotFilename", snapshotFilename)
                .add("term", term)
                .add("index", index)
                .toString();
    }
}
