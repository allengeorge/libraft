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

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkArgument;

// not final so that Mockito can spy on it
class TempFileSnapshotsStore implements SnapshotsStore {

    private final Path snapshotDirectory;

    private @Nullable Path latestSnapshotPath;
    private long snapshotTerm = -1;  // only set if latestSnapshotPath is not null
    private long snapshotIndex = -1; // only set if latestSnapshotPath is not null

    public TempFileSnapshotsStore(File tempSnapshotDirectory) {
        this.snapshotDirectory = tempSnapshotDirectory.toPath();
    }

    @Override
    public UnitTestTempFileSnapshotWriter newSnapshotWriter() throws StorageException {
        try {
            Path snapshotPath = Files.createTempFile(snapshotDirectory, "temp", "snap");
            return new UnitTestTempFileSnapshotWriter(snapshotPath);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void storeSnapshot(ExtendedSnapshotWriter snapshotWriter) throws StorageException {
        UnitTestTempFileSnapshotWriter writer = (UnitTestTempFileSnapshotWriter) snapshotWriter;

        checkArgument(writer.getTerm() != -1);
        checkArgument(writer.getIndex() != -1);

        snapshotTerm = writer.getTerm();
        snapshotIndex = writer.getIndex();

        latestSnapshotPath = writer.getSnapshotPath();
    }

    @Override
    public @Nullable ExtendedSnapshot getLatestSnapshot() throws StorageException {
        if (latestSnapshotPath == null) {
            return null;
        } else {
            checkArgument(snapshotTerm != -1);
            checkArgument(snapshotIndex != -1);

            return new UnitTestTempFileSnapshot(snapshotTerm, snapshotIndex, latestSnapshotPath);
        }
    }
}
