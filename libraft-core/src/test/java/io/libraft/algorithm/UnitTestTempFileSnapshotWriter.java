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

import com.google.common.base.Objects;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class UnitTestTempFileSnapshotWriter implements SnapshotsStore.ExtendedSnapshotWriter {

    private final Path snapshotPath;

    private long term;
    private long index;

    private @Nullable OutputStream outputStream;

    public UnitTestTempFileSnapshotWriter(Path snapshotPath) {
        this.snapshotPath = snapshotPath;
    }

    Path getSnapshotPath() {
        return snapshotPath;
    }

    @Override
    public long getTerm() {
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

    @Override
    public OutputStream getSnapshotOutputStream() throws IOException {
        if (outputStream == null) {
            outputStream = Files.newOutputStream(snapshotPath, StandardOpenOption.WRITE);
        }

        return outputStream;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UnitTestTempFileSnapshotWriter other = (UnitTestTempFileSnapshotWriter) o;

        return snapshotPath.equals(other.snapshotPath)
                && index == other.index
                && term == other.term
                && (outputStream != null ? outputStream.equals(other.outputStream) : other.outputStream == null);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(term, index);
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add("path", snapshotPath)
                .add("term", term)
                .add("index", index)
                .add("outputStream", outputStream)
                .toString();
    }
}
