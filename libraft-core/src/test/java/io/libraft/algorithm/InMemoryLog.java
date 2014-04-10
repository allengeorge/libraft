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
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.ArrayList;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Implements the {@link Log} interface representing the Raft consensus log. This log is stored
 * in memory only, and meant to be used only in unit tests.
 * <p/>
 * NOTE: This class is explicitly not final so that it can be spied upon using Mockito.
 */
class InMemoryLog implements Log {

    private final ArrayList<LogEntry> entries = Lists.newArrayList();

    @Override
    public @Nullable LogEntry get(long index) throws StorageException {
        checkArgument(index >= 0, index);

        if (index >= entries.size()) {
            return null;
        }

        return entries.get((int) index);
    }

    @Override
    public @Nullable LogEntry getFirst() throws StorageException {
        if (entries.isEmpty()) {
            return null;
        }

        LogEntry returned = null;

        for (LogEntry entry : entries) {
            // noinspection ConstantConditions
            if (entry != null) { // note that entry _can_ be null; I intentionally put nulls into indexes that are 'empty'
                returned = entry;
                break;
            }
        }

        return returned;
    }

    @Override
    public @Nullable LogEntry getLast() throws StorageException {
        if (entries.isEmpty()) {
            return null;
        }

        return entries.get(entries.size() - 1);
    }

    @Override
    public void put(LogEntry entry) throws StorageException {
        checkArgument(entry.getIndex() >= 0);

        if (entry.getIndex() == 0) {
            checkArgument(entry.equals(LogEntry.SENTINEL));
        }

        if (entry.getIndex() > entries.size()) { // [sigh] manually resize the list
            for (int i = entries.size(); i < entry.getIndex(); i++) {
                entries.add(i, null);
            }
        }

        if (entry.getIndex() < entries.size()) {
            entries.set((int) entry.getIndex(), entry);
        } else {
            checkArgument(entry.getIndex() == entries.size(), "index:%s size:%s", entry.getIndex(), entries.size());
            entries.add((int) entry.getIndex(), entry);
        }
    }

    @Override
    public void truncate(long index) throws StorageException {
        if (index >= entries.size()) {
            return;
        }

        for (int i = entries.size() - 1; i >= (int) index; i--) {
            entries.remove(i);
        }
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add("entries", entries)
                .toString();
    }
}
