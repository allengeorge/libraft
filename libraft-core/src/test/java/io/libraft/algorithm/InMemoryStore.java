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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Implements the {@link Store} interface representing the
 * data that has to be placed in durable storage for the Raft distributed consensus
 * algorithm. This clss (obviously) does not use durable storage, and is meant
 * to be used in unit tests only.
 * <p/>
 * NOTE: This class is explicitly not final so that it can be spied upon using Mockito.
 */
class InMemoryStore implements Store {

    private long currentTerm = 0;
    private long commitIndex = 0;
    private final Map<Long, String> votedFor = Maps.newHashMap();

    @Override
    public long getCurrentTerm() throws StorageException {
        return currentTerm;
    }

    @Override
    public void setCurrentTerm(long currentTerm) throws StorageException {
        if (this.currentTerm != 0 && currentTerm != 0) {
            Preconditions.checkArgument(currentTerm > this.currentTerm);
        }

        this.currentTerm = currentTerm;
    }

    @Override
    public long getCommitIndex() throws StorageException {
        return commitIndex;
    }

    @Override
    public void setCommitIndex(long commitIndex) throws StorageException {
        if (this.currentTerm != 0 && commitIndex != 0) {
            Preconditions.checkArgument(commitIndex > this.commitIndex);
        }

        this.commitIndex = commitIndex;
    }

    @Override
    public @Nullable String getVotedFor(long term) throws StorageException {
        return votedFor.get(term);
    }

    @Override
    public void setVotedFor(long term, String server) throws StorageException {
        votedFor.put(term, server);
    }

    @Override
    public void clearVotedFor() throws StorageException {
        votedFor.clear();
    }
}
