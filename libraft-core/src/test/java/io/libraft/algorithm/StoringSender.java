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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

class StoringSender implements RPCSender {

    //
    // rpc types
    //

    static abstract class RPCCall {

        final String server;
        final long term;

        private RPCCall(String server, long term) {
            this.server = server;
            this.term = term;
        }
    }

    static final class RequestVote extends RPCCall {

        final long lastLogIndex;
        final long lastLogTerm;

        public RequestVote(String server, long term, long lastLogIndex, long lastLogTerm) {
            super(server, term);
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add("server", server)
                    .add("term", term)
                    .add("lastLogIndex", lastLogIndex)
                    .add("lastLogTerm", lastLogTerm)
                    .toString();
        }
    }

    static final class RequestVoteReply extends RPCCall {

        final boolean voteGranted;

        public RequestVoteReply(String server, long term, boolean voteGranted) {
            super(server, term);
            this.voteGranted = voteGranted;
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add("server", server)
                    .add("term", term)
                    .add("voteGranted", voteGranted)
                    .toString();
        }
    }

    static final class AppendEntries extends RPCCall {

        final long commitIndex;
        final long prevLogIndex;
        final long prevLogTerm;
        final Collection<LogEntry> entries;

        public AppendEntries(String server, long term, long commitIndex, long prevLogIndex, long prevLogTerm, Collection<LogEntry> entries) {
            super(server, term);
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.commitIndex = commitIndex;
            this.entries = entries;
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add("server", server)
                    .add("term", term)
                    .add("commitIndex", commitIndex)
                    .add("prevLogIndex", prevLogIndex)
                    .add("prevLogTerm", prevLogTerm)
                    .add("entries", entries)
                    .toString();
        }
    }

    static final class AppendEntriesReply extends RPCCall {

        final long prevLogIndex;
        final long entryCount;
        final boolean applied;

        public AppendEntriesReply(String server, long term, long prevLogIndex, long entryCount, boolean applied) {
            super(server, term);
            this.prevLogIndex = prevLogIndex;
            this.entryCount = entryCount;
            this.applied = applied;
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add("server", server)
                    .add("term", term)
                    .add("prevLogIndex", prevLogIndex)
                    .add("entryCount", entryCount)
                    .add("applied", applied)
                    .toString();
        }
    }

    //
    // rpc
    //

    StoringSender() {
    }

    //
    // rpc storage
    //

    private final List<RPCCall> rpcCalls = Lists.newLinkedList();

    public Collection<RPCCall> getCalls() {
        return Lists.newArrayList(rpcCalls);
    }

    boolean hasNext() {
        return !rpcCalls.isEmpty();
    }

    void drainSentRPCs() {
        rpcCalls.clear();
    }

    <T extends RPCCall> T nextAndRemove(Class<T> klass) {
        Preconditions.checkState(!rpcCalls.isEmpty());

        Iterator<RPCCall> it = rpcCalls.iterator();
        RPCCall next = it.next();
        it.remove();

        return klass.cast(next);
    }

    //
    // rpc methods
    //

    @Override
    public void requestVote(String server, long term, long lastLogIndex, long lastLogTerm) throws RPCException {
        rpcCalls.add(new RequestVote(server, term, lastLogIndex, lastLogTerm));
    }

    @Override
    public void requestVoteReply(String server, long term, boolean voteGranted) throws RPCException {
        rpcCalls.add(new RequestVoteReply(server, term, voteGranted));
    }

    @Override
    public void appendEntries(String server, long term, long commitIndex, long prevLogIndex, long prevLogTerm, @Nullable Collection<LogEntry> entries) throws RPCException {
        rpcCalls.add(new AppendEntries(server, term, commitIndex, prevLogIndex, prevLogTerm, entries));
    }

    @Override
    public void appendEntriesReply(String server, long term, long prevLogIndex, long entryCount, boolean applied) throws RPCException {
        rpcCalls.add(new AppendEntriesReply(server, term, prevLogIndex, entryCount, applied));
    }
}
