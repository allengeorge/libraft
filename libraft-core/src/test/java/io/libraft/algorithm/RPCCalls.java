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

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

import static io.libraft.algorithm.StoringSender.AppendEntries;
import static io.libraft.algorithm.StoringSender.AppendEntriesReply;
import static io.libraft.algorithm.StoringSender.RPCCall;
import static io.libraft.algorithm.StoringSender.RequestVote;
import static io.libraft.algorithm.StoringSender.RequestVoteReply;
import static io.libraft.algorithm.StoringSender.SnapshotChunk;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

abstract class RPCCalls {

    static void assertNoMoreMessagesSent(StoringSender sender) {
        assertThat(sender.hasNext(), equalTo(false));
    }

    static <T extends RPCCall> List<T> getCallsOfType(StoringSender sender, int callCount, Class<T> klass) {
        List<T> calls = Lists.newArrayListWithCapacity(callCount);

        for (int i = 0; i < callCount; i++) {
            calls.add(i, sender.nextAndRemove(klass));
        }

        return calls;
    }

    static List<String> getDestinations(Collection<? extends RPCCall> calls) {
        List<String> destinations = Lists.newArrayListWithCapacity(calls.size());

        for (RPCCall call : calls) {
            destinations.add(call.server);
        }

        return destinations;
    }

    static void assertThatRequestVoteHasValues(RequestVote requestVote, long term, long lastLogTerm, long lastLogIndex) {
        assertThat(requestVote.term, equalTo(term));
        assertThat(requestVote.lastLogTerm, equalTo(lastLogTerm));
        assertThat(requestVote.lastLogIndex, equalTo(lastLogIndex));
    }

    static void assertThatRequestVoteReplyHasValues(RequestVoteReply requestVoteReply, String server, long term, boolean voteGranted) {
        assertThat(requestVoteReply.server, equalTo(server));
        assertThat(requestVoteReply.term, equalTo(term));
        assertThat(requestVoteReply.voteGranted, equalTo(voteGranted));
    }

    static void assertThatAppendEntriesHasValues(AppendEntries appendEntries, long term, long commitIndex, long prevLogTerm, long prevLogIndex, LogEntry... entries) {
        assertThat(appendEntries.term, equalTo(term));
        assertThat(appendEntries.commitIndex, equalTo(commitIndex));
        assertThat(appendEntries.prevLogTerm, equalTo(prevLogTerm));
        assertThat(appendEntries.prevLogIndex, equalTo(prevLogIndex));
        if (entries.length == 0) {
            assertThat(appendEntries.entries, nullValue());
        } else {
            for (LogEntry entry : entries) {
                assertThat(entry, notNullValue());
            }
            assertThat(appendEntries.entries, hasSize(entries.length));
            assertThat(appendEntries.entries, contains(entries));
        }
    }

     static void assertThatAppendEntriesReplyHasValues(AppendEntriesReply appendEntriesReply, String server, long term, long prevLogIndex, long entryCount, boolean applied) {
        assertThat(appendEntriesReply.server, equalTo(server));
        assertThat(appendEntriesReply.term, equalTo(term));
        assertThat(appendEntriesReply.prevLogIndex, equalTo(prevLogIndex));
        assertThat(appendEntriesReply.entryCount, equalTo(entryCount));
        assertThat(appendEntriesReply.applied, equalTo(applied));
    }

    static void assertThatSnapshotChunkHasValues(SnapshotChunk snapshotChunk, long term, long snapshotTerm, long snapshotIndex, int seqnum, boolean hasMoreChunks) {
        assertThat(snapshotChunk.term, equalTo(term));
        assertThat(snapshotChunk.snapshotTerm, equalTo(snapshotTerm));
        assertThat(snapshotChunk.snapshotIndex, equalTo(snapshotIndex));
        assertThat(snapshotChunk.seqnum, equalTo(seqnum));
        if (hasMoreChunks) {
            assertThat(snapshotChunk.chunkInputStream, notNullValue());
        } else {
            assertThat(snapshotChunk.chunkInputStream, nullValue());
        }
    }

    private RPCCalls() { } // private to prevent instantiation

}
