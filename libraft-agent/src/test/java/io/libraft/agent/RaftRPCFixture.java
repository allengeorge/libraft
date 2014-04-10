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

package io.libraft.agent;

import com.google.common.collect.Lists;
import io.libraft.agent.protocol.AppendEntries;
import io.libraft.agent.protocol.AppendEntriesReply;
import io.libraft.agent.protocol.RequestVote;
import io.libraft.agent.protocol.RequestVoteReply;
import io.libraft.algorithm.LogEntry;
import org.junit.Ignore;

@Ignore
public abstract  class RaftRPCFixture {

    private RaftRPCFixture() { // to prevent instantiation
    }

    public static final RequestVote REQUEST_VOTE = new RequestVote("SERVER_02", "SERVER_04", 32, 10, 31);

    public static final RequestVoteReply REQUEST_VOTE_REPLY = new RequestVoteReply("SERVER_01", "SERVER_03", 9, false);

    public static final AppendEntries APPEND_ENTRIES = new AppendEntries("SERVER_03", "SERVER_04", 32, 10, 17, 31,
                    Lists.<LogEntry>newArrayList(
                            new LogEntry.ClientEntry(18, 31, new UnitTestCommand()),
                            new LogEntry.NoopEntry(19, 32),
                            new LogEntry.NoopEntry(20, 32)));

    public static final AppendEntriesReply APPEND_ENTRIES_REPLY = new AppendEntriesReply("SERVER_01", "SERVER_02", 99, 92, 1, true);
}
