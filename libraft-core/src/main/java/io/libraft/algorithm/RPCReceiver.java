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
import java.util.Collection;

/**
 * Incoming Raft RPC message notification service. This is the
 * interface through which {@link RaftAlgorithm} is
 * notified of messages received from other Raft servers. Messages
 * <strong>may</strong> arrive out of order, <strong>may</strong> be
 * duplicates, and <strong>may</strong> arrive from servers that are
 * no longer in the cluster. Messages are sent via the corresponding
 * methods in the {@link RPCSender} interface.
 * <p/>
 * The following messages can be received:
 * <ul>
 *     <li>RequestVote</li>
 *     <li>RequestVoteReply</li>
 *     <li>AppendEntries</li>
 *     <li>AppendEntriesReply</li>
 * </ul>
 * <p/>
 * Implementations <strong>must</strong> guarantee that all incoming messages
 * are well-formed. Implementations <strong>can</strong> assume that if the recipient
 * throws an exception that the system is in an unrecoverable state and can
 * be halted.
 */
public interface RPCReceiver {

    /**
     * Indicates that a RequestVote was received.
     *
     * @param server unique id of the Raft server that sent this message
     * @param term election term in which this message was sent
     * @param lastLogIndex index of the last log entry in the Raft server's Raft log at the time the message was sent
     * @param lastLogTerm election term in which the last log entry in the Raft server's Raft log was created
     */
    void onRequestVote(String server, long term, long lastLogIndex, long lastLogTerm);

    /**
     * Indicates that a RequestVoteReply was received. This is a response to a previously-sent RequestVote message.
     *
     * @param server unique id of the Raft server that sent this message
     * @param term election term in which this message was sent
     * @param voteGranted true if {@code server} granted its vote to the local Raft server in {@code term}, false otherwise
     */
    void onRequestVoteReply(String server, long term, boolean voteGranted);

    /**
     * Indicates that an AppendEntries was received.
     *
     * @param server unique id of the Raft server that sent this message
     * @param term election term in which this message was sent
     * @param commitIndex index of the last {@code LogEntry} that {@code server} believes is committed.
     *                    {@code commitIndex} <strong>may</strong> be >= the local Raft server's log length
     *                    even after all {@code entries} are successfully appended
     * @param prevLogIndex index of the {@code LogEntry} immediately before the first {@code LogEntry} in {@code entries}
     * @param prevLogTerm election term in which the {@code LogEntry} at {@code prevLogIndex} was created
     * @param entries sequence of monotonic, gapless, {@code LogEntry} instances with
     *                indices {@code prevLogIndex + 1}, {@code prevLogIndex + 2} .. {@code prevLogIndex + entries.count()}.
     *                May be {@code null} if this AppendEntries message is a heartbeat
     *
     * @see LogEntry
     */
    void onAppendEntries(String server, long term, long commitIndex, long prevLogIndex, long prevLogTerm, @Nullable Collection<LogEntry> entries);

    /**
     * Indicates that an AppendEntriesReply was received. This is a response to previously-sent AppendEntries message.
     *
     * @param server unique id of the server that sent this message
     * @param term election term in which this message was sent
     * @param prevLogIndex index of the {@link LogEntry} after
     *                     which log entries were meant to be appended
     * @param entryCount number of log entries meant to be appended to the {@code source} server's log.
     *                   0, if this is a response to a heartbeat
     * @param applied true if the log prefix uniquely defined by
     *                ({@code prevLogIndex}, {@code prevLogTerm}) in the AppendEntries
     *                request matched and the Raft server was able to append the entries
     *                {@code prevLogIndex + 1} to {@code prevLogIndex + entryCount}
     *                to its log. false otherwise
     */
    void onAppendEntriesReply(String server, long term, long prevLogIndex, long entryCount, boolean applied);
}
