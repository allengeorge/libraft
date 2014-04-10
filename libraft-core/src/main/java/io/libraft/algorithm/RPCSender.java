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
 * Raft RPC message sending service. {@link RaftAlgorithm}
 * uses this component to send the following RPC messages to other servers
 * in the Raft cluster:
 * <ul>
 *     <li>RequestVote</li>
 *     <li>RequestVoteReply</li>
 *     <li>AppendEntries</li>
 *     <li>AppendEntriesReply</li>
 * </ul>
 * <p/>
 * Implementations <strong>do not</strong> have to validate the correctness
 * of message fields. They <strong>only</strong> have to offer
 * best-effort delivery semantics. Messages <strong>may</strong>
 * be sent or received out-of-order, <strong>may</strong> be duplicated,
 * or <strong>may</strong> be lost. Implementations <strong>may</strong>
 * choose to retry sending on the sender's behalf <strong>without</strong> notifying the sender.
 * All implementation-specific checked exceptions <strong>must</strong>
 * be wrapped in an {@link RPCException} and rethrown.
 * <p/>
 * {@code RaftAlgorithm} will be notified of incoming
 * messages via the corresponding callback methods in {@link RPCReceiver}.
 * There is a corresponding callback method for each method below.
 */
public interface RPCSender {

    /**
     * Send a RequestVote.
     *
     * @param server unique id of the Raft server to which the message should be sent
     * @param term election term in which this message was generated
     * @param lastLogIndex index of the last {@code LogEntry} in the local Raft server's Raft log
     * @param lastLogTerm election term in which the {@code LogEntry} at {@code lastLogIndex} was created
     *
     * @see LogEntry
     */
    void requestVote(String server, long term, long lastLogIndex, long lastLogTerm) throws RPCException;

    /**
     * Send a RequestVoteReply. Response to a RequestVote.
     *
     * @param server unique id of the Raft server to which the message should be sent
     * @param term election term in which this message was generated
     * @param voteGranted true if the local Raft server granted {@code server} its vote in {@code term}, false otherwise
     */
    void requestVoteReply(String server, long term, boolean voteGranted) throws RPCException;

    /**
     * Send an AppendEntries.
     *
     * @param server unique id of the Raft server to which the message should be sent
     * @param term election term in which this message was generated
     * @param commitIndex index of the last {@code LogEntry} that the local Raft server believes to be committed
     * @param prevLogIndex index of the {@code LogEntry} immediately before the first {@code LogEntry} in {@code entries}
     * @param prevLogTerm election term in which the {@code LogEntry} at {@code prevLogIndex} was created
     * @param entries sequence of monotonic, gapless, {@code LogEntry} instances with
     *                indices {@code prevLogIndex + 1}, {@code prevLogIndex + 2} .. {@code prevLogIndex + entries.count()}.
     *                {@code null} if this AppendEntries message is a heartbeat
     *
     * @see LogEntry
     */
    void appendEntries(String server, long term, long commitIndex, long prevLogIndex, long prevLogTerm, @Nullable Collection<LogEntry> entries) throws RPCException;

    /**
     * Send an AppendEntriesReply. Response to an AppendEntries.
     *
     * @param server unique id of the Raft server to which the message should be sent
     * @param term election term in which this message was generated
     * @param prevLogIndex index of the {@code LogEntry} after which log entries were meant to be appended to the local Raft server's log
     * @param entryCount number of log entries meant to be appended to the {@code source} server's log.
     *                   0, if this is a response to a heartbeat
     * @param applied true if the log prefix uniquely defined by
     *                ({@code prevLogIndex}, {@code prevLogTerm}) in the AppendEntries
     *                request matched and the local Raft server was able to append the entries
     *                {@code prevLogIndex + 1} to {@code prevLogIndex + entryCount}
     *                to its log. false otherwise
     *
     * @see LogEntry
     */
    void appendEntriesReply(String server, long term, long prevLogIndex, long entryCount, boolean applied) throws RPCException;
}
