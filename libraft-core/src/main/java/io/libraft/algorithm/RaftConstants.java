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

import java.util.concurrent.TimeUnit;

/**
 * Default constants used in {@link RaftAlgorithm}.
 * These constants control network timeouts, Raft algorithm timeouts, <em>etc.</em>
 * <p/>
 * All time constants are defined in milliseconds (ms).
 */
public abstract class RaftConstants {

    private static final long TWELVE_HOURS = 12 * 60 * 60 * 1000;

    // MIN_ELECTION_TIMEOUT and ADDITIONAL_ELECTION_TIMEOUT_RANGE are chosen so that:
    // - The max election timeout (MAX_ELECTION_TIMEOUT) is 300 ms
    // - Even if the following sequence of events occur:
    //   - S1 electionTimeout initialized to MIN_ELECTION_TIMEOUT
    //   - S1 crashes the moment electionTimeout expires
    //   - S1 restarts immediately (0 ms downtime)
    //   - S1 electionTimeout initialized again to MIN_ELECTION_TIMEOUT
    //   We still have the chance for the _ongoing_ election to complete
    //   (because the MAX_ELECTION_TIMEOUT = 300ms) and for the chosen leader
    //   to try send a heartbeat AppendEntries RPC to S1 (in fact, it has _4_ chances,
    //   because HEARTBEAT_INTERVAL = 15ms), so that S1 can find out who the
    //   current leader is, and not cause another election to occur.
    /**
     * Minimum time interval after which a follower will:
     * <ol>
     *     <li>become a candidate</li>
     *     <li><strong>and</strong> begin a new election cycle</li>
     * </ol>
     * if it does <strong>not</strong> hear from the current election term's leader.
     */
    public static final int MIN_ELECTION_TIMEOUT = 180;
    /**
     * Upper bound to the random amount of time added to
     * {@link RaftConstants#MIN_ELECTION_TIMEOUT} to
     * create the actual election timeout for a candidate or follower.
     * <p/>
     * To allow elections to converge quickly
     * election timeouts are randomized. There must also be a
     * floor to the timeouts to prevent transient
     * conditions from triggering spurious elections.
     * These two conditions mean that each server in the Raft cluster chooses an
     * election timeout using the following formula:
     * <pre>
     *     ELECTION_TIMEOUT = MIN_ELECTION_TIMEOUT + randomIntInRange(0, ADDITIONAL_ELECTION_TIMEOUT_RANGE)
     * </pre>
     * As a result, {@code ELECTION_TIMEOUT} has the following range:
     * {@code [MIN_ELECTION_TIMEOUT, MIN_ELECTION_TIMEOUT + ADDITIONAL_ELECTION_TIMEOUT_RANGE]}.
     */
    public static final int ADDITIONAL_ELECTION_TIMEOUT_RANGE = 120;

    // for the system to make progress RPC_TIMEOUT << MAX_ELECTION_TIMEOUT << Server MTBF
    /**
     * Time interval within which {@link RaftAlgorithm}
     * expects the response (RequestVoteReply, AppendEntriesReply)
     * to a Raft RPC request (RequestVote, AppendEntries).
     */
    public static final int RPC_TIMEOUT = 30;

    // AFAICT, the HEARTBEAT_INTERVAL has some interesting properties:
    // 1. it has to be << MIN_ELECTION_TIMEOUT so that
    //    a few missing messages won't unnecessarily trigger an election
    // 2. it can't be too aggressive and flood the network
    // 3. it appears to function similarly to a COMMIT message
    //    in a 2-phase commit, because in the absence of additional
    //    writes, it is the only way for followers to find out if
    //    log entries written locally as a result of an earlier AppendEntries
    //    message can be delivered to clients
    // 4. if 3. is true, then in an idle system with a perfect network it
    //    takes a minimum of 1 network round trip + HEARTBEAT_INTERVAL + 1 network 1-way trip
    //    for a client to be notified _at a follower_ that a state-machine
    //    command can be applied
    /**
     * Time interval after which the leader generates an AppendEntries message.
     * A new interval is started immediately after sending the messages generated in the previous one.
     */
    public static final int HEARTBEAT_INTERVAL = 15;

    /**
     * Time interval at which to check if a snapshot should be made.
     */
    public static final long SNAPSHOT_CHECK_INTERVAL = TWELVE_HOURS;

    /**
     * {@link java.util.concurrent.TimeUnit} in which all
     * the time constants in {@code RaftConstants} are defined.
     */
    public static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    /**
     * Exit code used to shutdown the JVM when
     * {@link RaftAlgorithm} encounters an uncaught throwable.
     */
    public static final int UNCAUGHT_THROWABLE_EXIT_CODE = 129;

    /**
     * Constant that signals that {@link RaftAlgorithm} should
     * <strong>not</strong> create snapshots and compact the Raft log.
     */
    public static final int SNAPSHOTS_DISABLED = -1;

    /**
     * Initial value returned by {@link io.libraft.SnapshotWriter#getIndex()}.
     * This is an <em>invalid</em> log index.
     */
    public static final long INITIAL_SNAPSHOT_WRITER_LOG_INDEX = -1;

    // to prevent instantiation of RaftConstants
    private RaftConstants() {}
}
