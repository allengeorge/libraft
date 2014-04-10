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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.libraft.Command;
import io.libraft.Committed;
import io.libraft.CommittedCommand;
import io.libraft.NotLeaderException;
import io.libraft.Raft;
import io.libraft.RaftListener;
import io.libraft.ReplicationException;
import io.libraft.SnapshotWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.libraft.algorithm.SnapshotsStore.ExtendedSnapshot;
import static io.libraft.algorithm.SnapshotsStore.ExtendedSnapshotWriter;

/**
 * Raft distributed consensus algorithm.
 * <p/>
 * An instance of this component is instantiated by each Raft server
 * in the Raft cluster. Clients use {@link Raft} to
 * interact with {@code RaftAlgorithm}. Messages to other servers
 * in the Raft cluster are sent via a {@link RPCSender}, and incoming
 * messages are consumed via the {@link RPCReceiver} interface.
 * Raft algorithm metadata is stored in an instance of a {@link Store},
 * {@link LogEntry} instances are persisted to the durable
 * {@link Log}, while snapshots are persisted to the {@link SnapshotsStore}.
 * This component uses a {@link Timer} to schedule tasks
 * for future execution.
 *
 * <h3>Thread Safety</h3>
 * This class is thread-safe.
 * <p/>
 * Thread-safety is achieved through use
 * of a 'big lock', which serializes operations on {@code RaftAlgorithm}.
 * The lock is held during <strong>all</strong> calls to external subsystems
 * ({@code RPCSender}, {@code Store}, {@code SnapshotStore},
 * {@code Log}, {@code Timer}), <strong>while</strong> processing incoming
 * messages from {@code RPCReceiver}, <strong>and</strong> while clients are
 * notified of committed log entries and snapshots via
 * {@link io.libraft.RaftListener#applyCommitted(Committed)}.
 * <p/>
 * Unfortunately this design is prone to deadlocks. For example, the
 * following lock structure:
 * <ul>
 *     <li>{@code big lock}: {@code RaftAlgorithm}</li>
 *     <li>{@code client lock}: {@code Raft}, {@code RaftListener}</li>
 * </ul>
 * Could result in a deadlock during the following call sequence:
 * <pre>
 *
 *     RPCReceiver                  RaftAlgorithm                 RaftListener                       Raft
 *     -----------                  -------------                 ------------                       ----
 *         |                             |                             |                              |
 *         | -- onAppendEntriesReply --> |                             |                              |
 *         |   [ acquire 'big lock' ]    |                             |                              |
 *         |                             |                             |                              |
 *         |                             |                             |                              |
 *         |                             |                   [ acquire 'client lock' ]                |
 *         |                             | <-----------------------submitCommand--------------------- |
 *         |                             |                   [  wait for 'big lock'  ]                |
 *         |                             |                             |                              |
 *         |                             |                             |                              |
 *         |                             |  ----- applyCommitted ----> |                              |
 *         |                             |  [ wait for 'client lock' ] |                              |
 *         |                             |                             |                              |
 * </pre>
 * Clients should account for this in designing and implementing their locking
 * mechanisms.
 *
 * <h3>{@code RaftListener} Callbacks</h3>
 * {@code RaftAlgorithm} expects that <strong>all</strong> calls to
 * {@code RaftListener} complete <strong>without</strong> throwing.
 * This implementation <strong>will</strong> terminate
 * the JVM with a {@link RaftConstants#UNCAUGHT_THROWABLE_EXIT_CODE}
 * exit code on <strong>any</strong> uncaught listener exception.
 * This behaviour was chosen because {@code RaftAlgorithm} cannot
 * guarantee that the overall system (which includes both
 * {@code RaftAlgorithm} and client code) is in a consistent state
 * and will continue to operate correctly after an unhandled error.
 *
 * <h3>Exception Handling</h3>
 * There are firve major entry points into {@code RaftAlgorithm}:
 * <ol>
 *     <li>Timer tasks</li>
 *     <li>on[MessageName]</li>
 *     <li>issueCommand</li>
 *     <li>snapshotWritten</li>
 *     <li>getNextCommitted</li>
 * </ol>
 * Exceptions thrown during {@code RaftAlgorithm}
 * calls are caught at these entry points. The only situations otherwise
 * are those where internal methods can consume the throwable:
 * <ol>
 *     <li>without leaving the system in an inconsistent state</li>
 *     <li><strong>and</strong> allow subsequent correct operation.</li>
 * </ol>
 * {@link RPCException} instances are considered recoverable and
 * {@code RaftAlgorithm} will continue to operate in the face of network
 * errors. {@link StorageException} instances are considered
 * <strong>unrecoverable</strong> and are wrapped in a {@link RaftError}
 * and rethrown. Clients <strong>must not</strong> catch {@code RaftError} and
 * <strong>must not</strong> proceed after encountering a {@code RaftError}.
 */
public final class RaftAlgorithm implements RPCReceiver, Raft {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftAlgorithm.class);

    // FIXME (AG): prevent client callbacks from making a call back into RaftAlgorithm!
    // FIXME (AG): restructure threading to avoid deadlocks
    // FIXME (AG): what should I do if the snapshot doesn't actually exist? - I need to reset the log to zero so that we can reboot the system

    // TODO (AG): this has to be expanded to deal with non-voting members during reconfiguration
    // TODO (AG): put in a transition() method that enforces this state machine in one location
    /**
     * Current Raft role of a server in the Raft cluster.
     * <p/>
     * Valid roles are:
     * <ul>
     *     <li>{@code FOLLOWER}</li>
     *     <li>{@code CANDIDATE}</li>
     *     <li>{@code LEADER}</li>
     * </ul>
     * A server in the Raft cluster can
     * <strong>only</strong> be in <strong>one</strong>
     * of these roles at a time. Servers transition
     * between these roles through a combination of messages
     * and timeouts. Valid transitions are detailed in the diagram below:
     * <pre>
     * FOLLOWER -> CANDIDATE -> LEADER --+
     *    ^            |                 |
     *    |            |                 |
     *    +------------+                 |
     *    |                              |
     *    |                              |
     *    +------------------------------+
     * </pre>
     * All other transitions are invalid and should result
     * in an {@link java.lang.IllegalStateException}.
     */
    enum Role {

        /**
         * A server in this role responds to, and
         * replicates log entries proposed by the leader
         * in the current election term.
         */
        FOLLOWER,

        /**
         * A server in this role requests votes
         * from other servers in the cluster in order to
         * become the leader in the current
         * election term.
         */
        CANDIDATE,

        /**
         * A server in this role is the
         * <strong>only</strong> one that can append
         * log entries to the replicated log. It
         * is also the <strong>only</strong>
         * one that a client can use to replicate
         * {@code Command} instances.
         */
        LEADER,
    }

    // TODO (AG): See if this same concept can be applied to the AppendEntries processing
    /**
     * Log replication status assigned to each follower
     * server by the leader.
     * <p/>
     * When a server assumes leadership, it attempts to bring
     * the logs of all other servers in the Raft cluster
     * into sync with its own. To achieve this, it:
     * <ol>
     *     <li>Finds a log prefix it has in common with the follower.</li>
     *     <li>Appends its own existing (and newly submitted) entries after that prefix.</li>
     * </ol>
     * This corresponds to the following states:
     * <ol>
     *     <li>{@code PREFIX_SEARCH}</li>
     *     <li>{@code APPLYING}</li>
     * </ol>
     * A follower server in the Raft cluster can
     * <strong>only</strong> be in <strong>one</strong>
     * of these states at a time. Servers transition
     * between these states via AppendEntriesReply
     * messages. Valid transitions are detailed in the diagram below:
     * <pre>
     *     PREFIX_SEARCH +--> APPLYING +
     *           ^       |       ^     |
     *           |       |       |     |
     *           +-------+       +-----+
     * </pre>
     * All other transitions are invalid and should result
     * in an {@link java.lang.IllegalStateException}.
     */
    private enum Phase {

        /**
         * Indicates that the leader server is searching
         * for a log prefix that it has in common with the
         * follower server.
         */
        PREFIX_SEARCH,

        /**
         * Indicates that the leader server has found
         * a log prefix in common with the follower
         * server, and is syncing log entries to the
         * follower's log. Whether the follower server
         * is fully in sync can be determined by comparing
         * its {@code nextIndex}, and the index
         * of the last log entry in the leader server's log.
         */
        APPLYING
    }

    // Holds information about each server in the Raft cluster.
    private final class ServerDatum {

        private long nextIndex;
        private Phase phase;

        private ServerDatum(long nextIndex, Phase phase) {
            this.nextIndex = nextIndex;
            this.phase = phase;
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ServerDatum other = (ServerDatum) o;

            return nextIndex == other.nextIndex && phase == other.phase;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(nextIndex, phase);
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add("nextIndex", nextIndex)
                    .add("phase", phase)
                    .toString();
        }
    }

    // Holds information about pending commands
    // (i.e. commands that the client has submitted to the local server).
    private final class CommandDatum {

        private final LogEntry.ClientEntry clientEntry;
        private final SettableFuture<Void> commandFuture;

        private CommandDatum(LogEntry.ClientEntry clientEntry, SettableFuture<Void> commandFuture) {
            this.clientEntry = clientEntry;
            this.commandFuture = commandFuture;
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CommandDatum other = (CommandDatum) o;
            return clientEntry.equals(other.clientEntry) && commandFuture.equals(other.commandFuture);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(clientEntry, commandFuture);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("clientEntry", clientEntry)
                    .add("commandFuture", commandFuture)
                    .toString();
        }
    }

    // Represents a noop that has been committed to the Raft cluster.
    private static final class ClusterCommittedNoop implements Committed {

        private final long index;

        /**
         * Constructor.
         *
         * @param index index > 0 in the Raft log of the committed noop
         */
        ClusterCommittedNoop(long index) {
            this.index = index;
        }

        @Override
        public Type getType() {
            return Type.SKIP;
        }

        @Override
        public long getIndex() {
            return index;
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ClusterCommittedCommand other = (ClusterCommittedCommand) o;
            return index == other.index;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(index);
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add("index", index)
                    .toString();
        }
    }

    // Represents a command that has been committed to the Raft cluster.
    private static final class ClusterCommittedCommand implements CommittedCommand {

        private final long index;
        private final Command command;

        /**
         * Constructor.
         *
         * @param index index > 0 in the Raft log of the committed {@code Command}
         * @param command the committed {@code Command} instance
         *
         * @see Command
         */
        ClusterCommittedCommand(long index, Command command) {
            this.index = index;
            this.command = command;
        }

        @Override
        public Type getType() {
            return Type.COMMAND;
        }

        @Override
        public long getIndex() {
            return index;
        }

        @Override
        public Command getCommand() {
            return command;
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ClusterCommittedCommand other = (ClusterCommittedCommand) o;

            return index == other.index && command.equals(other.command);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(index, command);
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add("index", index)
                    .add("command", command)
                    .toString();
        }
    }

    // Implementation of TimeoutTask that crashes if the task throws an exception
    private abstract class AlgorithmTimeoutTask implements Timer.TimeoutTask {

        private final String taskName;

        private AlgorithmTimeoutTask(String taskName) {
            this.taskName = taskName;
        }

        @SuppressWarnings("TryWithIdenticalCatches")
        @Override
        public final void run(Timer.TimeoutHandle timeoutHandle) {
            try {
                synchronized (RaftAlgorithm.this) {
                    runSafely(timeoutHandle);
                }
            } catch (RuntimeException e) {
                crash(e);
            } catch (StorageException e) {
                crash(e);
            } catch (Exception e) {
                LOGGER.warn("{}: [{}]: uncaught exception during task execution", self, taskName, e);
            } catch (Throwable t) {
                crash(t);
            }
        }

        protected abstract void runSafely(Timer.TimeoutHandle timeoutHandle) throws Exception;

        private void crash(Throwable t) {
            LOGGER.error("{}: [{}]: uncaught throwable during task execution - terminating", self, taskName, t);
            System.exit(RaftConstants.UNCAUGHT_THROWABLE_EXIT_CODE);
        }
    }

    // TODO (AG): when I incorporate configuration changes, modify all rpc methods to check if the message came from a server in the cluster
    // TODO (AG): when I chunk outgoing entries in AppendEntries I should verify that the commitIndex never greater than min(commitIndex, sent logIndices)
    //            i.e. commitIndex should be the min(commitIndex, serverData.nextIndex - 1 + entryCount)
    // TODO (AG): make calling back listeners safer
    //            - do not hold lock while notifying listeners
    //            - call back on separate thread (maybe?) to better insulate this component from arbitrary errors in foreign code (but what about log, etc.)
    //            - make listener callbacks _last_, after modifying all state

    // common to all roles
    private final Random random;
    private final Timer timer;
    private final RPCSender sender;
    private final Store store;
    private final Log log;
    private final SnapshotsStore snapshotsStore;
    private final RaftListener listener;
    private final String self;
    private final ImmutableSet<String> cluster;

    private boolean initialized = false;
    private boolean running = false;
    private Role role = Role.FOLLOWER;
    private Timer.TimeoutHandle electionTimeoutHandle = null;
    private Timer.TimeoutHandle snapshotTimeoutHandle = null;

    // snapshots
    private final int minEntriesToSnapshot;
    private final long snapshotCheckInterval;

    // timeout values
    private final long rpcTimeout;
    private final long minElectionTimeout;
    private final long additionalElectionTimeoutRange;
    private final long heartbeatInterval;
    private final TimeUnit timeoutTimeUnit;

    // used when you're a follower
    private String leader = null;
    private long nextToApplyLogIndex = -1;

    // used when you're either a candidate or a leader
    private int clusterQuorumSize = 0;

    // used when you're a candidate
    private final Map<String, Boolean> votedServers = Maps.newHashMap();

    // used when you're a leader
    private Timer.TimeoutHandle heartbeatTimeoutHandle = null;
    private final Map<String, ServerDatum> serverData = Maps.newHashMap();
    private final Map<Long, CommandDatum> commands = Maps.newHashMap();

    /**
     * Constructor.
     * <p/>
     * This constructor uses default values from
     * {@link io.libraft.algorithm.RaftConstants} for all timeouts.
     *
     * @param random {@link Random} used to generate message ids, timeout intervals, <em>etc.</em>
     * @param timer {@link Timer} used to schedule schedule tasks for future execution
     * @param sender {@link RPCSender} used to send Raft messages to other Raft servers in the Raft cluster
     * @param store {@link Store} in which Raft metadata is durably persisted
     * @param log {@link Log} in which {@link LogEntry} instances are durably persisted
     * @param snapshotsStore {@link SnapshotsStore} in which {@link io.libraft.Snapshot} instances are durably persisted
     * @param listener {@link RaftListener} that will be notified every time a
     *                 {@link Command} is committed and can be consumed by the client
     * @param self unique id of the local Raft server that instantiated this {@code RaftAlgorithm} instance
     * @param cluster immutable set of unique ids of all the Raft servers in the Raft cluster.
     *                {@code cluster} <strong>must</strong> include {@code self}
     *
     * @throws java.lang.IllegalArgumentException if the cluster configuration is malformed
     */
    public RaftAlgorithm(
            Random random,
            Timer timer,
            RPCSender sender,
            Store store,
            Log log,
            SnapshotsStore snapshotsStore,
            RaftListener listener,
            String self,
            Set<String> cluster) {
        this(random,
             timer,
             sender,
             store,
             log,
                snapshotsStore,
             listener,
             self,
             cluster,
             RaftConstants.SNAPSHOTS_DISABLED,
             RaftConstants.SNAPSHOT_CHECK_INTERVAL,
             RaftConstants.RPC_TIMEOUT,
             RaftConstants.MIN_ELECTION_TIMEOUT,
             RaftConstants.ADDITIONAL_ELECTION_TIMEOUT_RANGE,
             RaftConstants.HEARTBEAT_INTERVAL,
             RaftConstants.TIME_UNIT);
    }

    /**
     * Constructor.
     * <p/>
     * This constructor allows custom values to be specified for all timeouts.
     *
     * @param random {@link java.util.Random} used to generate message ids, timeout periods, <em>etc.</em>
     * @param timer {@link Timer} used to schedule schedule tasks for future execution
     * @param sender {@link RPCSender} used to send Raft messages to other Raft servers in the Raft cluster
     * @param store {@link Store} in which Raft metadata is durably persisted
     * @param log {@link Log} in which {@link LogEntry} instances are durably persisted
     * @param snapshotsStore {@link SnapshotsStore} in which {@link io.libraft.Snapshot} instances are durably persisted
     * @param listener {@link RaftListener} that will be notified every time a
     *                 {@link Command} is committed and can be consumed by the client
     * @param self unique id of the local Raft server that instantiated this {@code RaftAlgorithm} instance
     * @param cluster immutable set of unique ids of all the Raft servers in the Raft cluster.
     *                {@code cluster} <strong>must</strong> include {@code self}
     * @param minEntriesToSnapshot minimum number of {@link LogEntry} instances coalesced into
     *                             a {@link io.libraft.Snapshot}
     * @param snapshotCheckInterval time interval after which to check if the {@link Log} has enough
     *                              {@link LogEntry} for it to be compacted and a {@link io.libraft.Snapshot}
     *                              to be created
     * @param rpcTimeout time after which a Raft server expects a response (RequestVoteReply, AppendEntriesReply)
     *                   to its RPC request (RequestVote, AppendEntries)
     * @param minElectionTimeout minimum time after which a follower Raft server will initiate a new election
     *                           cycle if it does not hear from the leader of the current election term
     * @param additionalElectionTimeoutRange upper bound to the random amount of time added to
     *                                       {@code minElectionTimeout} to generate a random election
     *                                       timeout total
     * @param heartbeatInterval time interval after which the leader server will send an AppendEntries
     *                         message. The next interval starts <strong>only</strong> after
     *                         all messages from the previous interval have been sent
     * @param timeoutTimeUnit {@link TimeUnit} in which all timeouts are specified
     *
     * @throws java.lang.IllegalArgumentException if either the cluster configuration is
     * malformed or the timeout configuration is invalid
     */
    public RaftAlgorithm(
            Random random,
            Timer timer,
            RPCSender sender,
            Store store,
            Log log,
            SnapshotsStore snapshotsStore,
            RaftListener listener,
            String self,
            Set<String> cluster,
            int minEntriesToSnapshot,
            long snapshotCheckInterval,
            long rpcTimeout,
            long minElectionTimeout,
            long additionalElectionTimeoutRange,
            long heartbeatInterval,
            TimeUnit timeoutTimeUnit) {
        checkClusterParameters(self, cluster);
        checkSnapshotParameters(minEntriesToSnapshot);
        checkTimeoutParameters(rpcTimeout, minElectionTimeout, additionalElectionTimeoutRange, heartbeatInterval);

        // set the quorum size _before_ we remove our id from the list
        this.clusterQuorumSize = (int) Math.ceil(((double) cluster.size() / 2));

        // create a local copy of the cluster that doesn't include your id
        Set<String> others = Sets.newHashSet(cluster);
        boolean removed = others.remove(self);

        checkState(removed, "self:%s not removed from cluster:%s", self, cluster);

        this.random = random;
        this.timer = timer;
        this.sender = sender;
        this.store = store;
        this.log = log;
        this.snapshotsStore = snapshotsStore;
        this.listener = listener;
        this.self = self;
        this.cluster = ImmutableSet.copyOf(others);
        this.minEntriesToSnapshot = minEntriesToSnapshot;
        this.snapshotCheckInterval = snapshotCheckInterval;
        this.rpcTimeout = rpcTimeout;
        this.minElectionTimeout = minElectionTimeout;
        this.additionalElectionTimeoutRange = additionalElectionTimeoutRange;
        this.heartbeatInterval = heartbeatInterval;
        this.timeoutTimeUnit = timeoutTimeUnit;
    }

    private void checkClusterParameters(String self, Set<String> cluster) {
        checkArgument(cluster.size() >= 3 && cluster.size() <= 7, "invalid cluster size:%s", cluster.size());
        checkArgument(cluster.contains(self), "missing self:%s in cluster:%s", self, cluster);
    }

    private void checkSnapshotParameters(int minEntriesToSnapshot) {
        checkArgument(minEntriesToSnapshot == RaftConstants.SNAPSHOTS_DISABLED || minEntriesToSnapshot > 0,
                "snapshots should be disabled or the min entries to snapshot should be > 0 value:%s", minEntriesToSnapshot);
    }

    private void checkTimeoutParameters(long rpcTimeout, long minElectionTimeout, long additionalElectionTimeoutRange, long heartbeatInterval) {
        checkArgument(minElectionTimeout > 0);
        checkArgument(additionalElectionTimeoutRange >= 0);
        checkArgument(rpcTimeout > 0);
        checkArgument(heartbeatInterval > 0);

        // in both of these cases we want at least 3 timeouts
        // worth of time to pass before a node triggers an election.
        // this means that we can tolerate a few network packet losses
        // while at the same time being _somewhat_ responsive
        // to long-lived partition events
        checkArgument(rpcTimeout <= (minElectionTimeout / 3));
        checkArgument(heartbeatInterval <= (minElectionTimeout / 3));

        // the full rationale for this check is given in RaftConstants#MIN_ELECTION_TIMEOUT
        // the summary follows:
        //   minElectionTimeout + additionalElectionTimeoutRange = the maxElectionTimeout
        //   maxElectionTimeout is the largest amount of time
        //   for which a node is willing to wait to become a leader.
        //   once it becomes a leader it immediately sends out a heartbeat.
        //   it then sends a heartbeat every heartbeatInterval
        //   this check gives another node enough time to complete an election and send at least 2 heartbeats
        //   confirming its leadership before this node decides to trigger an election.
        //   this mitigates the case where this node may:
        //     1. start an election timeout
        //     2. crash the moment the timeout ends
        //     3. restart immediately without delay
        //     4. trigger an election because it didn't hear from the current leader in a timely fashion
        checkArgument((2 * minElectionTimeout) >= (minElectionTimeout + additionalElectionTimeoutRange + (2 * heartbeatInterval)));
    }

    /**
     * Initialize the {@code RaftAlgorithm instance}
     * <p/>
     * Following a successful call to {@code initialize} a
     * call to {@link RaftAlgorithm#start()} can be made. Any
     * exception thrown by this method indicates that this
     * {@code RaftAlgorithm} instance <strong>should not</strong> be used.
     */
    public synchronized void initialize() throws StorageException {
        checkState(!initialized, "cannot be initialized twice");

        setupPersistentState();

        initialized = true;
    }

    /**
     * Start the {@code RaftAlgorithm} instance.
     * <p/>
     * Following a successful call to {@code start()}
     * {@code RaftAlgorithm} can process incoming Raft messages,
     * participate in the Raft distributed consensus algorithm
     * and (if leader), propose client commands for inclusion in
     * the replicated log. This method is a noop if the
     * system has already been started.
     * <p/>
     * The recommended ordering to start this component and its
     * dependencies/dependents is:
     * <ol>
     *     <li>{@code snapshotStore}</li>
     *     <li>{@code store}</li>
     *     <li>{@code log}</li>
     *     <li>{@code listener}</li>
     *     <li>{@code sender}</li>
     *     <li>{@code timer}</li>
     *     <li>{@code raftAlgorithm <-- this component}</li>
     * </ol>
     */
    public synchronized void start() {
        checkState(initialized, "must be initialized first");

        if (running) {
            return;
        }

        resetState();
        scheduleNextElectionTimeout();
        scheduleNextSnapshotTimeout();

        running = true;
    }

    private void setupPersistentState() throws StorageException {
        ExtendedSnapshot latestSnapshot = snapshotsStore.getLatestSnapshot();
        LogEntry firstLog = log.getFirst();
        LogEntry lastLog = log.getLast();
        if (latestSnapshot == null && lastLog == null) { // first time starting up
            store.setCurrentTerm(0);
            store.setCommitIndex(0);

            log.truncate(0);
            log.put(LogEntry.SENTINEL);

            store.clearVotedFor();
            store.setVotedFor(0, null);
        } else { // do incredibly basic sanity checks (doesn't check if log in good state or if snapshot actually exists)
            // TODO (AG): I would like a method that would return the last votedFor
            long currentTerm = store.getCurrentTerm();
            long commitIndex = store.getCommitIndex();
            checkSnapshotLogAndCommitIndicesAndTerms(latestSnapshot, firstLog, lastLog, currentTerm, commitIndex);
        }
    }

    /**
     * Stop the {@code RaftAlgorithm} instance.
     * <p/>
     * If any method call is <strong>already</strong> in progress
     * it <strong>will</strong> complete. All
     * pending timeouts are cancelled and no more will be
     * scheduled. Any messages still inbound via {@code RPCReceiver}
     * are dropped silently.
     * <p/>
     * If this server is the leader server all incomplete
     * futures returned by {@link Raft#submitCommand(io.libraft.Command)}
     * <strong>will</strong> be cancelled. This <strong>does not</strong>
     * indicate that the client {@code Command} was not replicated.
     * Clients <strong>should</strong> use application-level protocols
     * to determine how much progress was made before the call to
     * {@code stop()}. Subsequent calls to
     * {@link Raft#submitCommand(io.libraft.Command)} <strong>will</strong>
     * throw an {@link java.lang.IllegalStateException}.
     */
    public synchronized void stop() {
        if (!running) {
            return;
        }

        stopElectionTimeout();
        stopSnapshotTimeout();
        stopHeartbeatTimeout();

        running = false;
    }

    /**
     * Get the {@link io.libraft.algorithm.RaftAlgorithm.Role} this server is in.
     *
     * @return {@code Role} this server is in.
     */
    synchronized Role getRole() {
        return role;
    }

    /**
     * Get the unique id of the current leader server if known.
     *
     * @return unique id of the current leader server, if known. {@code null} otherwise.
     */
    synchronized @Nullable String getLeader() {
        return leader;
    }

    /**
     * Get the nextIndex for the specified {@code server} in the Raft cluster.
     *
     * @param server unique id of the server for which nextIndex should be returned
     * @return log index >= 0 of the nextIndex for the server
     */
    synchronized long getNextIndex(String server) {
        checkState(role == Role.LEADER, "role:%s", role);
        checkState(self.equals(leader), "self:%s leader:%s", self, leader);
        checkState(serverData.containsKey(server), "server:%s", server);

        return serverData.get(server).nextIndex;
    }

    private void logNotRunning() {
        LOGGER.warn("{}: algorithm has been stopped", self);
    }

    private void resetState() {
        role = Role.FOLLOWER;
        electionTimeoutHandle = null;
        snapshotTimeoutHandle = null;
        leader = null;
        nextToApplyLogIndex = -1;
        votedServers.clear();
        heartbeatTimeoutHandle = null;
        serverData.clear();
        failAllOutstandingCommands();
    }

    private void failAllOutstandingCommands() {
        Collection<CommandDatum> failedCommands = ImmutableList.copyOf(commands.values());
        commands.clear();

        for (CommandDatum commandDatum : failedCommands) {
            commandDatum.commandFuture.setException(new ReplicationException(commandDatum.clientEntry.getCommand()));
        }
    }

    private void stopElectionTimeout() {
        if (electionTimeoutHandle != null) {
            electionTimeoutHandle.cancel();
            electionTimeoutHandle = null;
        }
    }

    private void stopSnapshotTimeout() {
        if (snapshotTimeoutHandle != null) {
            snapshotTimeoutHandle.cancel();
            snapshotTimeoutHandle = null;
        }
    }

    private void stopHeartbeatTimeout() {
        if (heartbeatTimeoutHandle != null) {
            heartbeatTimeoutHandle.cancel();
            heartbeatTimeoutHandle = null;
        }
    }

    private void scheduleNextElectionTimeout() {
        stopElectionTimeout();

        long electionTimeout;
        if (additionalElectionTimeoutRange > 0) {
            electionTimeout = minElectionTimeout + random.nextInt((int) additionalElectionTimeoutRange);
        } else {
            electionTimeout = minElectionTimeout;
        }

        AlgorithmTimeoutTask electionTimeoutTask = new AlgorithmTimeoutTask("election timeout task") {
            @Override
            protected void runSafely(Timer.TimeoutHandle timeoutHandle) throws Exception {
                if (timeoutHandle != electionTimeoutHandle) {
                    LOGGER.warn("{}: election timeout task cancelled");
                    return;
                }

                handleElectionTimeout();
            }
        };

        electionTimeoutHandle = timer.newTimeout(electionTimeoutTask, electionTimeout, timeoutTimeUnit);
    }

    private void handleElectionTimeout() throws StorageException {
        LOGGER.info("{}: handle election timeout", self); // TODO (AG): specify term

        if (!running) {
            logNotRunning();
            return;
        }

        switch (role) {
            case CANDIDATE:
                role = Role.FOLLOWER;
                // fallthrough
            case FOLLOWER:
                beginElection();
                break;
        }
    }

    // bail
    // if there's something wrong with the
    // persistence layer, I don't know, and don't want to know
    // how to deal with it. the system should not attempt to continue
    // because we have no idea exactly what happened and whether
    // it's recoverable or not
    private void handleStorageException(StorageException e) {
        throw new RaftError(e);
    }

    private void beginElection() throws StorageException {
        checkState(role == Role.FOLLOWER, "role:%s", role);

        long currentTerm = store.getCurrentTerm() + 1;
        becomeCandidate(currentTerm);

        LogEntry lastLog = checkNotNull(log.getLast());
        sendRequestVoteRPCs(currentTerm, lastLog.getIndex(), lastLog.getTerm());
    }

    private void sendRequestVoteRPCs(final long electionTerm, final long lastLogIndex, final long lastLogTerm) {
        AlgorithmTimeoutTask rpcTimeoutTask = new AlgorithmTimeoutTask("request vote rpc timeout") {
            @Override
            protected void runSafely(Timer.TimeoutHandle timeoutHandle) throws Exception {
                handleRequestVoteRPCTimeout(electionTerm, lastLogIndex, lastLogTerm);
            }
        };

        timer.newTimeout(rpcTimeoutTask, rpcTimeout, timeoutTimeUnit);

        for (String server : cluster) {
            // this check is not strictly necessary
            // we're resilient to duplicate RequestVote
            if (!votedServers.containsKey(server)) {
                try {
                    sender.requestVote(server, electionTerm, lastLogIndex, lastLogTerm);
                } catch (RPCException e) {
                    LOGGER.warn("{}: fail send RequestVote to {} for term {} cause:{}", self, server, electionTerm, e.getMessage());
                }
            }
        }
    }

    private void handleRequestVoteRPCTimeout(long electionTerm, long lastLogIndex, long lastLogTerm) throws StorageException {
        LOGGER.trace("{}: handle RequestVote RPC timeout for term {}", self, electionTerm);

        if (!running) {
            logNotRunning();
            return;
        }

        long currentTerm = store.getCurrentTerm();

        if (currentTerm > electionTerm) {
            return;
        }

        if (role != Role.CANDIDATE) { // may have voted for a candidate with a dominant log or, found a leader
            return;
        }

        checkState(currentTerm == electionTerm, "currentTerm:%s electionTerm:%s", currentTerm, electionTerm);
        checkState(role == Role.CANDIDATE, "role:%s", role);

        int voteCount = countGrantedVotes();
        if (voteCount < clusterQuorumSize) {
            sendRequestVoteRPCs(electionTerm, lastLogIndex, lastLogTerm);
        }
    }

    // becomeFollower has become 3 methods because
    // when updating the term I want to log a role change
    // and stop the heartbeat _before_ doing a db operation
    // and setting state. in the other case I want to
    // log a role change and stop the heartbeat before
    // setting state _only_. both these cases are triggered in
    // different circumstances, so, instead of using a
    // single method with a boolean flag, I chose to split it
    // into three with a common core (setFollowerState) and
    // two clearly-labelled entry points

    private void becomeFollowerWithoutUpdatingCurrentTerm(long currentTerm, @Nullable String newLeader) {
        logRoleChange(currentTerm, role, Role.FOLLOWER);

        stopHeartbeatTimeout();

        setFollowerState(newLeader);
    }

    // NOTE: in all following become{Role} methods, there's really no
    // need to explicitly stop the heartbeat timeouts, because the next time
    // the heartbeat is triggered the timeout task will notice that the
    // currentTerm has changed and cancel that timeout instance
    //
    // also, these methods are package-private for _unit-test use_ only!

    /**
     * Transition this server from {@link Role#CANDIDATE} or {@link Role#LEADER} to {@link Role#FOLLOWER}.
     * <p/>
     * <strong>This method is package-private for testing
     * reasons only!</strong> It should <strong>never</strong>
     * be called in a non-test context!
     *
     * @param newCurrentTerm new election term for this {@code RaftAlgorithm} instance
     * @param newLeader unique id of the leader server if known. {@code null} otherwise
     */
    synchronized void becomeFollower(long newCurrentTerm, @Nullable String newLeader) throws StorageException {
        long currentTerm = store.getCurrentTerm();

        checkArgument(currentTerm < newCurrentTerm, "currentTerm:%s newCurrentTerm:%s", currentTerm, newCurrentTerm);

        logRoleChange(newCurrentTerm, role, Role.FOLLOWER);

        stopHeartbeatTimeout();

        store.setCurrentTerm(newCurrentTerm);

        setFollowerState(newLeader);
    }

    // DO NOT CALL THIS METHOD DIRECTLY!
    private void setFollowerState(@Nullable String newLeader) {
        role = Role.FOLLOWER;

        setLeader(newLeader);

        nextToApplyLogIndex = -1;
        votedServers.clear();
        serverData.clear();
        failAllOutstandingCommands();

        scheduleNextElectionTimeout();
    }

    private void setLeader(@Nullable String newLeader) {
        String oldLeader = leader;
        leader = newLeader;
        if ((oldLeader == null && newLeader != null) || (oldLeader != null && !oldLeader.equals(newLeader))) {
            try {
                LOGGER.info("{}: leader changed from {} to {}", self, oldLeader, newLeader);
                listener.onLeadershipChange(leader);
            } catch (Exception e) {
                LOGGER.warn("{}: listener throw exception when notified of leadership change", self, e);
            }
        }
    }

    /**
     * Transition this server from {@link Role#FOLLOWER} to {@link Role#CANDIDATE}.
     * <p/>
     * <strong>This method is package-private for testing
     * reasons only!</strong> It should <strong>never</strong>
     * be called in a non-test context!
     *
     * @param newCurrentTerm new election term for this {@code RaftAlgorithm} instance
     */
    synchronized void becomeCandidate(long newCurrentTerm) throws StorageException {
        long currentTerm = store.getCurrentTerm();

        checkArgument(currentTerm < newCurrentTerm, "currentTerm:%s newCurrentTerm:%s", currentTerm, newCurrentTerm);
        checkState(commands.isEmpty(), "commands:%s", commands);
        checkState(role == Role.FOLLOWER, "invalid transition from %s -> %s", role, Role.CANDIDATE);

        logRoleChange(newCurrentTerm, role, Role.CANDIDATE);

        stopHeartbeatTimeout();

        store.setCurrentTerm(newCurrentTerm);

        role = Role.CANDIDATE;

        setLeader(null);

        nextToApplyLogIndex = -1;
        serverData.clear();

        votedServers.clear();
        votedServers.put(self, true);
        store.setVotedFor(newCurrentTerm, self);

        scheduleNextElectionTimeout();
    }

    /**
     * Transition this server from {@link Role#CANDIDATE} to {@link Role#LEADER}.
     * <p/>
     * <strong>This method is package-private for testing
     * reasons only!</strong> It should <strong>never</strong>
     * be called in a non-test context!
     *
     * @param expectedCurrentTerm election term in which this {@code RaftAlgorithm} instance should
     *                            be leader. This method expects that {@link io.libraft.algorithm.Store#getCurrentTerm()}
     *                            will return {@code expectedCurrentTerm}.
     */
    synchronized void becomeLeader(long expectedCurrentTerm) throws StorageException {
        long currentTerm = store.getCurrentTerm();

        checkArgument(currentTerm == expectedCurrentTerm, "currentTerm:%s expectedCurrentTerm:%s", currentTerm, expectedCurrentTerm);

        String votedFor = store.getVotedFor(expectedCurrentTerm);
        LogEntry lastLog = checkNotNull(log.getLast());

        checkState(leader == null, "leader:%s", leader);
        checkState(checkNotNull(votedFor).equals(self), "currentTerm:%s votedFor:%s", currentTerm, votedFor);
        checkState(lastLog.getTerm() < currentTerm, "currentTerm:%s lastLog:%s", currentTerm, lastLog);
        checkState(commands.isEmpty(), "commands:%s", commands);
        checkState(role == Role.CANDIDATE, "invalid transition from %s -> %s", role, Role.LEADER);

        logRoleChange(currentTerm, role, Role.LEADER);

        stopElectionTimeout();

        stopHeartbeatTimeout();

        role = Role.LEADER;

        setLeader(self);

        nextToApplyLogIndex = -1;
        votedServers.clear();

        long lastLogIndex = lastLog.getIndex();

        for (String member : cluster) {
            // start off by initializing nextIndex to our belief of their prefix
            // notice that it does _not_ include the NOOP entry that we're just about to add
            serverData.put(member, new ServerDatum(lastLogIndex + 1, Phase.PREFIX_SEARCH));
        }

        // add a NOOP entry
        // essentially, this allows me to start the synchronizing process early
        // it may be that there are people out there with more entries than I,
        // or fewer entries than I. by starting off early I can get everyone up
        // to speed more quickly and get a more accurate picture of their prefixes
        log.put(new LogEntry.NoopEntry(lastLogIndex + 1, currentTerm));

        // send out the first heartbeat
        heartbeat(currentTerm);
    }

    private void logRoleChange(long currentTerm, Role oldRole, Role newRole) {
        if (oldRole != newRole) {
            LOGGER.info("{}: changing role {}->{} in term {}", self, oldRole, newRole, currentTerm);
        }
    }

    private void heartbeat(final long currentTerm) throws StorageException {
        AlgorithmTimeoutTask heartbeatTimeoutTask = new AlgorithmTimeoutTask("heartbeat") {
            @Override
            protected void runSafely(Timer.TimeoutHandle timeoutHandle) throws Exception {
                if (timeoutHandle != heartbeatTimeoutHandle) {
                    LOGGER.warn("{}: heartbeat task cancelled for term:{}", self, currentTerm);
                    return;
                }

                handleHeartbeatTimeout(currentTerm);
            }
        };

        long commitIndex = store.getCommitIndex();
        LogEntry lastLog = checkNotNull(log.getLast());

        checkState(commitIndex <= lastLog.getIndex());

        long nextIndex;
        long prevLogIndex;
        long prevLogTerm;
        Collection<LogEntry> entries;
        for (String server : cluster) {
            nextIndex = serverData.get(server).nextIndex;

            if (nextIndex == (lastLog.getIndex() + 1)) { // we believe their prefix matches (standard heartbeat)
                prevLogIndex = lastLog.getIndex();
                prevLogTerm = lastLog.getTerm();
                entries = null;
            } else { // prefix mismatch (they need to be caught up), or, we don't know yet
                LogEntry prevLog = checkNotNull(log.get(nextIndex - 1));

                prevLogIndex = prevLog.getIndex();
                prevLogTerm = prevLog.getTerm();

                long entryCount = lastLog.getIndex() - prevLogIndex;

                // NOTE: when sending entries, you never include the prevLog.
                // This is because the entries being added come _after_ prevLog, and
                // prevLog{Index,Term} are simply used as safety checks
                entries = Lists.newArrayListWithCapacity((int) entryCount);
                for (long logIndex = prevLogIndex + 1; logIndex <= prevLogIndex + entryCount; logIndex++) {
                    entries.add(log.get(logIndex));
                }
            }

            try {
                sender.appendEntries(server, currentTerm, commitIndex, prevLogIndex, prevLogTerm, entries);
            } catch (RPCException e) {
                LOGGER.warn("{}: fail send heartbeat with {} entries to {} cause:{}", self, getEntryCount(entries), server, e.getMessage());
            }
        }

        heartbeatTimeoutHandle = timer.newTimeout(heartbeatTimeoutTask, heartbeatInterval, timeoutTimeUnit);
    }

    private void handleHeartbeatTimeout(long currentTerm) throws StorageException {
        LOGGER.trace("{}: heartbeat task for term {}", self, currentTerm);

        if (!running) {
            logNotRunning();
            return;
        }

        long actualTerm = store.getCurrentTerm();
        if (actualTerm != currentTerm) {
            return;
        }

        checkState(role == Role.LEADER, "role:%s", role);
        checkState(self.equals(leader), "self:%s leader:%s", self, leader);

        heartbeat(currentTerm);
    }

    //----------------------------------------------------------------------------------------------------------------//
    //
    // RequestVote
    //

    @Override
    public synchronized void onRequestVote(String server, long term, long lastLogIndex, long lastLogTerm) {
        LOGGER.trace("{}: RequestVote from {}: term:{} lastLogIndex:{} lastLogTerm:{}", self, server, term, lastLogIndex, lastLogTerm);

        if (!running) {
            logNotRunning();
            return;
        }

        try {
            checkArgument(term >= 1);
            checkArgument(lastLogIndex >= 0);
            checkArgument(lastLogTerm >= 0);

            long currentTerm = store.getCurrentTerm();

            if (term < currentTerm) {
                LOGGER.trace("{}: RequestVote from {}: old term: term:{} currentTerm:{}", self, server, term, currentTerm);
                sendRequestVoteReplyIgnoreRPCException(server, term, currentTerm, false);
                return;
            }

            if (term > currentTerm) {
                String votedFor = store.getVotedFor(term);
                checkState(votedFor == null, "voted in future term:%s for:%s", term, votedFor);

                becomeFollower(term, null);
                currentTerm = store.getCurrentTerm(); // IMPORTANT: UPDATING currentTerm!!!
            }

            checkState(term == currentTerm, "term:%s currentTerm:%s", term, currentTerm);

            String votedFor = store.getVotedFor(term);
            LogEntry selfLastLog = checkNotNull(log.getLast());
            int candidateLogDominates = doesLogDominate(lastLogIndex, lastLogTerm, selfLastLog.getIndex(), selfLastLog.getTerm());
            boolean voteGranted = false;

            // you can grant the vote if:
            // - there's no leader for this term
            // - you are in a position to grant a vote (you've voted for no one, or yourself)
            // - the candidate's log prefix >= yours
            //
            // NOTE: the actual condition below is a slight optimization, in that if you've
            // already voted for yourself (i.e. you're a candidate) you don't
            // grant a vote to someone else unless their log strictly dominates yours.
            // this prevents a latecomer with an identical log as yours from delaying an election result

            if (leader == null && ((votedFor == null && candidateLogDominates >= 0) || (self.equals(votedFor) && candidateLogDominates == 1))) {
                // TODO (AG): it's not clear to me whether I should set votedFor before or after updateFollowerState
                becomeFollowerWithoutUpdatingCurrentTerm(currentTerm, null); // don't set the leader
                store.setVotedFor(term, server);
                voteGranted = true;
            }

            sendRequestVoteReplyIgnoreRPCException(server, currentTerm, currentTerm, voteGranted);
        } catch (StorageException e) {
            handleStorageException(e);
        }
    }

    private void sendRequestVoteReplyIgnoreRPCException(String server, long term, long currentTerm, boolean voteGranted) {
        try {
            sender.requestVoteReply(server, currentTerm, voteGranted);
        } catch (RPCException e) {
            LOGGER.warn("{}: RequestVote from {}: fail send RequestVoteReply for term:{} cause:{}", self, server, term, e.getMessage());
        }
    }

    private int doesLogDominate(long lastLogIndex0, long lastLogTerm0, long lastLogIndex1, long lastLogTerm1) {
        if (lastLogTerm0 < lastLogTerm1) {
            return -1;
        }

        if (lastLogTerm0 > lastLogTerm1) {
            return 1;
        }

        if (lastLogIndex0 < lastLogIndex1) {
            return -1;
        } else if (lastLogIndex0 == lastLogIndex1) {
            return 0;
        } else {
            return 1;
        }
    }

    //----------------------------------------------------------------------------------------------------------------//
    //
    // RequestVoteReply
    //

    @Override
    public synchronized void onRequestVoteReply(String server, long term, boolean voteGranted) {
        LOGGER.trace("{}: RequestVoteReply from {}: term:{} voteGranted:{}", self, server, term, voteGranted);

        if (!running) {
            logNotRunning();
            return;
        }

        try {
            checkState(term >= 1);

            long currentTerm = store.getCurrentTerm();

            if (term > currentTerm) {
                becomeFollower(term, null);
                return;
            }

            // this is a vote in the current term
            // that can impact if we become a leader or not
            if (term == currentTerm && role == Role.CANDIDATE) {
                Boolean previousVote = votedServers.put(server, voteGranted);
                if (previousVote != null) { // duplicate RPC
                    checkState(previousVote == voteGranted, "rescinded vote: server:%s previousVote:%s voteGranted:%s", server, previousVote, voteGranted);
                    return;
                }

                if (countGrantedVotes() >= clusterQuorumSize) {
                    becomeLeader(currentTerm);
                }
            }
        } catch (StorageException e) {
            handleStorageException(e);
        }
    }

    private int countGrantedVotes() {
        int voteCount = 0;

        for(Map.Entry<String, Boolean> entry : votedServers.entrySet()) {
            if (entry.getValue().equals(Boolean.TRUE)) {
                voteCount++;
            }
        }

        return voteCount;
    }

    //----------------------------------------------------------------------------------------------------------------//
    //
    // AppendEntries
    //

    @Override
    public synchronized void onAppendEntries(String server, long term, long commitIndex, long prevLogIndex, long prevLogTerm, @Nullable Collection<LogEntry> entries) {
        LOGGER.trace("{}: AppendEntries from {}: term:{} commitIndex:{} prevLogIndex:{} prevLogTerm:{} entryCount:{}",
                self, server, term, commitIndex, prevLogIndex, prevLogTerm, getEntryCount(entries));

        if (!running) {
            logNotRunning();
            return;
        }

        try {
            checkArgument(term >= 1);
            checkArgument(commitIndex >= 0);
            checkArgument(prevLogIndex >= 0);
            checkArgument(prevLogTerm >= 0);
            checkArgument(entries == null || entries.size() > 0);

            // deal with heartbeats specially by creating an empty
            // list, so I don't have to have a giant if block in the
            // log application code
            if (entries == null) {
                entries = Collections.emptyList();
            }

            long entryCount = entries.size();
            long currentTerm = store.getCurrentTerm();
            long selfCommitIndex = store.getCommitIndex();

            if (term < currentTerm) {
                // this is an unusual case that can happen if a server sends out
                // an AppendEntries for an index, crashes, gets re-elected as leader,
                // and sends out a new AppendEntries for that same index. If the
                // older AppendEntries is delivered after we receive knowledge of the
                // election we will respond with a NACK, causing the leader to think
                // that we're NACKing the new AppendEntries and causing an unnecessary
                // message exchange
                //
                // since we already know that this is an old message, and we also
                // know that the sender knows this (after all, they are the leader!)
                // we can safely ignore this
                // this is one of those cases where it's actually better to be silent
                // if you aren't you'll reply with something like:
                // leader, currentTerm, prevLogIndex, entryCount, applied=false
                // if (unfortunately) the leader is attempting to send you entries that start
                // at that prefix it'll mistakenly bump down your prevLogIndex
                if (leader != null && leader.equals(server)) {
                    LOGGER.trace("{}: AppendEntries from {}: late request", self, server);
                    return;
                }

                LOGGER.trace("{}: AppendEntries from {}: term < currentTerm: term:{} currentTerm:{}", self, server, term, currentTerm);
                sendAppendEntriesReplyIgnoreRPCException(server, currentTerm, prevLogIndex, entryCount, false);
                return;
            }

            if (term > currentTerm) {
                becomeFollower(term, server);
                currentTerm = store.getCurrentTerm(); // UPDATING currentTerm
            }

            // this can happen if you've crashed and restarted within a term
            // on restart you are in the FOLLOWER role, (because the election timeout hasn't triggered)
            // yet you don't know who the current leader is yet
            if (role == Role.FOLLOWER && leader == null) {
                checkState(term == currentTerm, "term:%s currentTerm:%s", term, currentTerm);
                becomeFollowerWithoutUpdatingCurrentTerm(term, server);
            }

            if (role != Role.FOLLOWER) {
                checkState(role == Role.CANDIDATE);
                becomeFollowerWithoutUpdatingCurrentTerm(term, server);
            }

            scheduleNextElectionTimeout();

            LogEntry prevLog = log.get(prevLogIndex);
            if (prefixMismatch(prevLog, prevLogTerm)) {
                LOGGER.trace("{}: prefix mismatch at index:{} expected term:{} entry:{}", self, prevLogIndex, prevLogTerm, prevLog);
                sendAppendEntriesReplyIgnoreRPCException(server, currentTerm, prevLogIndex, entryCount, false);
                return;
            } else {
                // reassigned to prevent IDEA warning in for... loop
                // done here, because this is the earliest place we can
                // say conclusively that prevLog is not null
                prevLog = checkNotNull(prevLog);

                if (nextToApplyLogIndex == -1) {
                    nextToApplyLogIndex = Math.max(selfCommitIndex + 1, prevLogIndex + 1);
                }
            }

            log.truncate(nextToApplyLogIndex);

            for (LogEntry entry : entries) {
                entry = checkNotNull(entry);
                checkArgument(entry.getIndex() == prevLog.getIndex() + 1, "entries has hole: entry:%s prevLog:%s", entry, prevLog);

                prevLog = entry;

                if (entry.getIndex() != nextToApplyLogIndex) {
                    LogEntry skippedLog = log.get(entry.getIndex());
                    checkState(entry.equals(skippedLog), "mismatch at index:%s entry:%s logEntry:%s", entry.getIndex(), entry, skippedLog);
                } else {
                    LOGGER.trace("{}: add entry:{}", self, entry);
                    log.put(entry);
                    nextToApplyLogIndex = nextToApplyLogIndex + 1;
                }
            }

            // TODO (AG): consider not sending responses for known duplicates
            // These would be messages that have a lower commitIndex than ours,
            // and for which we _don't_ apply any entries

            // it's actually safe to send our reply before updating commitIndex
            // that's because the reply is only signals that our log matches. The
            // receiver has no expectations as to the health of our commitIndex
            sendAppendEntriesReplyIgnoreRPCException(server, currentTerm, prevLogIndex, entryCount, true);

            long originalSelfCommitIndex = selfCommitIndex;
            long possibleSelfCommitIndex = Math.min(nextToApplyLogIndex -1 , commitIndex);

            if (possibleSelfCommitIndex > selfCommitIndex) {
                store.setCommitIndex(possibleSelfCommitIndex);
                selfCommitIndex = store.getCommitIndex(); // UPDATING selfCommitIndex
                setCommandFuturesAndNotifyClient(originalSelfCommitIndex + 1, selfCommitIndex);
            }
        } catch (StorageException e) {
            handleStorageException(e);
        }
    }

    private void sendAppendEntriesReplyIgnoreRPCException(String server, long currentTerm, long prevLogIndex, long entryCount, boolean applied) {
        try {
            sender.appendEntriesReply(server, currentTerm, prevLogIndex, entryCount, applied);
        } catch (RPCException e) {
            LOGGER.warn("{}: fail send AppendEntriesReply to {} cause:{}", self, server, e.getMessage());
        }
    }

    private boolean prefixMismatch(@Nullable LogEntry prevLog, long expectedPrevLogTerm) {
        return (prevLog == null) || (prevLog.getTerm() != expectedPrevLogTerm);
    }

    private void setCommandFuturesAndNotifyClient(long firstNewCommittedIndex, long lastNewCommittedIndex) throws StorageException {
        for (long logIndex = firstNewCommittedIndex; logIndex <= lastNewCommittedIndex; logIndex++) {
            LogEntry logEntry = checkNotNull(log.get(logIndex));

            // trigger the command future if it exists
            CommandDatum commandDatum = commands.remove(logIndex);
            if (commandDatum != null) {
                // only leaders should have outstanding command futures
                checkState(role == Role.LEADER, "%s: role:%s", self, role);
                // the leader should never submit different commands for the same index, so, let's verify this
                checkState(commandDatum.clientEntry.equals(logEntry), "%s: overwrote command at %s: expected:%s actual:%s", self, logIndex, commandDatum.clientEntry, logEntry);
                // finally, let's trigger the future
                commandDatum.commandFuture.set(null);
            }

            // create the object we want to notify the client of
            Committed committed = convertLogEntryToCommitted(logEntry);
            checkState(committed != null, "unsupported log entry type:%s", logEntry.getType().name());

            // notify the client
            try {
                listener.applyCommitted(committed);
            } catch (Exception e) {
                throw new RaftError(String.format("fail notify listener of committed %s at index %d", committed, committed.getIndex()), e);
            }
        }
    }

    private static Committed convertLogEntryToCommitted(LogEntry currentLog) {
        Committed committed = null;

        if (currentLog.getType() == LogEntry.Type.CLIENT) {
            committed = new ClusterCommittedCommand(currentLog.getIndex(), ((LogEntry.ClientEntry) currentLog).getCommand());
        } else if (currentLog.getType() == LogEntry.Type.NOOP) {
            committed = new ClusterCommittedNoop(currentLog.getIndex());
        }

        return committed;
    }

    //----------------------------------------------------------------------------------------------------------------//
    //
    // AppendEntriesReply
    //

    @Override
    public synchronized void onAppendEntriesReply(String server, long term, long prevLogIndex, long entryCount, boolean applied) {
        LOGGER.trace("{}: AppendEntriesReply from {}: term:{} prevLogIndex:{} entryCount:{} applied:{}", self, server, term, prevLogIndex, entryCount, applied);

        if (!running) {
            logNotRunning();
            return;
        }

        try {
            checkArgument(term >= 1);
            checkArgument(prevLogIndex >= 0);
            checkArgument(entryCount >= 0);

            long currentTerm = store.getCurrentTerm();

            if (term < currentTerm) {
                LOGGER.warn("{}: AppendEntriesReply from {}: ignore: term:{} currentTerm:{}", self, server, term, currentTerm);
                return;
            }

            if (term > currentTerm) {
                LOGGER.warn("{}: AppendEntriesReply from {}: become follower: term:{} currentTerm:{}", self, server, term, currentTerm);
                becomeFollower(term, null);
                return;
            }

            checkState(role == Role.LEADER, "role:%s", role);
            checkState(self.equals(leader), "self:%s leader:%s", self, leader);

            ServerDatum serverDatum = checkNotNull(serverData.get(server), "no server data for %s", server);

            if (serverDatum.phase == Phase.PREFIX_SEARCH) {
                if (prevLogIndex + 1 == serverDatum.nextIndex) {
                    if (applied) {
                        serverDatum.phase = Phase.APPLYING;
                    } else {
                        checkArgument(prevLogIndex > 0);
                        serverDatum.nextIndex--; // TODO (AG): immediately send out AppendEntries
                        return;
                    }
                } else {
                    return; // ignore out-of-order reply
                }
            }

            checkState(serverDatum.phase == Phase.APPLYING);
            checkState(applied);

            long lastAppliedIndex = prevLogIndex + entryCount;

            // check that they aren't applying more entries than we know of
            LogEntry lastLog = checkNotNull(log.getLast());
            checkArgument(lastAppliedIndex <= lastLog.getIndex(), "lastAppliedIndex:%s lastLog:%s", lastAppliedIndex, lastLog);

            if (lastAppliedIndex >= serverDatum.nextIndex) {
                serverDatum.nextIndex = lastAppliedIndex + 1;
            } else {
                return; // this reply contains no new information
            }

            long originalCommitIndex = store.getCommitIndex();
            long possibleCommitIndex = findPossibleCommitIndex(lastLog, originalCommitIndex);

            if (possibleCommitIndex > originalCommitIndex) {
                LogEntry potentialCommittedLog = checkNotNull(log.get(possibleCommitIndex), "possibleCommitIndex:%s", possibleCommitIndex);
                checkState(potentialCommittedLog.getTerm() <= currentTerm, "future log:%s", potentialCommittedLog);

                if (potentialCommittedLog.getTerm() == currentTerm) {
                    store.setCommitIndex(possibleCommitIndex);
                    setCommandFuturesAndNotifyClient(originalCommitIndex + 1, possibleCommitIndex);
                }
            }
        } catch (StorageException e) {
            handleStorageException(e);
        }
    }

    private long findPossibleCommitIndex(LogEntry lastLog, long originalCommitIndex) {
        ArrayList<Long> indices = Lists.newArrayListWithCapacity(serverData.size());

        indices.add(lastLog.getIndex());
        for (ServerDatum serverDatum : serverData.values()) {
            if (serverDatum.phase == Phase.APPLYING) {
                indices.add(serverDatum.nextIndex - 1);
            }
        }

        if (indices.size() < clusterQuorumSize) {
            return originalCommitIndex;
        }

        Collections.sort(indices);

        long possibleCommitIndex = indices.get(indices.size() - clusterQuorumSize);
        possibleCommitIndex = Math.max(possibleCommitIndex, originalCommitIndex);
        return possibleCommitIndex;
    }

    //----------------------------------------------------------------------------------------------------------------//
    //
    // client entry-point
    //

    // FIXME (AG): It _may not_ be necessary to verify log state before every snapshot/getCommitted operation

    //----------------------------------------------------------------------------------------------------------------//
    //
    // snapshots
    //

    private void scheduleNextSnapshotTimeout() {
        if (minEntriesToSnapshot == RaftConstants.SNAPSHOTS_DISABLED) {
            LOGGER.trace("{}: snapshots disabled - skip schedule");
            return;
        }

        stopSnapshotTimeout();

        AlgorithmTimeoutTask snapshotTimeoutTask = new AlgorithmTimeoutTask("snapshot timeout task") {
            @Override
            protected void runSafely(Timer.TimeoutHandle timeoutHandle) throws Exception {
                if (timeoutHandle != snapshotTimeoutHandle) {
                    LOGGER.warn("{}: snapshot timeout task cancelled");
                    return;
                }

                handleSnapshotTimeout();
            }
        };

        snapshotTimeoutHandle = timer.newTimeout(snapshotTimeoutTask, snapshotCheckInterval, timeoutTimeUnit);
    }

    private void handleSnapshotTimeout() throws StorageException {
        LOGGER.info("{}: handle snapshot timeout", self);

        if (!running) {
            logNotRunning();
            return;
        }

        checkState(minEntriesToSnapshot != RaftConstants.SNAPSHOTS_DISABLED, "snapshots disabled");

        long commitIndex = store.getCommitIndex();
        long firstLogIndex = 0; // NOTE: we do not count the SENTINEL as an entry that can be committed FIXME (AG): is this valid?

        ExtendedSnapshot latestSnapshot = snapshotsStore.getLatestSnapshot();
        if (latestSnapshot != null) {
            firstLogIndex = latestSnapshot.getIndex();
        }

        // NOTE: the listener may not have applied all the committed entries - we'll check this when they submit their snapshot
        if ((commitIndex - firstLogIndex) >= minEntriesToSnapshot) {
            SnapshotsStore.ExtendedSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
            snapshotWriter.setIndex(RaftConstants.INITIAL_SNAPSHOT_WRITER_LOG_INDEX);
            listener.writeSnapshot(snapshotWriter);
        }

        scheduleNextSnapshotTimeout();
    }

    @Override
    public synchronized void snapshotWritten(SnapshotWriter snapshotWriter) {
        try {
            LOGGER.trace("{}: snapshot created for {}", self, snapshotWriter);

            checkState(running);

            checkArgument(snapshotWriter instanceof ExtendedSnapshotWriter, "unknown SnapshotWriter type:%s", snapshotWriter.getClass().getSimpleName());
            ExtendedSnapshotWriter extendedSnapshotWriter = (ExtendedSnapshotWriter) snapshotWriter;

            // check that they actually attempted to set the last applied index
            long lastAppliedIndex = extendedSnapshotWriter.getIndex();
            checkArgument(lastAppliedIndex >= 0, "lastAppliedIndex:%s", lastAppliedIndex);

            if (lastAppliedIndex == 0) {
                LOGGER.trace("{}: noop snapshot for {}", self, snapshotWriter);
                return;
            }

            // bounds checks
            // check that they haven't claimed to apply more entries than committed
            ExtendedSnapshot latestSnapshot = snapshotsStore.getLatestSnapshot();
            LogEntry firstLog = log.getFirst();
            LogEntry lastLog = log.getLast();
            long currentTerm = store.getCurrentTerm();
            long commitIndex = store.getCommitIndex();

            checkArgument(lastAppliedIndex > 0, "lastAppliedIndex:%s", lastAppliedIndex); // already dealt with '0' case
            checkSnapshotLogAndCommitIndicesAndTerms(latestSnapshot, firstLog, lastLog, currentTerm, commitIndex);
            checkArgument(lastAppliedIndex <= commitIndex, "lastAppliedIndex:%s commitIndex:%s", lastAppliedIndex, commitIndex); // this check must come after checking the snapshot, log and commit indices

            // this check allows us to avoid the situation where we
            // generate snapshots that don't actually have an impact on log length
            long numEntriesInSnapshot;
            if (latestSnapshot != null) {
                numEntriesInSnapshot = lastAppliedIndex - latestSnapshot.getIndex(); // check how many entries _from the end of the snapshot_ onwards are in this snapshot
            } else {
                numEntriesInSnapshot = lastAppliedIndex; // we have a log _only_, so lastAppliedIndex - 0
            }

            if (numEntriesInSnapshot < minEntriesToSnapshot) {
                LOGGER.warn("{}: ignoring snapshot extendedSnapshotWriter - insufficient snapshot entries:{} minEntriesToSnapshot:{}", self, numEntriesInSnapshot, minEntriesToSnapshot);
                return;
            }

            // set the term for the last entry in the snapshot
            LogEntry logEntry = checkNotNull(log.get(lastAppliedIndex));
            long lastAppliedTerm = logEntry.getTerm();
            extendedSnapshotWriter.setTerm(lastAppliedTerm);

            // attempt to store the snapshot on disk and truncate the log
            snapshotsStore.storeSnapshot(extendedSnapshotWriter);
            // TODO (AG): truncate the log!
        } catch (StorageException e) {
            handleStorageException(e);
        }
    }

    //----------------------------------------------------------------------------------------------------------------//
    //
    // search for available committed state
    //

    @Override
    public synchronized @Nullable Committed getNextCommitted(long indexToSearchFrom) {
        LOGGER.trace("{}: get next committed from {}", self, indexToSearchFrom);

        checkState(initialized);
        checkArgument(indexToSearchFrom >= 0, "index:%s must be positive", indexToSearchFrom);

        try {
            long currentTerm = store.getCurrentTerm();
            long commitIndex = store.getCommitIndex();
            ExtendedSnapshot latestSnapshot = snapshotsStore.getLatestSnapshot();

            // bounds checks
            checkSnapshotLogAndCommitIndicesAndTerms(latestSnapshot, log.getFirst(), log.getLast(), currentTerm, commitIndex);
            checkArgument(indexToSearchFrom <= commitIndex, "indexToSearchFrom:%s commitIndex:%s", indexToSearchFrom, commitIndex);

            // there are no more entries that the caller can apply
            if (indexToSearchFrom == commitIndex) {
                return null;
            }

            // TODO (AG): I'm pretty sure at some point I'm going to have to share this code with the algorithm methods

            //
            // OK...note that the log and the snapshot may overlap as follows
            //
            //                  -----------------------------------
            //  .... EMPTY ....| n  | n+1 | n+2 | n+3 | n+4 | n+5 | .... MORE ENTRIES ..... (LOG)
            //                 -----------------------------------
            // ---------------------------------
            //  LAST APPLIED = n               | (SNAPSHOT)
            // --------------------------------
            //
            // when the caller specifies an indexToSearchFrom we want to choose _either_ a snapshot _or_ a client log entry
            // conceptually, the decision process is as follows:
            //
            // 1. if the indexToSearchFrom > last applied index in the snapshot, return the first log entry they can apply
            // 2. if the indexToSearchFrom < first log and we have a valid snapshot, return the snapshot
            // 3. if the indexToSearchFrom is within the overlap area, pick a log entry if possible, otherwise, fall down to the snapshot
            //

            Committed committed = null;

            // 1. try to find a log entry they can apply
            for (long index = indexToSearchFrom + 1; index <= commitIndex; index++) {
                LogEntry currentLog = log.get(index);

                // if there's nothing left in the log we're done
                if (currentLog == null) {
                    break;
                }

                // check if this entry should be returned to the client
                committed = convertLogEntryToCommitted(currentLog);
                if (committed != null) {
                    break;
                }
            }

            // 2. we couldn't find a log entry, so let's see if there's a snapshot they can use
            if ((committed == null) && (latestSnapshot != null) && (indexToSearchFrom < latestSnapshot.getIndex())) {
                committed = latestSnapshot;
            }

            // return whatever we have (this may be null!)
            return committed;
        } catch (StorageException e) {
            handleStorageException(e);
            throw new RaftError("handleStorageException did not throw", e); // should not ever get here
        }
    }

    private static void checkSnapshotLogAndCommitIndicesAndTerms(@Nullable ExtendedSnapshot latestSnapshot, @Nullable LogEntry firstLog, @Nullable LogEntry lastLog, long currentTerm, long commitIndex) {
        checkArgument((firstLog == null && lastLog == null) || (firstLog != null && lastLog != null), "firstLog:%s lastLog:%s", firstLog, lastLog);
        checkArgument(currentTerm >= LogEntry.SENTINEL.getTerm(), "currentTerm:%s", currentTerm);
        checkArgument(commitIndex >= LogEntry.SENTINEL.getIndex(), "commitIndex:%s", commitIndex);

        // can't have nothing
        checkState(lastLog != null || latestSnapshot != null, "both log and snapshot cannot be missing");

        long lastTerm = LogEntry.SENTINEL.getTerm();
        long lastIndex = LogEntry.SENTINEL.getIndex();

        // start off with the snapshot
        if (latestSnapshot != null) {
            lastTerm = latestSnapshot.getTerm();
            lastIndex = latestSnapshot.getIndex();
        }

        // if a snapshot exists...

        // check that it has a valid term
        checkState(lastTerm <= currentTerm, "lastTerm:%s currentTerm:%s", lastTerm, currentTerm);
        // and that it doesn't contain more entries than were committed
        checkState(lastIndex <= commitIndex, "snapshot: lastIndex:%s commitIndex:%s", lastIndex, commitIndex);

        // we have a log as well
        if (firstLog != null) {
            // by the check at the beginning of this function we should have a lastLog
            lastLog = checkNotNull(lastLog);
            // check the lower bound of the log
            if (latestSnapshot == null) {
                // no snapshot? we have to start at the beginning then
                checkState(firstLog.getIndex() == LogEntry.SENTINEL.getIndex(), "firstLogIndex:%s", firstLog.getIndex());
            } else {
                // otherwise...the lower bound is the sentinel
                checkState(firstLog.getIndex() >= LogEntry.SENTINEL.getIndex(), "firstLogIndex:%s", firstLog.getIndex());
            }
            // ensure that there is no hole between the boundary of the snapshot and the lower limit of the log
            checkState(firstLog.getIndex() <= lastIndex + 1, "snapshot and log hole: firstLogIndex:%s lastIndex+1:%s", firstLog.getIndex(), lastIndex + 1);
            // check that the relationship between the first and last log indices is OK
            checkState(firstLog.getIndex() <= lastLog.getIndex(), "first and last log flip: firstLog:%s lastLog:%s", firstLog.getIndex(), lastLog.getIndex()); // equals for the case where log has only one entry
            // check that our log is not smaller than a snapshot (if one exists)
            checkState(lastLog.getIndex() >= lastIndex, "lastIndex shrinking: lastLog:%s lastIndex:%s", lastLog, lastIndex);
            // set the last term/index values
            lastTerm = lastLog.getTerm();
            lastIndex = lastLog.getIndex();
        }

        // finally, check that the log as a whole satisfies these properties
        checkState(lastTerm <= currentTerm, "lastTerm:%s currentTerm:%s", lastTerm, currentTerm);
        checkState(commitIndex <= lastIndex, "commitIndex:%s lastIndex:%s", commitIndex, lastIndex);
    }

    //----------------------------------------------------------------------------------------------------------------//
    //
    // submit a command
    //

    @Override
    public synchronized ListenableFuture<Void> submitCommand(Command command) throws NotLeaderException {
        LOGGER.trace("{}: submit command {}", self, command);

        checkState(running);

        if (role != Role.LEADER) {
            throw new NotLeaderException(self, leader);
        }

        SettableFuture<Void> commandFuture = SettableFuture.create();

        // if an exception is thrown, it'll be before
        // the entry is placed into the map by addClientEntry
        // which means we don't have to remove it in
        // the catch handlers
        try {
            addClientEntry(store.getCurrentTerm(), command, commandFuture);
        } catch (RuntimeException e) {
            commandFuture.setException(e);
            throw e;
        } catch (StorageException e) {
            commandFuture.setException(e);
            handleStorageException(e);
        } catch (Exception e) {
            commandFuture.setException(e);
        }

        return commandFuture;
    }

    private void addClientEntry(long currentTerm, Command command, SettableFuture<Void> commandFuture) throws StorageException {
        LogEntry lastLog = checkNotNull(log.getLast());

        // TODO (AG): add an appendLogEntry method to make this easier
        long clientLogIndex = lastLog.getIndex() + 1;

        LogEntry prevClientLog = log.get(clientLogIndex);
        checkState(prevClientLog == null, "overwrote %s at index %s in term %s", prevClientLog, clientLogIndex, currentTerm);

        LogEntry.ClientEntry clientLog = new LogEntry.ClientEntry(clientLogIndex, currentTerm, command);
        log.put(clientLog);

        CommandDatum prevCommandDatum = commands.put(clientLog.getIndex(), new CommandDatum(clientLog, commandFuture));
        checkState(prevCommandDatum == null, "overwrote client entry: index:%s", clientLog.getIndex());

        sendAppendEntriesForClientEntry(cluster);
    }

    private void sendAppendEntriesForClientEntry(Set<String> servers) throws StorageException {
        long currentTerm = store.getCurrentTerm();
        long commitIndex = store.getCommitIndex();
        LogEntry lastLog = checkNotNull(log.getLast());

        for (String server : servers) {
            ServerDatum serverDatum = serverData.get(server);

            long serverPrevLogIndex = serverDatum.nextIndex - 1;
            long unreplicatedEntryCount = lastLog.getIndex() - serverPrevLogIndex;
            List<LogEntry> entries = Lists.newArrayListWithCapacity((int) unreplicatedEntryCount);

            for (long logIndex = serverDatum.nextIndex; logIndex <= lastLog.getIndex(); logIndex++) {
                entries.add(log.get(logIndex));
            }

            LogEntry serverPrevLog = checkNotNull(log.get(serverPrevLogIndex));

            try {
                sender.appendEntries(server, currentTerm, commitIndex, serverPrevLog.getIndex(), serverPrevLog.getTerm(), entries);
            } catch (RPCException e) {
                LOGGER.warn("{}: fail send AppendEntries with {} entries cause:{}", server, getEntryCount(entries), e.getMessage());
            }
        }
    }

    private int getEntryCount(@Nullable Collection<LogEntry> entries) {
        return entries == null ? 0 : entries.size();
    }

    //----------------------------------------------------------------------------------------------------------------//
    //
    //
    // Unit Test Support Methods
    //
    //
    //----------------------------------------------------------------------------------------------------------------//

    // NOTE: all methods have suffix "ForUnitTestsOnly" so that it's explicit what context it's meant to run in

    /**
     * Insert a {@link LogEntry} into the {@code RaftAlgorithm} instance's
     * {@link io.libraft.algorithm.Log}.
     * <p/>
     * <strong>This method is package-private for testing
     * reasons only!</strong> It should <strong>never</strong>
     * be called in a non-test context!
     * <p/>
     * <strong>Should</strong> only be called if no remote servers have
     * received information about the log entry (if any) at {@code entry.getIndex()}.
     *
     * @param entry {@link LogEntry} to be inserted into the {@code RaftAlgorithm} instance's log
     */
    synchronized void addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(LogEntry entry) throws StorageException {
        checkState(role == Role.LEADER, "role:%s", role);

        long commitIndex = store.getCommitIndex();
        checkArgument(entry.getIndex() > commitIndex, "entry:%s commitIndex:%s", entry, commitIndex);

        log.put(entry);
    }

    /**
     * Sets the nextIndex value for the named server. This value is modified
     * every time the leader receives an AppendEntriesReply with applied = 'false'. Unfortunately,
     * if nextIndex has to be rewound a lot many heartbeat rounds have to be done,
     * which makes the tests extremely ugly. This method allows tests
     * to short-circuit that.
     * <p/>
     * <strong>This method is package-private for testing
     * reasons only!</strong> It should <strong>never</strong>
     * be called in a non-test context!
     *
     * @param server unique id of the server for which {@code nextIndex} should be modified
     * @param nextIndex nextIndex value for this server
     */
    synchronized void setServerNextIndexWhileLeaderForUnitTestsOnly(String server, long nextIndex) {
        checkArgument(serverData.containsKey(server));
        serverData.put(server, new ServerDatum(nextIndex, Phase.PREFIX_SEARCH));
    }

    /**
     * Get the {@code Timer.TimeoutHandle} for the next scheduled election timeout.
     * <p/>
     * <strong>This method is package-private for testing
     * reasons only!</strong> It should <strong>never</strong>
     * be called in a non-test context!
     *
     * @return instance of {@code Timer.TimeoutHandle} for the next
     * election timeout, or null if no timeout is scheduled
     */
    synchronized Timer.TimeoutHandle getElectionTimeoutHandleForUnitTestsOnly() {
        return electionTimeoutHandle;
    }

    /**
     * Get the {@code Timer.TimeoutHandle} for the next scheduled heartbeat.
     * <p/>
     * <strong>This method is package-private for testing
     * reasons only!</strong> It should <strong>never</strong>
     * be called in a non-test context!
     *
     * @return instance of {@code Timer.TimeoutHandle} for the next
     * heartbeat timeout, or null if no timeout is scheduled
     * @throws IllegalStateException if this server is <strong>not</strong> the leader
     */
    synchronized Timer.TimeoutHandle getHeartbeatTimeoutHandleForUnitTestsOnly() {
        checkState(role == Role.LEADER, "role:%s", role);
        return heartbeatTimeoutHandle;
    }

    /**
     * Get the {@code Timer.TimeoutHandle} for the next scheduled snapshot check.
     * <p/>
     * <strong>This method is package-private for testing
     * reasons only!</strong> It should <strong>never</strong>
     * be called in a non-test context!
     *
     * @return instance of {@code Timer.TimeoutHandle} for the next
     * snapshot check timeout, or null if no timeout is scheduled
     */
    synchronized Timer.TimeoutHandle getSnapshotTimeoutHandleForUnitTestsOnly() {
        return snapshotTimeoutHandle;
    }
}
