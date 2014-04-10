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

package io.libraft.agent.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.libraft.agent.RaftAgentConstants;
import io.libraft.algorithm.RaftConstants;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

/**
 * Represents the top-level configuration for libraft-agent. Contains the following properties/blocks:
 * <ul>
 *     <li>rpcTimeout</li>
 *     <li>minElementTimeout</li>
 *     <li>additionalElectionTimeoutRange</li>
 *     <li>heartbeatInterval</li>
 *     <li>connectTimeout</li>
 *     <li>minReconnectInterval</li>
 *     <li>additionalReconnectIntervalRange</li>
 *     <li>database</li>
 *     <li>snapshots</li>
 *     <li>cluster</li>
 * </ul>
 * All timeouts and intervals are specified in <strong>milliseconds</strong>.
 * <p/>
 * See the project README.md for more on the configuration.
 */
public final class RaftConfiguration {

    private static final String TIME_UNIT = "timeUnit";
    private static final String RPC_TIMEOUT = "rpcTimeout";
    private static final String MIN_ELECTION_TIMEOUT = "minElectionTimeout";
    private static final String ADDITIONAL_ELECTION_TIMEOUT_RANGE = "additionalElectionTimeoutRange";
    private static final String HEARTBEAT_INTERVAL = "heartbeatInterval";
    private static final String CONNECT_TIMEOUT = "connectTimeout";
    private static final String MIN_RECONNECT_INTERVAL = "minReconnectInterval";
    private static final String ADDITIONAL_RECONNECT_INTERVAL_RANGE = "additionalReconnectIntervalRange";
    private static final String SNAPSHOTS = "snapshots";
    private static final String DATABASE = "database";
    private static final String CLUSTER = "cluster";

    @Min(1)
    @Max(RaftConfigurationConstants.SIXTY_SECONDS)
    @NotNull
    @JsonProperty(RPC_TIMEOUT)
    private int rpcTimeout = RaftConstants.RPC_TIMEOUT;

    @Min(1)
    @Max(RaftConfigurationConstants.SIXTY_SECONDS)
    @NotNull
    @JsonProperty(MIN_ELECTION_TIMEOUT)
    private int minElectionTimeout = RaftConstants.MIN_ELECTION_TIMEOUT;

    @Min(0)
    @Max(RaftConfigurationConstants.SIXTY_SECONDS)
    @NotNull
    @JsonProperty(ADDITIONAL_ELECTION_TIMEOUT_RANGE)
    private int additionalElectionTimeoutRange = RaftConstants.ADDITIONAL_ELECTION_TIMEOUT_RANGE;

    @Min(1)
    @Max(RaftConfigurationConstants.SIXTY_SECONDS)
    @NotNull
    @JsonProperty(HEARTBEAT_INTERVAL)
    private int heartbeatInterval = RaftConstants.HEARTBEAT_INTERVAL;

    @Min(0)
    @Max(RaftConfigurationConstants.SIXTY_SECONDS)
    @NotNull
    @JsonProperty(CONNECT_TIMEOUT)
    private int connectTimeout = RaftAgentConstants.CONNECT_TIMEOUT;

    @Min(1)
    @Max(RaftConfigurationConstants.SIXTY_SECONDS)
    @NotNull
    @JsonProperty(MIN_RECONNECT_INTERVAL)
    private int minReconnectInterval = RaftAgentConstants.MIN_RECONNECT_INTERVAL;

    @Min(0)
    @Max(RaftConfigurationConstants.SIXTY_SECONDS)
    @NotNull
    @JsonProperty(ADDITIONAL_RECONNECT_INTERVAL_RANGE)
    private int additionalReconnectIntervalRange = RaftAgentConstants.ADDITIONAL_RECONNECT_INTERVAL_RANGE;

    @JsonIgnore
    private final TimeUnit timeUnit = RaftConfigurationConstants.DEFAULT_TIME_UNIT;

    //
    // these configuration blocks have defaults
    //

    @Valid
    @JsonProperty(SNAPSHOTS)
    private RaftSnapshotsConfiguration raftSnapshotsConfiguration = new RaftSnapshotsConfiguration(); // snapshots are disabled by default

    //
    // these configuration blocks have no defaults
    //

    @Valid
    @JsonProperty(DATABASE)
    private final RaftDatabaseConfiguration raftDatabaseConfiguration;

    @Valid
    @JsonProperty(CLUSTER)
    private final RaftClusterConfiguration raftClusterConfiguration;

    /**
     * Constructor.
     *
     * @param raftDatabaseConfiguration instance of {@code RaftDatabaseConfiguration}. This instance will be validated
     * @param raftClusterConfiguration instance of {@code RaftClusterConfiguration}. This instance will be validated
     */
    @JsonCreator
    public RaftConfiguration(@JsonProperty(DATABASE) RaftDatabaseConfiguration raftDatabaseConfiguration, @JsonProperty(CLUSTER) RaftClusterConfiguration raftClusterConfiguration) {
        this.raftDatabaseConfiguration = raftDatabaseConfiguration;
        this.raftClusterConfiguration = raftClusterConfiguration;
    }

    /**
     * Get the {@link java.util.concurrent.TimeUnit} in which all
     * configuration timeouts and intervals are specified.
     *
     * @return time unit in which all configuration timeouts and intervals are specified
     */
    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    /**
     * Get the Raft RPC timeout.
     * <p/>
     * This value defines how long a {@link io.libraft.Raft} implementation
     * will wait for the response to a RPC request.
     *
     * @return rpc timeout value >= 0
     */
    public int getRPCTimeout() {
        return rpcTimeout;
    }

    /**
     * Set the Raft RPC timeout.
     *
     * @param rpcTimeout rpc timeout value > 0
     *
     * @see RaftConfiguration#getRPCTimeout()
     */
    public void setRPCTimeout(int rpcTimeout) {
        this.rpcTimeout = rpcTimeout;
    }

    /**
     * Get the minimum Raft election timeout.
     * <p/>
     * This value defines how long a Raft follower will wait for a valid
     * AppendEntries message from the leader before holding a new election.
     * The <strong>actual</strong> election timeout is defined as:
     * <pre>
     *     minElectionTimeout + randomInRange(0, additionElectionTimeoutRange)
     * </pre>
     *
     * @return minimum election timeout value >= 0
     *
     * @see RaftConfiguration#getAdditionalElectionTimeoutRange()
     */
    public int getMinElectionTimeout() {
        return minElectionTimeout;
    }

    /**
     * Set the minimum Raft election timeout.
     *
     * @param minElectionTimeout minimum election timeout value > 0
     *
     * @see RaftConfiguration#getMinElectionTimeout()
     */
    public void setMinElectionTimeout(int minElectionTimeout) {
        this.minElectionTimeout = minElectionTimeout;
    }

    /**
     * Get the maximum additional amount of time added to
     * {@link RaftConfiguration#getMinElectionTimeout()} to generate the
     * total election timeout for a Raft server.
     *
     * @return additional election timeout range >= 0
     *
     * @see RaftConfiguration#getMinElectionTimeout()
     */
    public int getAdditionalElectionTimeoutRange() {
        return additionalElectionTimeoutRange;
    }

    /**
     * Set the maximum additional amount of time added to
     * {@link RaftConfiguration#getMinElectionTimeout()} to generate the
     * election timeout for a raft server.
     *
     * @param additionalElectionTimeoutRange additional election timeout range > 0
     *
     * @see RaftConfiguration#getAdditionalElectionTimeoutRange()
     */
    public void setAdditionalElectionTimeoutRange(int additionalElectionTimeoutRange) {
        this.additionalElectionTimeoutRange = additionalElectionTimeoutRange;
    }

    /**
     * Get the Raft heartbeat interval.
     * <p/>
     * This value defines the <strong>maximum</strong>
     * interval <strong>between</strong> messages from
     * a Raft leader to a Raft follower.
     *
     * @return heartbeat interval >= 0
     */
    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * Set the Raft heartbeat interval.
     *
     * @param heartbeatInterval heartbeat interval > 0
     *
     * @see RaftConfiguration#getHeartbeatInterval()
     */
    public void setHeartbeatInterval(int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    /**
     * Get the {@link io.libraft.agent.rpc.RaftNetworkClient} connect timeout.
     * <p/>
     * This value defines the amount of time the local Raft server will wait to
     * establish a connection to another Raft server. If the connection is not established
     * when this time period expires the in-progress connection is torn down and a
     * reconnect attempt is scheduled.
     *
     * @return connect timeout value >= 0
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Set the {@link io.libraft.agent.rpc.RaftNetworkClient} connect timeout.
     *
     * @param connectTimeout connect timeout value > 0
     *
     * @see RaftConfiguration#getConnectTimeout()
     */
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Get the minimum reconnect interval.
     * <p/>
     * This value defines the <strong>minimum</strong> amount of time
     * the local Raft server will wait before reconnecting to another
     * Raft server. The <strong>actual</strong> reconnect interval is
     * defined as:
     * <pre>
     *     minReconnectInterval + randomInRange(0, additionalReconnectIntervalRange)
     * </pre>
     *
     * @return minimum reconnect interval >= 0
     *
     * @see RaftConfiguration#getAdditionalElectionTimeoutRange()
     */
    public int getMinReconnectInterval() {
        return minReconnectInterval;
    }

    /**
     * Set the minimum reconnect interval.
     *
     * @param minReconnectInterval minimum reconnect interval > 0
     *
     * @see RaftConfiguration#getMinElectionTimeout()
     */
    public void setMinReconnectInterval(int minReconnectInterval) {
        this.minReconnectInterval = minReconnectInterval;
    }

    /**
     * Get the maximum additional amount of time added to
     * {@link RaftConfiguration#getMinReconnectInterval()} to generate the
     * reconnect interval for a {@link io.libraft.agent.rpc.RaftNetworkClient}.
     *
     * @return additional reconnect interval range >= 0
     *
     * @see RaftConfiguration#getMinReconnectInterval()
     */
    public int getAdditionalReconnectIntervalRange() {
        return additionalReconnectIntervalRange;
    }

    /**
     * Set the maximum additional amount of time added to
     * {@link RaftConfiguration#getMinReconnectInterval()} to generate the
     * reconnect interval for a {@link io.libraft.agent.rpc.RaftNetworkClient}.
     *
     * @param additionalReconnectIntervalRange additional reconnect interval range >= 0
     *
     * @see RaftConfiguration#getAdditionalReconnectIntervalRange()
     */
    public void setAdditionalReconnectIntervalRange(int additionalReconnectIntervalRange) {
        this.additionalReconnectIntervalRange = additionalReconnectIntervalRange;
    }

    /**
     * Get the Raft database configuration.
     *
     * @return an instance of {@code RaftDatabaseConfiguration} that
     *         describes the configuration parameters for the {@link io.libraft.algorithm.Log},
     *         {@link io.libraft.algorithm.Store} and {@link io.libraft.algorithm.SnapshotsStore}
     *         database backend
     */
    public RaftDatabaseConfiguration getRaftDatabaseConfiguration() {
        return raftDatabaseConfiguration;
    }

    /**
     * Get the Raft snapshots configuration.
     *
     * @return an instance of {@code RaftSnapshotsConfiguration} that
     *         describes the configuration parameters associated with
     *         creating and storing snapshots using an
     *         {@link io.libraft.agent.snapshots.OnDiskSnapshotsStore}
     */
    public RaftSnapshotsConfiguration getRaftSnapshotsConfiguration() {
        return raftSnapshotsConfiguration;
    }

    /**
     * Set the Raft snapshots configuration.
     *
     * @param raftSnapshotsConfiguration instance of {@code RaftSnapshotsConfiguration} that
     *                                   describes the configuration parameters associated with
     *                                   creating and storing snapshots using an
     *                                   {@link io.libraft.agent.snapshots.OnDiskSnapshotsStore}
     */
    public void setRaftSnapshotsConfiguration(RaftSnapshotsConfiguration raftSnapshotsConfiguration) {
        this.raftSnapshotsConfiguration = raftSnapshotsConfiguration;
    }

    /**
     * Get the Raft cluster configuration.
     *
     * @return an instance of {@code RaftClusterConfiguration} that describes
     *         the configuration of the Raft cluster to which the local Raft server belongs
     */
    public RaftClusterConfiguration getRaftClusterConfiguration() {
        return raftClusterConfiguration;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RaftConfiguration other = (RaftConfiguration) o;
        return timeUnit == other.timeUnit
                && rpcTimeout == other.rpcTimeout
                && minElectionTimeout == other.minElectionTimeout
                && additionalElectionTimeoutRange == other.additionalElectionTimeoutRange
                && heartbeatInterval == other.heartbeatInterval
                && connectTimeout == other.connectTimeout
                && minReconnectInterval == other.minReconnectInterval
                && additionalReconnectIntervalRange == other.additionalReconnectIntervalRange
                && raftDatabaseConfiguration.equals(other.raftDatabaseConfiguration)
                && raftSnapshotsConfiguration.equals(other.raftSnapshotsConfiguration)
                && raftClusterConfiguration.equals(other.raftClusterConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                timeUnit,
                rpcTimeout,
                minElectionTimeout,
                additionalElectionTimeoutRange,
                heartbeatInterval,
                connectTimeout,
                minReconnectInterval,
                additionalReconnectIntervalRange,
                raftDatabaseConfiguration,
                raftSnapshotsConfiguration,
                raftClusterConfiguration
        );
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add(TIME_UNIT, timeUnit)
                .add(RPC_TIMEOUT, rpcTimeout)
                .add(MIN_ELECTION_TIMEOUT, minElectionTimeout)
                .add(ADDITIONAL_ELECTION_TIMEOUT_RANGE, additionalElectionTimeoutRange)
                .add(HEARTBEAT_INTERVAL, heartbeatInterval)
                .add(CONNECT_TIMEOUT, connectTimeout)
                .add(MIN_RECONNECT_INTERVAL, minReconnectInterval)
                .add(ADDITIONAL_RECONNECT_INTERVAL_RANGE, additionalReconnectIntervalRange)
                .add(DATABASE, raftDatabaseConfiguration)
                .add(SNAPSHOTS, raftSnapshotsConfiguration)
                .add(CLUSTER, raftClusterConfiguration)
                .toString();
    }
}
