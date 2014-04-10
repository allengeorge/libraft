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

package io.libraft.kayvee.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.yammer.dropwizard.config.Configuration;
import io.libraft.agent.configuration.RaftDatabaseConfiguration;
import io.libraft.agent.configuration.RaftSnapshotsConfiguration;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

// NOTE: _only_ the root configuration has to extend the dropwizard
// configuration object, because the root configuration object has to
// contain all the standard Dropwizard configuration stanzas
/**
 * Defines the root KayVee configuration. Contains
 * three extra blocks on top of the standard blocks and properties supplied
 * by Dropwizard:
 * <ul>
 *     <li>raftDatabase: configuration properties for the database
 *         in which all Raft metadata and log entries are persisted.</li>
 *     <li>snapshots: configuration properties for KayVee
 *         {@code key=>value} state snapshots.</li>
 *     <li>cluster: KayVee cluster configuration properties.</li>
 * </ul>
 * See the KayVee README.md for more on the KayVee configuration.
 */
public final class KayVeeConfiguration extends Configuration {

    private static final String HTTP = "http";
    private static final String LOGGING = "logging";
    private static final String RAFT_DATABASE = "raftDatabase";
    private static final String SNAPSHOTS = "snapshots";
    private static final String CLUSTER = "cluster";

    @Valid
    @NotNull
    @JsonProperty(RAFT_DATABASE)
    private RaftDatabaseConfiguration raftDatabaseConfiguration;

    @Valid
    @NotNull
    @JsonProperty(SNAPSHOTS)
    private RaftSnapshotsConfiguration snapshotsConfiguration = new RaftSnapshotsConfiguration(); // disable snapshots unless overridden

    @Valid
    @NotNull
    @JsonProperty(CLUSTER)
    private ClusterConfiguration clusterConfiguration;

    /**
     * Get the Raft log/metadata database configuration block.
     *
     * @return instance of {@code RaftDatabaseConfiguration} in which
     * Raft log/metadata properties are stored
     */
    public RaftDatabaseConfiguration getRaftDatabaseConfiguration() {
        return raftDatabaseConfiguration;
    }

    /**
     * Set the Raft log/metadata database configuration block.
     *
     * @param raftDatabaseConfiguration instance of {@code RaftDatabaseConfiguration}
     *                                  in which Raft log/metadata properties are stored
     */
    public void setRaftDatabaseConfiguration(RaftDatabaseConfiguration raftDatabaseConfiguration) {
        this.raftDatabaseConfiguration = raftDatabaseConfiguration;
    }

    /**
     * Get the KayVee snapshot configuration block.
     *
     * @return instance of {@code RaftSnapshotsConfiguration} in which
     * the snapshot properties are stored
     */
    public RaftSnapshotsConfiguration getSnapshotsConfiguration() {
        return snapshotsConfiguration;
    }

    /**
     * Set the kayVee snapshot configuration block.
     *
     * @param snapshotsConfiguration instance of {@code RaftSnapshotsConfiguration} in which
     *                               the snapshot properties are stored
     */
    public void setSnapshotsConfiguration(RaftSnapshotsConfiguration snapshotsConfiguration) {
        this.snapshotsConfiguration = snapshotsConfiguration;
    }

    /**
     * Get the KayVee cluster configuration block.
     *
     * @return instance of {@code ClusterConfiguration} in which the
     * KayVee cluster configuration properties are stored
     */
    public ClusterConfiguration getClusterConfiguration() {
        return clusterConfiguration;
    }

    /**
     * Set the KayVee cluster configuration block.
     *
     * @param clusterConfiguration instance of {@code ClusterConfiguration} in
     *                             which the KayVee cluster configuration properties are stored
     */
    public void setClusterConfiguration(ClusterConfiguration clusterConfiguration) {
        this.clusterConfiguration = clusterConfiguration;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KayVeeConfiguration other = (KayVeeConfiguration) o;

        return getHttpConfiguration().getBindHost().equals(other.getHttpConfiguration().getBindHost())
                && getHttpConfiguration().getPort() == other.getHttpConfiguration().getPort()
                && raftDatabaseConfiguration.equals(other.raftDatabaseConfiguration)
                && snapshotsConfiguration.equals(other.snapshotsConfiguration)
                && clusterConfiguration.equals(other.clusterConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                getHttpConfiguration().getBindHost(),
                getHttpConfiguration().getPort(),
                raftDatabaseConfiguration,
                snapshotsConfiguration,
                clusterConfiguration);
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add(HTTP, getHttpConfiguration())
                .add(LOGGING, getLoggingConfiguration())
                .add(RAFT_DATABASE, raftDatabaseConfiguration)
                .add(SNAPSHOTS, snapshotsConfiguration)
                .add(CLUSTER, clusterConfiguration)
                .toString();
    }
}
