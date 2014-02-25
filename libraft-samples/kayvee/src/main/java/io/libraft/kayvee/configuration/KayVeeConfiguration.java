/*
 * Copyright (c) 2013, Allen A. George <allen dot george at gmail dot com>
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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

// NOTE: _only_ the root configuration has to extend the dropwizard
// configuration object, because the root configuration object has to
// contain all the standard Dropwizard configuration stanzas
/**
 * Defines the root KayVee configuration. Contains
 * 2 extra blocks on top of the standard blocks and properties supplied
 * by Dropwizard:
 * <ul>
 *     <li>raftDatabase</li>
 *     <li>cluster</li>
 * </ul>
 * See the KayVee README.md for more on the KayVee configuration.
 */
public class KayVeeConfiguration extends Configuration {

    @Valid
    @NotNull
    @JsonProperty("raftDatabase")
    private RaftDatabaseConfiguration raftDatabaseConfiguration;

    @Valid
    @NotNull
    @JsonProperty("cluster")
    private ClusterConfiguration clusterConfiguration;

    public RaftDatabaseConfiguration getRaftDatabaseConfiguration() {
        return raftDatabaseConfiguration;
    }

    @SuppressWarnings("unused")
    public void setRaftDatabaseConfiguration(RaftDatabaseConfiguration raftDatabaseConfiguration) {
        this.raftDatabaseConfiguration = raftDatabaseConfiguration;
    }

    public ClusterConfiguration getClusterConfiguration() {
        return clusterConfiguration;
    }

    @SuppressWarnings("unused")
    public void setClusterConfiguration(ClusterConfiguration clusterConfiguration) {
        this.clusterConfiguration = clusterConfiguration;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KayVeeConfiguration other = (KayVeeConfiguration) o;

        return getHttpConfiguration().getBindHost().equals(other.getHttpConfiguration().getBindHost())
                && getHttpConfiguration().getPort() == other.getHttpConfiguration().getPort()
                && raftDatabaseConfiguration.equals(other.raftDatabaseConfiguration)
                && clusterConfiguration.equals(other.clusterConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                getHttpConfiguration().getBindHost(),
                getHttpConfiguration().getPort(),
                raftDatabaseConfiguration,
                clusterConfiguration);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("http", getHttpConfiguration())
                .add("logging", getLoggingConfiguration())
                .add("raftDatabaseConfiguration", raftDatabaseConfiguration)
                .add("clusterConfiguration", clusterConfiguration)
                .toString();
    }
}
