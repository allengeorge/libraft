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

package io.libraft.kayvee;

import com.google.common.collect.ImmutableSet;
import com.yammer.dropwizard.config.HttpConfiguration;
import io.libraft.agent.configuration.RaftDatabaseConfiguration;
import io.libraft.agent.configuration.RaftSnapshotsConfiguration;
import io.libraft.kayvee.configuration.ClusterConfiguration;
import io.libraft.kayvee.configuration.ClusterMember;
import io.libraft.kayvee.configuration.KayVeeConfiguration;

public abstract class KayVeeConfigurationFixture {

    private KayVeeConfigurationFixture() { // to prevent instantiation
    }

    private static final HttpConfiguration HTTP_CONFIGURATION = new HttpConfiguration();
    static {
        HTTP_CONFIGURATION.setBindHost("localhost");
        HTTP_CONFIGURATION.setPort(8080);
        HTTP_CONFIGURATION.setAdminPort(8081);
    }

    private static final RaftDatabaseConfiguration RAFT_DATABASE_CONFIGURATION = new RaftDatabaseConfiguration(
            "org.h2.Driver",
            "jdbc:h2:mem:test_raft",
            "test",
            "test"
    );

    private static final ClusterConfiguration CLUSTER_CONFIGURATION = new ClusterConfiguration(
            "SERVER_00",
            ImmutableSet.of(
                    new ClusterMember("SERVER_00", "http://localhost:8080", "localhost:9080"),
                    new ClusterMember("SERVER_01", "http://localhost:8085", "localhost:9085"),
                    new ClusterMember("SERVER_02", "http://localhost:8090", "localhost:9090")
            )
    );

    private static final RaftSnapshotsConfiguration SNAPSHOTS_CONFIGURATION = new RaftSnapshotsConfiguration();
    static {
        SNAPSHOTS_CONFIGURATION.setMinEntriesToSnapshot(1000);
        SNAPSHOTS_CONFIGURATION.setSnapshotCheckInterval(12 * 60 * 60 * 1000);
        SNAPSHOTS_CONFIGURATION.setSnapshotsDirectory("snapshots");
    }

    public static final KayVeeConfiguration KAYVEE_CONFIGURATION = new KayVeeConfiguration();
    static {
        KAYVEE_CONFIGURATION.setHttpConfiguration(HTTP_CONFIGURATION);
        KAYVEE_CONFIGURATION.setRaftDatabaseConfiguration(RAFT_DATABASE_CONFIGURATION);
        KAYVEE_CONFIGURATION.setSnapshotsConfiguration(SNAPSHOTS_CONFIGURATION);
        KAYVEE_CONFIGURATION.setClusterConfiguration(CLUSTER_CONFIGURATION);
    }
}
