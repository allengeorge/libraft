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

package io.libraft.agent.configuration;

import com.google.common.collect.ImmutableSet;
import io.libraft.agent.RaftMember;

import java.net.InetSocketAddress;
import java.util.Set;

abstract class RaftConfigurationFixture {

    private RaftConfigurationFixture() { // to prevent instantiation
    }

    private static final Set<RaftMember> RAFT_MEMBERS = ImmutableSet.of(
            new RaftMember("S_00", InetSocketAddress.createUnresolved("192.168.1.100", 9990)),
            new RaftMember("S_01", InetSocketAddress.createUnresolved("192.168.1.100", 9991)),
            new RaftMember("S_02", InetSocketAddress.createUnresolved("192.168.1.100", 9992)),
            new RaftMember("S_03", InetSocketAddress.createUnresolved("192.168.1.100", 9993)),
            new RaftMember("S_04", InetSocketAddress.createUnresolved("192.168.1.100", 9994))
    );

    private static final RaftClusterConfiguration RAFT_CLUSTER_CONFIGURATION = new RaftClusterConfiguration("S_00", RAFT_MEMBERS);

    private static final RaftDatabaseConfiguration RAFT_DATABASE_CONFIGURATION = new RaftDatabaseConfiguration("org.h2.Driver", "jdbc:h2:test_db", "test", "test");

    private static final RaftDatabaseConfiguration RAFT_DATABASE_EMPTY_PASSWORD_CONFIGURATION = new RaftDatabaseConfiguration("org.h2.Driver", "jdbc:h2:test_db", "test", "");

    private static final RaftDatabaseConfiguration RAFT_DATABASE_NO_PASSWORD_CONFIGURATION = new RaftDatabaseConfiguration("org.h2.Driver", "jdbc:h2:test_db", "test", null);

    static final RaftConfiguration RAFT_REQUIRED_FIELDS_ONLY_CONFIGURATION = new RaftConfiguration(RAFT_DATABASE_CONFIGURATION, RAFT_CLUSTER_CONFIGURATION);

    static final RaftConfiguration RAFT_REQUIRED_FIELDS_ONLY_EMPTY_PASSWORD_CONFIGURATION = new RaftConfiguration(RAFT_DATABASE_EMPTY_PASSWORD_CONFIGURATION, RAFT_CLUSTER_CONFIGURATION);

    static final RaftConfiguration RAFT_REQUIRED_FIELDS_ONLY_NO_PASSWORD_CONFIGURATION = new RaftConfiguration(RAFT_DATABASE_NO_PASSWORD_CONFIGURATION, RAFT_CLUSTER_CONFIGURATION);

    static final RaftConfiguration RAFT_ALL_FIELDS_CONFIGURATION = new RaftConfiguration(RAFT_DATABASE_CONFIGURATION, RAFT_CLUSTER_CONFIGURATION);
    static {
        RAFT_ALL_FIELDS_CONFIGURATION.setMinElectionTimeout(180);
        RAFT_ALL_FIELDS_CONFIGURATION.setAdditionalElectionTimeoutRange(120);
        RAFT_ALL_FIELDS_CONFIGURATION.setRPCTimeout(30);
        RAFT_ALL_FIELDS_CONFIGURATION.setHeartbeatInterval(15);
        RAFT_ALL_FIELDS_CONFIGURATION.setConnectTimeout(5000);
        RAFT_ALL_FIELDS_CONFIGURATION.setMinReconnectInterval(10000);
        RAFT_ALL_FIELDS_CONFIGURATION.setAdditionalReconnectIntervalRange(1000);
    }
}
