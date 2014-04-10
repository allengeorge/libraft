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

import com.google.common.collect.Sets;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import io.libraft.agent.RaftAgent;
import io.libraft.agent.RaftMember;
import io.libraft.agent.configuration.RaftClusterConfiguration;
import io.libraft.agent.configuration.RaftConfiguration;
import io.libraft.agent.configuration.RaftDatabaseConfiguration;
import io.libraft.agent.configuration.RaftSnapshotsConfiguration;
import io.libraft.kayvee.configuration.ClusterConfiguration;
import io.libraft.kayvee.configuration.ClusterMember;
import io.libraft.kayvee.configuration.KayVeeConfiguration;
import io.libraft.kayvee.health.DistributedStoreCheck;
import io.libraft.kayvee.mappers.IllegalArgumentExceptionMapper;
import io.libraft.kayvee.mappers.KayVeeExceptionMapper;
import io.libraft.kayvee.resources.KeysResource;
import io.libraft.kayvee.store.DistributedStore;
import io.libraft.kayvee.store.KayVeeCommand;
import io.libraft.kayvee.store.LocalStore;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Main class for the KayVee key-value server.
 * <p/>
 * KayVee is a Dropwizard-based service, and its lifecycle is defined by Dropwizard.
 */
public class KayVee extends Service<KayVeeConfiguration> {

    public static void main(String[] args) throws Exception {
        KayVee kayVee = new KayVee();
        kayVee.run(args);
    }

    @Override
    public void initialize(Bootstrap<KayVeeConfiguration> bootstrap) {
        bootstrap.setName("kayvee");
    }

    @Override
    public void run(KayVeeConfiguration configuration, Environment environment) throws Exception {
        // create the local store
        LocalStore localStore = new LocalStore();

        // create and setup the distributed store
        RaftConfiguration raftConfiguration = createRaftConfiguration(configuration);
        DistributedStore distributedStore = new DistributedStore(localStore);
        RaftAgent raftAgent = RaftAgent.fromConfigurationObject(raftConfiguration, distributedStore); // create the agent
        raftAgent.setupJacksonAnnotatedCommandSerializationAndDeserialization(KayVeeCommand.class); // setup the agent to deal with our Command subclasses
        distributedStore.setRaftAgent(raftAgent);
        distributedStore.initialize();
        environment.manage(distributedStore);

        // setup our health checks
        environment.addHealthCheck(new DistributedStoreCheck(distributedStore));

        // setup the resources by which kayvee is accessed
        environment.addResource(new KeysResource(configuration.getClusterConfiguration().getMembers(), distributedStore));

        // setup our exception mappers
        environment.addProvider(IllegalArgumentExceptionMapper.class);
        environment.addProvider(KayVeeExceptionMapper.class);
    }

    private RaftConfiguration createRaftConfiguration(KayVeeConfiguration configuration) {
        ClusterConfiguration clusterConfiguration = configuration.getClusterConfiguration();
        Set<RaftMember> raftMembers = Sets.newHashSet();
        for (ClusterMember member : clusterConfiguration.getMembers()) {
            raftMembers.add(new RaftMember(member.getId(), InetSocketAddress.createUnresolved(member.getRaftHost(), member.getRaftPort())));
        }
        RaftClusterConfiguration raftClusterConfiguration = new RaftClusterConfiguration(clusterConfiguration.getSelf(), raftMembers);

        RaftDatabaseConfiguration raftDatabaseConfiguration = configuration.getRaftDatabaseConfiguration();
        RaftSnapshotsConfiguration snapshotsConfiguration = configuration.getSnapshotsConfiguration();

        RaftConfiguration raftConfiguration = new RaftConfiguration(raftDatabaseConfiguration, raftClusterConfiguration);
        raftConfiguration.setConnectTimeout(3000);
        raftConfiguration.setMinReconnectInterval(3000);
        raftConfiguration.setAdditionalReconnectIntervalRange(0);
        raftConfiguration.setMinElectionTimeout(30000);
        raftConfiguration.setAdditionalElectionTimeoutRange(10000);
        raftConfiguration.setHeartbeatInterval(7000);
        raftConfiguration.setRPCTimeout(1000);
        raftConfiguration.setRaftSnapshotsConfiguration(snapshotsConfiguration);

        return raftConfiguration;
    }
}