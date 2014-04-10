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

package io.libraft.agent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.libraft.Command;
import io.libraft.Committed;
import io.libraft.CommittedCommand;
import io.libraft.NotLeaderException;
import io.libraft.RaftListener;
import io.libraft.SnapshotWriter;
import io.libraft.agent.configuration.RaftClusterConfiguration;
import io.libraft.agent.configuration.RaftConfiguration;
import io.libraft.agent.configuration.RaftDatabaseConfiguration;
import io.libraft.agent.configuration.RaftSnapshotsConfiguration;
import io.libraft.algorithm.StorageException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/// FIXME (AG): simplify, and load snapshots on startup
@RunWith(Parameterized.class)
public final class RaftAgentTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftAgentTest.class);

    //----------------------------------------------------------------------------------------------------------------//

    // COMMAND SUBMITTER

    private static final int SUBMITTED_COMMAND_COUNT = 5;

    private static interface CommandSubmitter {

        void onAgentCreated(RaftAgent agent);

        void onAgentBecameLeader(RaftAgent leaderAgent) throws NotLeaderException;

        void onAgentAppliedCommands(String agentId, Set<Command> appliedCommands);
    }

    // this instance submits commands that are serialized using the custom RaftAgent serializer (this simply encodes binary as Base64)
    private static final class BinarySerializedCommandSubmitter implements CommandSubmitter {

        // command serializer and deserializer instances can be shared
        private final UnitTestCommandSerializer serializer = new UnitTestCommandSerializer();
        private final UnitTestCommandDeserializer deserializer = new UnitTestCommandDeserializer();

        // the commands that are going to be submitted
        // doesn't matter if they're submitted multiple times (the receivers will filter out duplicates)
        private final Command[] submittedCommands = new Command[] {
                new UnitTestCommand("1"),
                new UnitTestCommand("2"),
                new UnitTestCommand("3"),
                new UnitTestCommand("4"),
                new UnitTestCommand("5")
        };

        @Override
        public void onAgentCreated(RaftAgent agent) {
            agent.setupCustomCommandSerializationAndDeserialization(serializer, deserializer);
        }

        @Override
        public void onAgentBecameLeader(RaftAgent leaderAgent) throws NotLeaderException {
            for (Command command : submittedCommands) {
                leaderAgent.submitCommand(command);
            }
        }

        @Override
        public void onAgentAppliedCommands(String agentId, Set<Command> appliedCommands) {
            assertThat(agentId + " has not applied correct commands", appliedCommands, containsInAnyOrder(submittedCommands));
        }
    }

    // this instance submits commands that are Jackson-annotated (and serialized using Jackson)
    private static final class JacksonAnnotatedCommandSubmitter implements CommandSubmitter {

        // the commands that are going to be submitted
        // again, it doesn't matter if they're submitted multiple times (the receivers will filter out duplicates)
        private final Command[] submittedCommands = new Command[] {
                new AgentTestCommand.NOPCommand(1),
                new AgentTestCommand.GETCommand(2, "KEY"),
                new AgentTestCommand.ALLCommand(3),
                new AgentTestCommand.SETCommand(4, "KEY", "VALUE"),
                new AgentTestCommand.DELCommand(5, "KEY")
        };

        @Override
        public void onAgentCreated(RaftAgent agent) {
            agent.setupJacksonAnnotatedCommandSerializationAndDeserialization(AgentTestCommand.class);
        }

        @Override
        public void onAgentBecameLeader(RaftAgent leaderAgent) throws NotLeaderException {
            for (Command command : submittedCommands) {
                leaderAgent.submitCommand(command);
            }
        }

        @Override
        public void onAgentAppliedCommands(String agentId, Set<Command> appliedCommands) {
            assertThat(agentId + " has not applied correct commands", appliedCommands, containsInAnyOrder(submittedCommands));
        }
    }

    //----------------------------------------------------------------------------------------------------------------//

    // SETUP

    @Parameterized.Parameters
    public static Collection<Object[]> constructCommandSubmitters() {
        return Lists.newArrayList(
                new Object[] {new BinarySerializedCommandSubmitter()},
                new Object[] {new JacksonAnnotatedCommandSubmitter()}
        );
    }

    private final Random randomSeeder = new Random();

    private Random random;

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Rule
    public final Timeout timeout = new Timeout(10000); // 10 seconds

    @BeforeClass
    public static void setupDefaultUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread thread, Throwable throwable) {
                LoggerFactory.getLogger("UNCAUGHT").error("t:{} uncaught throwable", thread, throwable);
            }
        });
    }

    @Before
    public void setup() {
        long seed = randomSeeder.nextLong();
        LOGGER.info("seed:{}", seed);

        random = new Random(seed);
    }

    //----------------------------------------------------------------------------------------------------------------//

    // TEST

    private final CommandSubmitter commandSubmitter;

    public RaftAgentTest(CommandSubmitter commandSubmitter) {
        this.commandSubmitter = commandSubmitter;
    }

    /**
     * This is not a unit test. Instead, it functions as a simple integration test.
     * Since {@code RaftAgent} wraps a number of components that have to coordinate to work,
     * we use this test to verify that the wiring works.
     */
    @Test
    public void shouldFreshBootClusterAndApplyMultipleCommands() throws StorageException, InterruptedException {
        class MemberDatum {
            RaftConfiguration configuration; // set once, never modified
            RaftAgent agent; // set once, never modified
            final Set<Command> appliedCommands = Collections.synchronizedSet(Sets.<Command>newHashSet());
            final Semaphore appliedCommandSemaphore = new Semaphore(0); // triggered every time this agent applies a command
        }

        // will be referenced from within the listener callback to access the raft agent
        final Map<String, MemberDatum> memberData = Maps.newHashMap();
        final RaftSnapshotsConfiguration raftSnapshotsConfiguration = new RaftSnapshotsConfiguration(); // snapshots disabled

        // create the configuration for each raft member
        Map<String, Integer> serverToPorts = ImmutableMap.of("S0", getRandomPortNumber(), "S1", getRandomPortNumber(), "S2", getRandomPortNumber());
        for (Map.Entry<String, Integer> entry : serverToPorts.entrySet()) {
            String serverId = entry.getKey();

            // start off by generating the RaftMember objects representing the servers in the cluster
            // IMPORTANT: HAVE TO CREATE A SEPARATE SET FOR EACH AGENT BECAUSE RaftAgent.channel IS MODIFIED!
            Set<RaftMember> raftCluster = Sets.newHashSet();
            for (Map.Entry<String, Integer> clusterEntry: serverToPorts.entrySet()) {
                raftCluster.add(new RaftMember(clusterEntry.getKey(), new InetSocketAddress("localhost", clusterEntry.getValue())));
            }

            RaftClusterConfiguration raftClusterConfiguration = new RaftClusterConfiguration(serverId, raftCluster);
            RaftDatabaseConfiguration raftDatabaseConfiguration = new RaftDatabaseConfiguration(
                    "org.h2.Driver",
                    "jdbc:h2:mem:" + serverId + "-" + Math.abs(random.nextInt()),
                    "test",
                    "test");

            MemberDatum datum = new MemberDatum();
            datum.configuration =  new RaftConfiguration(raftDatabaseConfiguration, raftClusterConfiguration);
            datum.configuration.setMinElectionTimeout(1000);
            datum.configuration.setAdditionalElectionTimeoutRange(500);
            datum.configuration.setRPCTimeout(250);
            datum.configuration.setConnectTimeout(200);
            datum.configuration.setMinReconnectInterval(100);
            datum.configuration.setAdditionalReconnectIntervalRange(100);
            datum.configuration.setRaftSnapshotsConfiguration(raftSnapshotsConfiguration);

            memberData.put(serverId, datum);
        }

        // create the raft agent for each member as well as the listener callback
        for (Map.Entry<String, MemberDatum> entry : memberData.entrySet()) {
            final String id = entry.getKey();
            final MemberDatum datum = entry.getValue();

            datum.agent = RaftAgent.fromConfigurationObject(datum.configuration, new RaftListener() {

                final AtomicReference<String> leader = new AtomicReference<String>(null);

                @Override
                public void writeSnapshot(SnapshotWriter snapshotWriter) {
                    // noop
                }

                @Override
                public void onLeadershipChange(@Nullable String leader) {
                    String previousLeader = this.leader.getAndSet(leader);
                    if (previousLeader != null) {
                        LOGGER.warn("{}: replacing previous leader:{} to {}", id, previousLeader, leader);
                    } else {
                        LOGGER.info("{}: set leader to {}", id, leader);
                    }

                    if (id.equals(leader)) {
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    LOGGER.info("{}: submitting commands", id);

                                    RaftAgent agent = memberData.get(id).agent;

                                    commandSubmitter.onAgentBecameLeader(agent);
                                } catch (NotLeaderException e) {
                                    throw new RuntimeException("fail submit command due to NotLeaderException for " + id);
                                }
                            }
                        }).start();
                    }
                }

                @Override
                public void applyCommitted(Committed committed) {
                    if (committed.getType() == Committed.Type.SKIP) {
                        return;
                    }

                    CommittedCommand committedCommand = (CommittedCommand) committed;
                    long index = committedCommand.getIndex();
                    Command command = committedCommand.getCommand();

                    LOGGER.info("{}: apply command {} at index {}", id, command, index);

                    MemberDatum memberDatum = memberData.get(id);

                    if (memberDatum.appliedCommands.contains(command)) {
                        LOGGER.warn("{}: new leader resent command:{} - ignoring", id, command);
                        return;
                    }

                    memberDatum.appliedCommands.add(command);
                    memberDatum.appliedCommandSemaphore.release();
                }
            });
        }

        LOGGER.info("start all agents");

        // setup serializer and deserializer for all agents
        for (MemberDatum datum : memberData.values()) {
            commandSubmitter.onAgentCreated(datum.agent);
        }

        // initialize all the agents
        for (MemberDatum datum : memberData.values()) {
            datum.agent.initialize();
        }

        // start all the agents
        for (MemberDatum datum : memberData.values()) {
            datum.agent.start();
        }

        LOGGER.info("wait for commands to be applied");

        // wait for the commands to be applied (doesn't matter that I'm doing this sequentially)
        for (MemberDatum datum : memberData.values()) {
            datum.appliedCommandSemaphore.acquire(SUBMITTED_COMMAND_COUNT);
        }

        LOGGER.info("stop all agents");

        // stop all the agents
        for (MemberDatum datum : memberData.values()) {
            datum.agent.stop();
        }

        // check that we have the data we want
        for (Map.Entry<String, MemberDatum> entry : memberData.entrySet()) {
            commandSubmitter.onAgentAppliedCommands(entry.getKey(), entry.getValue().appliedCommands);
        }
    }

    int getRandomPortNumber() {
        return random.nextInt((65535 - 1024)) + 1024;
    }
}
