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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.libraft.Command;
import io.libraft.Committed;
import io.libraft.NotLeaderException;
import io.libraft.Raft;
import io.libraft.RaftListener;
import io.libraft.SnapshotWriter;
import io.libraft.agent.configuration.RaftClusterConfiguration;
import io.libraft.agent.configuration.RaftConfiguration;
import io.libraft.agent.configuration.RaftConfigurationLoader;
import io.libraft.agent.configuration.RaftDatabaseConfiguration;
import io.libraft.agent.configuration.RaftSnapshotsConfiguration;
import io.libraft.agent.persistence.JDBCLog;
import io.libraft.agent.persistence.JDBCStore;
import io.libraft.agent.snapshots.OnDiskSnapshotsStore;
import io.libraft.agent.protocol.RaftRPC;
import io.libraft.agent.rpc.RaftNetworkClient;
import io.libraft.algorithm.RaftAlgorithm;
import io.libraft.algorithm.StorageException;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientBossPool;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerBossPool;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorker;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.channel.socket.nio.ShareableWorkerPool;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkState;

/**
 * Component for controlling and interacting with the Raft cluster.
 * <p/>
 * This class:
 * <ul>
 *     <li>Instantiates and manages the lifecycle for
 *         service implementations required by {@link RaftAlgorithm}.</li>
 *     <li>Allows callers to interact with the Raft cluster via the {@link Raft} interface.</li>
 * </ul>
 * {@code RaftAgent} requires that {@link Command} instances be
 * serialized and deserialized into a suitable form for network transmission
 * and durable storage. Two {@code Command} types are supported:
 * <ul>
 *     <li>Jackson-annotated classes.</li>
 *     <li>POJOs.</li>
 * </ul>
 * <h3>Jackson-annotated classes</h3>
 * {@code RaftAgent} supports Jackson-annotated classes that derive from a <strong>single</strong> base class.
 * To setup serialization and deserialization for these classes callers should call
 * {@link RaftAgent#setupJacksonAnnotatedCommandSerializationAndDeserialization(Class)}
 * with the <strong>base class</strong> of the {@code Command} hierarchy as the parameter.
 * For example:
 * <pre>
 *     // command hierarchy
 *     CMD_BASE
 *     +-- CMD_SUB1
 *     +-- CMD_SUB2
 *
 *     // serialization/deserialization setup
 *     raftAgent.setupJacksonAnnotatedCommandSerializationAndDeserialization(CMD_BASE.class);
 * </pre>
 * <h3>POJOs</h3>
 * {@code RaftAgent} supports arbitrary POJOs tagged with the {@code Command} interface.
 * Callers must implement {@link CommandSerializer} and {@link CommandDeserializer} to transform
 * their POJOs to and from <strong>binary</strong>. To setup serialization and deserialization for
 * these classes callers should call {@link RaftAgent#setupCustomCommandSerializationAndDeserialization(CommandSerializer, CommandDeserializer)}
 * with their {@code CommandSerializer} and {@code CommandDeserializer} implementation instances
 * as parameters.
 * <p/>
 * {@code RaftAgent} should <strong>only</strong> be initialized with <strong>one</strong> of:
 * <ul>
 *     <li>{@link RaftAgent#setupJacksonAnnotatedCommandSerializationAndDeserialization(Class)}</li>
 *     <li>{@link RaftAgent#setupCustomCommandSerializationAndDeserialization(CommandSerializer, CommandDeserializer)}</li>
 * </ul>
 * For examples on setting up both kinds of serialization and deserialization see {@code RaftAgentTest}.
 * <p/>
 * This component is thread-safe.
 */
@SuppressWarnings("unused")
public class RaftAgent implements Raft {

    /**
     * Create an instance of {@code RaftAgent} from a configuration file.
     *
     * @param configFile location of the JSON configuration file. The
     *                   configuration in this file will be validated.
     *                   See the project README.md for more on the configuration file
     * @param raftListener instance of {@code RaftListener} that will be notified of events from the Raft cluster
     * @return valid {@code RaftAgent} that can be used to connect to, and (if leader),
     *         replicate {@link Command} instances to the Raft cluster
     * @throws IOException if the configuration file cannot be loaded or processed (i.e. contains invalid JSON)
     */
    public static RaftAgent fromConfigurationFile(String configFile, RaftListener raftListener) throws IOException {
        RaftConfiguration configuration = RaftConfigurationLoader.loadFromFile(configFile);
        return fromConfigurationObject(configuration, raftListener);
    }

    /**
     * Create an instance of {@code RaftAgent} from a {@code RaftConfiguration} object.
     * This method can be used when the {@code RaftAgent} configuration is part of a larger configuration.
     *
     * @param configuration instance of {@code RaftConfiguration} with the configuration to be used.
     *                      This object will be validated
     * @param raftListener instance of {@code RaftListener} that will be notified of events from the Raft cluster
     * @return valid {@code RaftAgent} that can be used to connect to, and (if leader),
     *         replicate {@link Command} instances to the Raft cluster
     */
    public static RaftAgent fromConfigurationObject(RaftConfiguration configuration, RaftListener raftListener) {
        RaftConfigurationLoader.validate(configuration);
        return new RaftAgent(configuration, raftListener);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftAgent.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final RaftNetworkClient raftNetworkClient;
    private final RaftAlgorithm raftAlgorithm;
    private final JDBCStore jdbcStore;
    private final JDBCLog jdbcLog;
    private final OnDiskSnapshotsStore snapshotStore;
    private final WrappedTimer timer;

    private ListeningExecutorService nonIoExecutorService;
    private ExecutorService ioExecutorService;
    private NioServerBossPool serverBossPool;
    private NioClientBossPool clientBossPool;
    private NioWorkerPool workerPool;
    private ShareableWorkerPool<NioWorker> sharedWorkerPool;

    private volatile boolean running; // set during start/stop and accessed by multiple threads

    private boolean setupConversion;
    private boolean initialized;

    private RaftAgent(RaftConfiguration configuration, RaftListener raftListener) {
        // default serializer and deserializer (based on Jackson)
        JacksonBasedCommandSerializer commandSerializer = new JacksonBasedCommandSerializer(mapper);
        JacksonBasedCommandDeserializer commandDeserializer = new JacksonBasedCommandDeserializer(mapper);

        RaftSnapshotsConfiguration snapshotsConfiguration = configuration.getRaftSnapshotsConfiguration();

        // TODO (AG): use this datasource and dbi across store, log, etc.
        // raft database setup
        RaftDatabaseConfiguration raftDatabaseConfiguration = configuration.getRaftDatabaseConfiguration();
        DataSource dataSource = new DataSource();
        dataSource.setName("raft-db");
        dataSource.setDriverClassName(raftDatabaseConfiguration.getDriverClass());
        dataSource.setUrl(raftDatabaseConfiguration.getUrl());
        dataSource.setInitialSize(3);
        dataSource.setMaxActive(3);
        dataSource.setMinIdle(dataSource.getMaxActive());
        dataSource.setMaxIdle(dataSource.getMaxActive());
        dataSource.setLogAbandoned(true);
        if (raftDatabaseConfiguration.getUser() != null) {
            dataSource.setUsername(raftDatabaseConfiguration.getUser());
        }
        if (raftDatabaseConfiguration.getPassword() != null) {
            dataSource.setPassword(raftDatabaseConfiguration.getPassword());
        }

        try {
            dataSource.createPool();
        } catch (SQLException e) {
            throw new IllegalArgumentException("fail create pool", e); // FIXME (AG): this is hugely bogus
        }

        DBI dbi = new DBI(dataSource);

        try {
            Class.forName(raftDatabaseConfiguration.getDriverClass());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
        jdbcStore = new JDBCStore(raftDatabaseConfiguration.getUrl(), raftDatabaseConfiguration.getUser(), raftDatabaseConfiguration.getPassword());
        jdbcLog = new JDBCLog(raftDatabaseConfiguration.getUrl(), raftDatabaseConfiguration.getUser(), raftDatabaseConfiguration.getPassword(), commandSerializer, commandDeserializer);
        snapshotStore = new OnDiskSnapshotsStore(dbi, snapshotsConfiguration.getSnapshotsDirectory());

        // network and algorithm setup
        timer = new WrappedTimer();

        Random random = new Random();
        RaftClusterConfiguration raftClusterConfiguration = configuration.getRaftClusterConfiguration();
        Set<RaftMember> cluster = raftClusterConfiguration.getMembers();
        raftNetworkClient = new RaftNetworkClient(
                random,
                timer,
                mapper,
                getSelfAsMember(raftClusterConfiguration.getSelf(), cluster),
                cluster,
                configuration.getConnectTimeout(),
                configuration.getMinReconnectInterval(),
                configuration.getAdditionalReconnectIntervalRange(),
                configuration.getTimeUnit());
        raftAlgorithm = new RaftAlgorithm(
                random,
                timer,
                raftNetworkClient,
                jdbcStore,
                jdbcLog,
                snapshotStore,
                raftListener,
                raftClusterConfiguration.getSelf(),
                getMemberIds(cluster),
                snapshotsConfiguration.getMinEntriesToSnapshot(),
                snapshotsConfiguration.getSnapshotCheckInterval(),
                configuration.getRPCTimeout(),
                configuration.getMinElectionTimeout(),
                configuration.getAdditionalElectionTimeoutRange(),
                configuration.getHeartbeatInterval(),
                configuration.getTimeUnit());
    }

    private RaftMember getSelfAsMember(String self, Collection<RaftMember> cluster) {
        RaftMember selfMember = null;
        for (RaftMember raftMember : cluster) {
            if (raftMember.getId().equals(self)) {
                selfMember = raftMember;
                break;
            }
        }

        checkState(selfMember != null);

        return selfMember;
    }

    private Set<String> getMemberIds(Set<RaftMember> cluster) {
        Set<String> clusterIds = Sets.newHashSet();

        for (RaftMember raftMember : cluster) {
            clusterIds.add(raftMember.getId());
        }

        return clusterIds;
    }

    /**
     * Setup serialization and deserialization for Jackson-annotated {@code Command} objects.
     * This method should <strong>only</strong> be called once.
     *
     * @param commandSubclassKlass the base class of the Jackson-annotated {@code Command} classes
     * @param <CommandSubclass> the base-class type of the Jackson-annotated {@code Command} classes
     * @throws IllegalStateException if this method is called multiple times
     *
     * @see RaftRPC#setupJacksonAnnotatedCommandSerializationAndDeserialization(ObjectMapper, Class)
     */
    public synchronized <CommandSubclass extends Command> void setupJacksonAnnotatedCommandSerializationAndDeserialization(Class<CommandSubclass> commandSubclassKlass) {
        checkState(!running);
        checkState(!initialized);
        checkState(!setupConversion);

        RaftRPC.setupJacksonAnnotatedCommandSerializationAndDeserialization(mapper, commandSubclassKlass);

        setupConversion = true;
    }

    /**
     * Setup custom serialization and deserialization for POJO {@link Command} objects.
     * This method should <strong>only</strong> be called once.
     *
     * @param commandSerializer {@code CommandSerializer} that can serialize a POJO {@code Command} into binary
     * @param commandDeserializer {@code CommandDeserializer} that can deserialize binary into a {@code Command} POJO
     * @throws IllegalStateException if this method is called multiple times
     *
     * @see RaftRPC#setupCustomCommandSerializationAndDeserialization(ObjectMapper, CommandSerializer, CommandDeserializer)
     */
    public synchronized void setupCustomCommandSerializationAndDeserialization(CommandSerializer commandSerializer, CommandDeserializer commandDeserializer) {
        checkState(!running);
        checkState(!initialized);
        checkState(!setupConversion);

        jdbcLog.setupCustomCommandSerializerAndDeserializer(commandSerializer, commandDeserializer);
        RaftRPC.setupCustomCommandSerializationAndDeserialization(mapper, commandSerializer, commandDeserializer);

        setupConversion = true;
    }

    /**
     * Initialize the local Raft server.
     * <p/>
     * Sets up the service implementation classes, creates database
     * tables and starts any thread pools necessary. Following this
     * call all service classes are <strong>fully initialized</strong>.
     * Even though various threads are started they <strong>will not</strong>
     * use or interact with the service implementation classes. Callers
     * still have exclusive access to the system.
     * <p/>
     * This method should <strong>only</strong> be called once before {@link RaftAgent#start()}.
     *
     * @throws StorageException if the persistence components cannot be initialized
     * @throws IllegalStateException if this method is called multiple times
     */
    public synchronized void initialize() throws StorageException {
        checkState(!running);
        checkState(!initialized);
        checkState(setupConversion);

        // start up the snapshots subsystem
        snapshotStore.initialize();
        // check that the snapshot metadata and the filesystem agree
        // FIXME (AG): this _may_ be expensive, especially if the user never bothers to clean out snapshots!
        // FIXME (AG): warning, warning - this is upfront work - probably a very, very bad idea
        snapshotStore.reconcileSnapshots();

        // initialize the log and store
        jdbcLog.initialize();
        jdbcStore.initialize();

        // initialize the various thread pools
        nonIoExecutorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        ioExecutorService = Executors.newCachedThreadPool();
        serverBossPool = new NioServerBossPool(ioExecutorService, 1);
        clientBossPool = new NioClientBossPool(ioExecutorService, 1);
        workerPool = new NioWorkerPool(ioExecutorService, 3);

        // TODO (AG): avoid creating threads in the initialize() method
        // initialize the networking subsystem
        sharedWorkerPool = new ShareableWorkerPool<NioWorker>(workerPool);
        ServerSocketChannelFactory serverChannelFactory = new NioServerSocketChannelFactory(serverBossPool, sharedWorkerPool);
        ClientSocketChannelFactory clientChannelFactory = new NioClientSocketChannelFactory(clientBossPool, sharedWorkerPool);
        raftNetworkClient.initialize(nonIoExecutorService, serverChannelFactory, clientChannelFactory, raftAlgorithm);

        raftAlgorithm.initialize();

        initialized = true;
    }

    /**
     * Start the local Raft server.
     * <p/>
     * This call <strong>must</strong> only be made <strong>after</strong>:
     * <ol>
     *     <li>one of {@link RaftAgent#setupJacksonAnnotatedCommandSerializationAndDeserialization(Class)} or
     *         {@link RaftAgent#setupCustomCommandSerializationAndDeserialization(CommandSerializer, CommandDeserializer)}</li>
     *     <li>{@link RaftAgent#initialize()}</li>
     * </ol>
     * Following a successful call to {@code start()} callers
     * <strong>no longer</strong> have exclusive access to system resources.
     * <p/>
     * Once a successful call to {@code start()} is made subsequent calls are noops.
     *
     * @throws IllegalStateException if this method is called before {@code RaftAgent} is initialized
     */
    public synchronized void start() {
        checkState(setupConversion);
        checkState(initialized);

        if (running) {
            return;
        }

        LOGGER.info("starting raft agent");

        timer.start();
        raftNetworkClient.start();
        raftAlgorithm.start();

        running = true;
    }

    /**
     * Stop the local Raft server.
     * <p/>
     * Stops and terminates all thread pools and cancels all timers.
     * Pending timeout tasks will be <strong>dropped</strong>. Following
     * a successful call to {@code stop()} callers will have exclusive
     * access to system resources again. Callers may choose to release this
     * {@code RaftAgent} instance <strong>or</strong> call {@link RaftAgent#start()}
     * again later to restart the {@code RaftAgent} - either is supported.
     * <p/>
     * Once a successful call to {@code stop()} is made subsequent calls are noops.
     */
    public synchronized void stop() {
        if (!running) {
            return;
        }

        LOGGER.info("stopping raft agent");

        raftAlgorithm.stop();
        raftNetworkClient.stop();

        serverBossPool.shutdown();
        clientBossPool.shutdown();
        workerPool.shutdown();
        sharedWorkerPool.shutdown();
        sharedWorkerPool.destroy();
        ioExecutorService.shutdownNow();
        nonIoExecutorService.shutdownNow();

        timer.stop();

        snapshotStore.teardown();
        jdbcLog.teardown();
        jdbcStore.teardown();

        running = false;
        initialized = false;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if this method is called while {@code RaftAgent} is not running
     */
    @Override
    public void snapshotWritten(SnapshotWriter snapshotWriter) {
        checkState(running); // implies conversion setup and initialization happened
        raftAlgorithm.snapshotWritten(snapshotWriter);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if this method is called before {@code RaftAgent} is initialized
     */
    @Override
    public @Nullable Committed getNextCommitted(long indexToSearchFrom) {
        checkState(setupConversion);
        checkState(initialized);

        return raftAlgorithm.getNextCommitted(indexToSearchFrom);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if this method is called when {@code RaftAgent} is not running
     */
    @Override
    public ListenableFuture<Void> submitCommand(Command command) throws NotLeaderException {
        checkState(running); // implies conversion setup and initialization happened

        // since access to raftAlgorithm is not serialized
        // here it's possible for raftAlgorithm to be stopped
        // by one thread in the time between a caller thread
        // doing a successful check above and calling submitCommand
        // below, triggering an ISE. I judge this acceptable.
        // The alternative is serializing access to raftAlgorithm here
        // (raftAlgorithm methods themselves are serialized) and
        // holding yet another lock while calling into raftAlgorithm

        return raftAlgorithm.submitCommand(command);
    }
}
