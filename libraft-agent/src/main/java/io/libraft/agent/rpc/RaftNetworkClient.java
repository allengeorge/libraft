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

package io.libraft.agent.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.libraft.agent.RaftMember;
import io.libraft.agent.protocol.AppendEntries;
import io.libraft.agent.protocol.AppendEntriesReply;
import io.libraft.agent.protocol.RaftRPC;
import io.libraft.agent.protocol.RequestVote;
import io.libraft.agent.protocol.RequestVoteReply;
import io.libraft.algorithm.LogEntry;
import io.libraft.algorithm.RPCException;
import io.libraft.algorithm.RPCReceiver;
import io.libraft.algorithm.RPCSender;
import io.libraft.algorithm.Timer;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ServerChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Network client that establishes both incoming and outgoing connections
 * to <strong>every other</strong> server in the Raft cluster. Assuming that
 * all servers in the cluster are functioning properly the cluster
 * will eventually be fully connected.
 * <p/>
 * This implementation <strong>retries</strong> failed or
 * broken connections repeatedly until they are established.
 * Following a connection failure {@code RaftNetworkClient}
 * will wait for {@code reconnectInterval} before attempting
 * to reestablish the connection. Two parameters control retry
 * behaviour:
 * <ol>
 *     <li>{@code minReconnectInterval}</li>
 *     <li>{@code additionalReconnectIntervalRange}</li>
 * </ol>
 * To avoid synchronized reconnections across the cluster
 * {@code reconnectInterval} is randomized using the formula:
 * <pre>
 *     reconnectInterval = minReconnectInterval + randomInRange(0, additionalReconnectIntervalRange)
 * </pre>
 * Where:
 * <ol>
 *     <li>{@code minReconnectInterval} acts as the floor to
 *         {@code reconnectInterval}. It is the <strong>minimum</strong> time
 *         {@code RaftNetworkClient} will wait before reconnecting.</li>
 *     <li>{@code additionalReconnectIntervalRange} acts as the random
 *         portion of {@code reconnectInterval}. It is the
 *         <strong>maximum additional</strong> time (optionally, 0)
 *         {@code RaftNetworkClient} will wait on top of {@code reconnectInterval}
 *         before reconnecting.</li>
 * </ol>
 * This means that {@code reconnectInterval} has the range
 * {@code [minReconnectInterval, minReconnectInterval + additionalReconnectIntervalRange]}.
 * <p/>
 * This component is thread-safe.
 */
public final class RaftNetworkClient implements RPCSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNetworkClient.class);

    private final ConcurrentMap<String, RaftMember> cluster = Maps.newConcurrentMap();
    private final ChannelGroup clusterChannelGroup = new DefaultChannelGroup("cluster clients");
    private final Random random;
    private final Timer timer;
    private final ObjectMapper mapper;
    private final RaftMember self;
    private final int connectTimeout;
    private final int minReconnectInterval;
    private final int additionalReconnectIntervalRange;
    private final TimeUnit timeUnit;

    private volatile boolean running; // set during start/stop and accessed by netty-I/O and RaftNetworkClient caller threads

    private ServerBootstrap server;
    private ClientBootstrap client;
    private Channel serverChannel;

    /**
     * Constructor.
     *
     * @param random instance of {@code Random} used to generate timeout intervals
     * @param timer instance of {@code Timer} used to schedule network timeouts
     * @param mapper instance of {@code ObjectMapper} used to generate JSON representations of {@link RaftRPC} messages
     * @param self unique id of the local Raft server
     * @param cluster set of unique ids - one for each Raft server in the cluster
     * @param connectTimeout maximum time {@code RaftNetworkClient} waits to establish a connection to another Raft server
     * @param minReconnectInterval minimum amount of time to wait before reconnecting to a Raft server
     * @param additionalReconnectIntervalRange maximum additional time added to {@code minReconnectInterval} to get
     *                                         the actual reconnect interval
     * @param timeUnit time unit in which all time intervals are specified
     */
    public RaftNetworkClient(
            Random random,
            Timer timer,
            ObjectMapper mapper,
            RaftMember self,
            Set<RaftMember> cluster,
            int connectTimeout,
            int minReconnectInterval,
            int additionalReconnectIntervalRange,
            TimeUnit timeUnit
    ) {
        checkArgument(connectTimeout > 0);
        checkArgument(minReconnectInterval > 0);
        checkArgument(additionalReconnectIntervalRange >= 0);

        this.random = random;
        this.timer = timer;
        this.mapper = mapper;
        this.self = self;
        this.connectTimeout = connectTimeout;
        this.minReconnectInterval = minReconnectInterval;
        this.additionalReconnectIntervalRange = additionalReconnectIntervalRange;
        this.timeUnit = timeUnit;

        for (RaftMember raftMember : cluster) {
            if (!raftMember.getId().equalsIgnoreCase(self.getId())) {
                raftMember.setChannel(null);
                this.cluster.put(raftMember.getId(), raftMember);
            }
        }
    }

    /**
     * Initialize the {@code RaftNetworkClient} instance.
     * <p/>
     * Sets up the {@code RaftNetworkClient} client and server modules.
     * Following a call to {@code initialize()} network threads are active
     * but will not accept any incoming connections or establish any
     * outgoing connections. As a result the caller still has
     * exclusive access to underlying system resources.
     *
     * @param nonIoExecutorService shared instance of {@code ListeningExecutorService} used to handle any non-io tasks (address-resolution, etc.)
     * @param serverChannelFactory instance of {@code ServerChannelFactory} used to handle incoming connections from remote Raft servers
     * @param clientChannelFactory instance of {@code ChannelFactory} used to create outgoing connections to remote Raft servers
     * @param receiver instance of {@code RPCReceiver} that will be notified of incoming {@link RaftRPC} messages
     * @throws IllegalStateException if this method is called multiple times
     */
    public synchronized void initialize(
            ListeningExecutorService nonIoExecutorService,
            ServerChannelFactory serverChannelFactory,
            ChannelFactory clientChannelFactory,
            RPCReceiver receiver) {
        checkState(!running);
        checkState(server == null);
        checkState(client == null);

        final AddressResolverHandler resolverHandler = new AddressResolverHandler(nonIoExecutorService); // sharable, because it is using the default provider
        final FinalUpstreamHandler finalUpstreamHandler = new FinalUpstreamHandler(self.getId());
        final RPCHandler rpcHandler = new RPCHandler(self.getId(), cluster.keySet(), receiver);
        final RPCConverters.RPCEncoder rpcEncoder = new RPCConverters.RPCEncoder(mapper);
        final RPCConverters.RPCDecoder rpcDecoder = new RPCConverters.RPCDecoder(mapper);

        server = new ServerBootstrap(serverChannelFactory);
        server.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("frame-decoder", new Framers.FrameDecoder());
                pipeline.addLast("handshake", new Handshakers.IncomingHandshakeHandler(mapper));
                pipeline.addLast("rpc-decoder", rpcDecoder);
                pipeline.addLast("raftrpc", rpcHandler);
                pipeline.addLast("final", finalUpstreamHandler);
                return pipeline;
            }
        });

        client = new ClientBootstrap(clientChannelFactory);
        client.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("resolver", resolverHandler);
                pipeline.addLast("frame-encoder", new Framers.FrameEncoder());
                pipeline.addLast("handshake", new Handshakers.OutgoingHandshakeHandler(self.getId(), mapper));
                pipeline.addLast("rpc-encoder", rpcEncoder);
                pipeline.addLast("final", finalUpstreamHandler);
                return pipeline;
            }
        });
    }

    /**
     * Start the {@code RaftNetworkClient}.
     * <p/>
     * Following a call to {@code start()} the network threads
     * will begin to do work. This means that the server component
     * will accept and service incoming connections. Likewise, the
     * the client component will attempt to establish outgoing
     * connections to other Raft servers. As a result the caller
     * no longer has exclusive access to underlying system resources.
     * <p/>
     * Once a successful call to {@code start()} is made subsequent calls are noops.
     */
    public synchronized void start() {
        if (running) {
            return;
        }

        LOGGER.info("{}: starting network client", self.getId());

        checkNotNull(server);
        checkNotNull(client);

        // IMPORTANT: set running _early, up here_
        // this is because the bind() and connect()
        // calls below will not succeed if their
        // callbacks see "running" as false
        running = true;

        SocketAddress bindAddress = getResolvedBindAddress();
        serverChannel = server.bind(bindAddress);

        for (RaftMember server : cluster.values()) {
            connect(server);
        }
    }

    private SocketAddress getResolvedBindAddress() { // FIXME (AG): find a way to remove this and put it into a handler
        SocketAddress bindAddress = self.getAddress();

        if (bindAddress instanceof InetSocketAddress) {
            InetSocketAddress inetBindAddress = (InetSocketAddress) bindAddress;
            if (inetBindAddress.isUnresolved()) {
                bindAddress = new InetSocketAddress(inetBindAddress.getHostName(), inetBindAddress.getPort());
            }
        }

        return bindAddress;
    }

    private void connect(final RaftMember server) {
        if (!running) {
            logNotRunning();
            return;
        }

        final String selfId = self.getId();
        final String serverId = server.getId();

        LOGGER.debug("{}: attempt connect to {}", selfId, serverId);

        ChannelFuture connectFuture = client.connect(server.getAddress());

        final Channel channel = connectFuture.getChannel();
        channel.setAttachment(serverId); // ONLY store the String name, not the RaftMember object itself!
        clusterChannelGroup.add(channel);

        channel.getCloseFuture().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOGGER.debug("{}: channel closed to {}", selfId, serverId);

                boolean swapped = server.compareAndSetChannel(future.getChannel(), null); // Hmm...previous should work
                if (!swapped) {
                    LOGGER.warn("{}: channel replaced to {}", selfId, serverId);
                }

                if (!running) {
                    logNotRunning();
                    return;
                }

                int reconnectTimeout;
                if (additionalReconnectIntervalRange > 0) {
                    reconnectTimeout = minReconnectInterval + random.nextInt(additionalReconnectIntervalRange);
                } else {
                    reconnectTimeout = minReconnectInterval;
                }

                timer.newTimeout(new Timer.TimeoutTask() {
                    @Override
                    public void run(Timer.TimeoutHandle timeoutHandle) {
                        try {
                            connect(server);
                        } catch (Exception e) {
                            LOGGER.warn("{}: fail reconnect to {}", selfId, serverId, e);
                        }
                    }
                }, reconnectTimeout, timeUnit);
            }
        });

        final Timer.TimeoutHandle connectTimeoutHandle = timer.newTimeout(new Timer.TimeoutTask() {
            @Override
            public void run(Timer.TimeoutHandle timeoutHandle) {
                try {
                    if (!channel.isConnected() && !channel.getCloseFuture().isDone()) {
                        LOGGER.warn("{}: connect timeout to {} triggered", selfId, serverId);
                        channel.close();
                    }
                } catch (Exception e) {
                    LOGGER.warn("{}: fail close unconnected channel to {}", selfId, serverId, e);
                }
            }
        }, connectTimeout, timeUnit);

        connectFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                connectTimeoutHandle.cancel();

                if (!future.isSuccess()) {
                    channel.close();
                    return; // reconnect handled by closeFuture
                }

                // will this case actually happen?
                // basically, to get here we would have to schedule the connect
                // just before stop() is called, after clusterChannelGroup.disconnect()
                // is called, and just before clusterChannelGroup.add() is called...
                if (!running) {
                    logNotRunning();
                    channel.close();
                    return;
                }

                LOGGER.info("{}: channel connected to {}", selfId, serverId);
                LOGGER.trace("{}: attempt set channel for {} to {}", selfId, server, channel.getId());

                boolean channelSet = server.compareAndSetChannel(null, channel);
                checkState(channelSet, "%s: fail set channel for %s", selfId, serverId); // FIXME (AG): now this will probably break!
            }
        });
    }

    private void logNotRunning() {
        LOGGER.info("{}: network client stopped", self.getId());
    }

    /**
     * Stop the {@code RaftNetworkClient}.
     * <p/>
     * When a call to {@code stop()} is made
     * all existing incoming and outgoing connections
     * are disconnected and closed. The server component
     * will stop accepting incoming connections and the
     * client component will not initiate any connections
     * to other Raft servers. Pending reconnect timeouts
     * will fail silently. Since these timeouts may yet
     * run in the future callers <strong>cannot</strong>
     * assume exclusive access to system resources immediately
     * following {@code stop()}.
     * <p/>
     * The network thread pools <strong>are not</strong>
     * terminated. It is the caller's responsibility to call
     * {@link ChannelFactory#releaseExternalResources()} on
     * {@code serverChannelFactory} and {@code clientChannelFactory}
     * to terminate them.
     * <p/>
     * Following a successful call to {@code stop()} subsequent calls are noops.
     */
    public synchronized void stop() {
        // set running to 'false' early, so that new
        // I/O operations and timeouts are rejected
        // not checking the value of 'running'
        // allows 'stop' to be called multiple times,
        // AFAIK, this is not an issue and are noops
        running = false;

        LOGGER.info("{}: stopping network client", self.getId());

        if (serverChannel != null) {
            serverChannel.disconnect().awaitUninterruptibly();
        }

        if (server != null) {
            server.shutdown();
        }

        clusterChannelGroup.disconnect().awaitUninterruptibly();

        if (client != null) {
            client.shutdown();
        }
    }

    private void write(String server, RaftRPC rpc) throws RPCException { // safe to access simultaneously by multiple threads
        RaftMember raftMember = cluster.get(server);
        checkNotNull(raftMember);

        if (!running) {
            logNotRunning();
            throw new RPCException("network client stopped");
        }

        Channel memberChannel = raftMember.getChannel();
        if (memberChannel == null) {
            throw new RPCException("no connection to " + server);
        }

        memberChannel.write(rpc); // FIXME (AG): make this a blocking write with a write timeout
    }

    @Override
    public void requestVote(String server, long term, long lastLogIndex, long lastLogTerm) throws RPCException {
        write(server, new RequestVote(self.getId(), server, term, lastLogIndex, lastLogTerm));
    }

    @Override
    public void requestVoteReply(String server, long term, boolean voteGranted) throws RPCException {
        write(server, new RequestVoteReply(self.getId(), server, term, voteGranted));
    }

    @Override
    public void appendEntries(String server, long term, long commitIndex, long prevLogIndex, long prevLogTerm, @Nullable Collection<LogEntry> entries) throws RPCException {
        write(server, new AppendEntries(self.getId(), server, term, commitIndex, prevLogIndex, prevLogTerm, entries));
    }

    @Override
    public void appendEntriesReply(String server, long term, long prevLogIndex, long entryCount, boolean applied) throws RPCException {
        write(server, new AppendEntriesReply(self.getId(), server, term, prevLogIndex, entryCount, applied));
    }
}
