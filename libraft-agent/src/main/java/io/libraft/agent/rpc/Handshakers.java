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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hibernate.validator.constraints.NotEmpty;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Provides {@link org.jboss.netty.channel.ChannelHandler} implementations
 * that send and receive/parse a {@code RaftNetworkClient} handshake message.
 * <p/>
 * When a {@link RaftNetworkClient} accepts a connection from a remote
 * server it's useful for it to know some information about the originating server.
 * To support this a handshake message is sent. The connection is not
 * accepted and upstream {@code RaftNetworkClient} code is not notified of this
 * connection until the handshake is received and successfully parsed.
 * A handshake contains the following fields:
 * <ul>
 *     <li><strong>serverId</strong>: unique server id of the remote {@code RaftNetworkClient}.</li>
 * </ul>
 */
abstract class Handshakers {

    private Handshakers() { } // to prevent instantiation

    /**
     * {@code RaftNetworkClient} handshake.
     * This object can be converted into JSON using a Jackson {@link ObjectMapper}.
     */
    private static final class Handshake {

        private static final String SERVER_ID = "serverId";

        @JsonProperty(SERVER_ID)
        @NotEmpty
        private final String serverId;

        @JsonCreator
        private Handshake(@JsonProperty(SERVER_ID) String serverId) {
            this.serverId = serverId;
        }

        @JsonIgnore
        public String getServerId() {
            return serverId;
        }
    }

    /**
     * Create a {@code RaftNetworkClient} handshake message.
     *
     * @param serverId unique id of the Raft server (sent in the handshake message)
     * @param mapper instance of {@code ObjectMapper} used to map handshake properties to
     *               their corresponding fields in the encoded handshake message
     * @return {@code ChannelBuffer} instance that contains the encoded handshake message
     * @throws JsonProcessingException if the handshake message cannot be constructed
     */
    static ChannelBuffer createHandshakeMessage(String serverId, ObjectMapper mapper) throws JsonProcessingException {
        return ChannelBuffers.wrappedBuffer(mapper.writeValueAsBytes(new Handshake(serverId)));
    }

    /**
     * Extract the unique id of the Raft server that sent a handshake
     * message from its wire representation.
     *
     * @param handshakeBuffer instance of {@code ChannelBuffer} that contains
     *                        the encoded handshake message
     * @param mapper instance of {@code ObjectMapper} used to map handshake fields
     *               in the encoded message to their corresponding Java representation
     * @return unique id of the Raft server that sent the handshake message
     * @throws IOException if a valid handshake cannot be read from the {@code handshakeBuffer}
     */
    static String getServerIdFromHandshake(ChannelBuffer handshakeBuffer, ObjectMapper mapper) throws IOException {
        Handshake handshake = getHandshakeFromBuffer(handshakeBuffer, mapper);
        return handshake.getServerId();
    }

    private static Handshake getHandshakeFromBuffer(ChannelBuffer handshakeBuffer, ObjectMapper mapper) throws IOException {
        byte[] handshakeBytes = new byte[handshakeBuffer.readableBytes()];
        handshakeBuffer.readBytes(handshakeBytes);
        return mapper.readValue(handshakeBytes, Handshake.class);
    }

    /**
     * Stateful {@link org.jboss.netty.channel.ChannelHandler}
     * that handles the receiving portion of the {@code RaftNetworkClient}
     * handshake protocol. This handler:
     * <ol>
     *     <li>Parses an incoming handshake message into its corresponding Java object.</li>
     *     <li>Implements the receiving portion of the handshake protocol.</li>
     * </ol>
     */
    static final class IncomingHandshakeHandler extends SimpleChannelHandler {

        private static final Logger LOGGER = LoggerFactory.getLogger(IncomingHandshakeHandler.class);

        private final ObjectMapper mapper;

        private SocketAddress remoteAddress;

        /**
         * Constructor.
         *
         * @param mapper instance of {@code ObjectMapper} used to map handshake
         *               JSON fields into their corresponding Java properties
         */
        IncomingHandshakeHandler(ObjectMapper mapper) {
            this.mapper = mapper;
        }

        @Override
        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            checkState(remoteAddress == null, "remoteAddress:%s", remoteAddress); // NOTE: this has to be saved so that a channelConnected event can be forwarded later

            remoteAddress = (SocketAddress) e.getValue();

            LOGGER.debug("drop channelConnected event and wait for incoming handshake");
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            checkNotNull(remoteAddress);

            // we assume that the framing handler is downstream,
            // and that the handshake is coming to us in one piece
            ChannelBuffer incomingHandshakeBuffer = (ChannelBuffer) e.getMessage();
            Handshake handshake = getHandshakeFromBuffer(incomingHandshakeBuffer, mapper);

            // set the channel attachment, remove this handler, and then pass a "channelConnected" event on
            ctx.getChannel().setAttachment(handshake.getServerId());
            ctx.getPipeline().remove(this);
            Channels.fireChannelConnected(ctx, remoteAddress);
        }
    }

    /**
     * Stateful {@link org.jboss.netty.channel.ChannelHandler}
     * that handles the sending portion of the {@code RaftNetworkClient}
     * handshake protocol. This handler:
     * <ol>
     *     <li>Converts a Java handshake object into its corresponding JSON representation.</li>
     *     <li>Implements the sending portion of the handshake protocol.</li>
     * </ol>
     */
    static final class OutgoingHandshakeHandler extends SimpleChannelHandler {

        private final ChannelBuffer handshakeBuffer;

        private ChannelFuture connectFuture;

        /**
         * Constructor.
         *
         * @param self unique id of the local Raft server
         * @param mapper instance of {@code ObjectMapper} used to map a
         *               Java handshake object's properties to their corresponding JSON fields
         */
        OutgoingHandshakeHandler(String self, ObjectMapper mapper) throws JsonProcessingException { // FIXME (AG): I'm not happy with constructors throwing exceptions
            this.handshakeBuffer = createHandshakeMessage(self, mapper);
        }

        @Override
        public void connectRequested(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            // hold on to the future so that we can determine when the
            // caller is notified that the channel has connected
            checkState(connectFuture == null, "connectFuture:%s", connectFuture);

            connectFuture = e.getFuture();

            // create the proxy connect future
            // this will be passed on, and we'll respond to it
            // and trigger the original future as necessary
            ChannelFuture proxyConnectFuture = Channels.future(e.getChannel());

            // trigger the original future if the proxy future failed (for whatever reason)
            proxyConnectFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        connectFuture.setFailure(future.getCause());
                    }
                }
            });

            // also trigger the original future if the channel is closed
            // I believe the original connect future should be triggered, but just in case....
            e.getChannel().getCloseFuture().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    connectFuture.setFailure(new ClosedChannelException());
                }
            });

            // forward the new connect request
            Channels.connect(ctx, proxyConnectFuture, (SocketAddress) e.getValue());
        }

        @Override
        public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
            checkNotNull(connectFuture);

            // the channel connected - don't forward the connect
            // event on. instead, send the handshake instead
            ChannelFuture writeFuture = Channels.future(e.getChannel());
            Channels.write(ctx, writeFuture, handshakeBuffer);

            // deal with the original connect future
            writeFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        // fire the original future, remove this handler and pass on the event (order is crucial)
                        ctx.getPipeline().remove(OutgoingHandshakeHandler.this);
                        connectFuture.setSuccess();
                        OutgoingHandshakeHandler.super.channelConnected(ctx, e);
                    } else {
                        // fire the original future and close the channel (order is crucial)
                        connectFuture.setFailure(future.getCause());
                        future.getChannel().close();
                    }
                }
            });
        }

        /**
         * Set the {@link ChannelFuture} associated with the
         * original downstream {@code connectRequested} event on this channel.
         * <p/>
         * <strong>This method is package-private for testing
         * reasons only!</strong> It should <strong>never</strong>
         * be called in a non-test context!
         *
         * @param connectFuture future associated with the original downstream {@code connectRequested} event
         */
        void setConnectFutureForUnitTestOnly(ChannelFuture connectFuture) {
            checkState(this.connectFuture == null, "connectFuture:%s", this.connectFuture);
            this.connectFuture = connectFuture;
        }
    }
}
