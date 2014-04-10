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

import io.libraft.agent.protocol.AppendEntries;
import io.libraft.agent.protocol.AppendEntriesReply;
import io.libraft.agent.protocol.RaftRPC;
import io.libraft.agent.protocol.RequestVote;
import io.libraft.agent.protocol.RequestVoteReply;
import io.libraft.algorithm.RPCReceiver;
import io.libraft.algorithm.RaftConstants;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Stateless {@link ChannelHandler} that dispatches
 * an incoming {@link RaftRPC} message to the specified {@link RPCReceiver}.
 * The incoming {@code RaftRPC} message is dropped if its source
 * (given by {@link RaftRPC#getSource()}) is <strong>not</strong> a
 * part of the current Raft cluster.
 * <p/>
 * This implementation <strong>kills the JVM</strong> if the {@code RPCReceiver}
 * throws an exception while processing the incoming {@code RaftRPC} message.
 */
@ChannelHandler.Sharable
final class RPCHandler extends SimpleChannelUpstreamHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleChannelUpstreamHandler.class);

    private final String self;
    private final Set<String> cluster;
    private final RPCReceiver receiver;

    /**
     * Constructor.
     *
     * @param self unique id of the local Raft server
     * @param cluster set of unique ids - one for each server in the Raft cluster
     * @param receiver instance of {@code RPCReceiver} that will be notified of incoming {@code RaftRPC} messages
     */
    RPCHandler(String self, Set<String> cluster, RPCReceiver receiver) {
        this.self = self;
        this.cluster = cluster;
        this.receiver = receiver;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        RaftRPC rpc = (RaftRPC) e.getMessage();

        if (!rpc.getDestination().equals(self)) {
            LOGGER.warn("{}: discard {} from {} - wrong destination ({})", self, rpc, rpc.getSource(), rpc.getDestination());
            return;
        }

        if (!cluster.contains(rpc.getSource())) {
            LOGGER.warn("{}: discard {} from {} - not in cluster", self, rpc, rpc.getSource());
            return;
        }

        try {
            if (rpc instanceof RequestVote) {
                RequestVote requestVote = (RequestVote) rpc;
                receiver.onRequestVote(
                        requestVote.getSource(),
                        requestVote.getTerm(),
                        requestVote.getLastLogIndex(),
                        requestVote.getLastLogTerm()
                );

            } else if (rpc instanceof RequestVoteReply) {
                RequestVoteReply requestVoteReply = (RequestVoteReply) rpc;
                receiver.onRequestVoteReply(
                        requestVoteReply.getSource(),
                        requestVoteReply.getTerm(),
                        requestVoteReply.isVoteGranted());

            } else if (rpc instanceof AppendEntries) {
                AppendEntries appendEntries = (AppendEntries) rpc;
                receiver.onAppendEntries(
                        appendEntries.getSource(),
                        appendEntries.getTerm(),
                        appendEntries.getCommitIndex(),
                        appendEntries.getPrevLogIndex(),
                        appendEntries.getPrevLogTerm(),
                        appendEntries.getEntries());

            } else {
                AppendEntriesReply appendEntriesReply = (AppendEntriesReply) rpc;
                receiver.onAppendEntriesReply(
                        appendEntriesReply.getSource(),
                        appendEntriesReply.getTerm(),
                        appendEntriesReply.getPrevLogIndex(),
                        appendEntriesReply.getEntryCount(),
                        appendEntriesReply.isApplied());
            }
        } catch (Throwable t) {
            LOGGER.error("{}: uncaught exception processing rpc:{} from {}", self, rpc, rpc.getSource(), t);
            System.exit(RaftConstants.UNCAUGHT_THROWABLE_EXIT_CODE);
        }
    }
}
