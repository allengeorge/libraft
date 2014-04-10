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

import com.google.common.base.Objects;
import org.hibernate.validator.constraints.NotEmpty;
import org.jboss.netty.channel.Channel;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Defines a Raft server in the Raft cluster.
 */
public final class RaftMember {

    @NotEmpty
    private final String id;

    @NotNull
    private final SocketAddress address;

    private final AtomicReference<Channel> channel = new AtomicReference<Channel>(null);

    /**
     * Constructor.
     *
     * @param id unique, non-null, non-empty id of the Raft server
     * @param address endpoint at which the Raft server is listening. This may be a
     *                network socket address, a local address, <em>etc.</em>
     */
    public RaftMember(String id, SocketAddress address) {
        this.id = id;
        this.address = address;
    }

    /**
     * Get the unique id of the Raft server.
     *
     * @return unique, non-null, non-empty id of the Raft server
     */
    public String getId() {
        return id;
    }

    /**
     * Get the address the Raft server is listening on.
     *
     * @return address the Raft server is listening on. This may be a network
     *         socket address, a local address, <em>etc.</em>
     */
    public SocketAddress getAddress() {
        return address;
    }

    /**
     * Get the {@link Channel} used to communicate with the Raft server.
     *
     * @return valid (i.e. connected) {@code Channel} used to communicate with the Raft server.
     *         {@code null} if no connection exists
     */
    public @Nullable Channel getChannel() {
        return channel.get();
    }

    /**
     * Set the {@link Channel} used to communicate with the Raft server.
     *
     * @param channel a valid, <strong>connected</strong> {@code Channel} to the
     *                Raft server. {@code null} if no connection exists
     */
    public void setChannel(@Nullable Channel channel) {
        this.channel.set(channel);
    }

    /**
     * Set the {@link Channel} used to communicate with the Raft server
     * to {@code newChannel} <strong>only if</strong> the current channel is {@code oldChannel}.
     * noop otherwise.
     *
     * @param oldChannel expected value of the {@code Channel}
     * @param newChannel a valid, <strong>connected</strong>d {@code Channel} to the Raft server.
     *                   {@code null} if no connection exists
     * @return true if the value was set to {@code newChannel}, false otherwise
     */
    public boolean compareAndSetChannel(@Nullable Channel oldChannel, @Nullable Channel newChannel) {
        return channel.compareAndSet(oldChannel, newChannel);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, address);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof RaftMember)) {
            return false;
        }
        if (o == this) {
            return true;
        }

        RaftMember other = (RaftMember) o;
        return id.equalsIgnoreCase(other.id) && address.equals(other.address);
    }

    @Override
    public String toString() {
        Channel currentChannel = channel.get();

        return Objects
                .toStringHelper(this)
                .add("id", id)
                .add("address", address.toString())
                .add("channel", currentChannel == null ? "null" : currentChannel.getId())
                .toString();
    }
}
