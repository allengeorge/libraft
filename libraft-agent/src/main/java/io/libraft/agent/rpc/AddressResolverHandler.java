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

package io.libraft.agent.rpc;

import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.DownstreamChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;

import java.net.InetSocketAddress;

// FIXME (AG): resolve the address on an external executor
/**
 * Stateless {@code ChannelHandler} that resolves an
 * <strong>unresolved</strong> {@link InetSocketAddress}
 * <strong>on the network IO thread</strong> before
 * initiating an outgoing connection. If the OS name-resolution
 * service is unavailable this <strong>can starve</strong>
 * already-connected {@link org.jboss.netty.channel.Channel} instances.
 */
@ChannelHandler.Sharable
final class AddressResolverHandler extends SimpleChannelDownstreamHandler {

    @Override
    public void connectRequested(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        ChannelStateEvent forwardedEvent = e;

        if (e.getValue() instanceof InetSocketAddress) {
            InetSocketAddress connectAddress = (InetSocketAddress) e.getValue();
            if (connectAddress.isUnresolved()) {
                connectAddress = new InetSocketAddress(connectAddress.getHostName(), connectAddress.getPort());
                forwardedEvent = new DownstreamChannelStateEvent(e.getChannel(), e.getFuture(), e.getState(), connectAddress);
            }
        }

        super.connectRequested(ctx, forwardedEvent);
    }
}
