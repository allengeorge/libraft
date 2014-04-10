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

package io.libraft.agent.configuration;

import com.google.common.net.HostAndPort;

import static com.google.common.base.Preconditions.checkState;

/**
 * Provides static utility methods for converting string endpoints
 * (i.e. addresses of the form <em>"host:port"</em>) into a valid
 * {@link HostAndPort} instance that can be used
 * in a {@link io.libraft.agent.RaftMember}.
 */
public abstract class Endpoints {

    private Endpoints() { } // private to prevent instantiation

    /**
     * Convert an address string of the form <em>"host:port"</em> into a {@link HostAndPort} instance.
     *
     * @param endpoint non-empty address string
     * @return a valid {@code HostAndPort} instance. Callers can use
     *         {@link com.google.common.net.HostAndPort#getHostText()} and
     *         {@link com.google.common.net.HostAndPort#getPort()} to get the
     *         host and port components of {@code endpoint}
     *
     * @throws IllegalStateException if {@code endpoint} cannot be parsed
     */
    public static HostAndPort getValidHostAndPortFromString(String endpoint) {
        HostAndPort hostAndPort = HostAndPort.fromString(endpoint);

        String host = hostAndPort.getHostText();
        checkState(host != null && !host.isEmpty(), "invalid host");

        int Port = hostAndPort.getPort();
        checkState(Port > 0, "invalid port");

        return hostAndPort;
    }
}