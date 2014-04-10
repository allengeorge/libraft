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

import java.util.concurrent.TimeUnit;

/**
 * Default values for parameters used by libraft-agent classes.
 */
public abstract class RaftAgentConstants {

    private RaftAgentConstants() { } // to protect instantiation

    /**
     * Default time unit in which all timeouts and intervals are specified.
     */
    public static final TimeUnit DEFAULT_AGENT_TIME_UNIT = TimeUnit.MILLISECONDS;

    /**
     * Default value for the <strong>minimum</strong> interval between
     * connection retries to a Raft server.
     *
     * @see io.libraft.agent.rpc.RaftNetworkClient
     */
    public static final int MIN_RECONNECT_INTERVAL = 5000;

    /**
     * Default value for the <strong>maximum additional</strong> time added to
     * {@link RaftAgentConstants#MIN_RECONNECT_INTERVAL}.
     *
     * @see io.libraft.agent.rpc.RaftNetworkClient
     */
    public static final int ADDITIONAL_RECONNECT_INTERVAL_RANGE = 1000;

    /**
     * Default value for the <strong>maximum</strong> time to wait to establish
     * a successful connection to a Raft server.
     */
    public static final int CONNECT_TIMEOUT = 5000;
}
