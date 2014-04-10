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

package io.libraft;

import javax.annotation.Nullable;

/**
 * Thrown when a {@link Command} is submitted to a {@link Raft} instance
 * and it is not the leader.
 */
@SuppressWarnings("unused")
public final class NotLeaderException extends RaftException {

    private static final long serialVersionUID = 6131822943716006815L;

    private String self;
    private @Nullable String leader;

    private NotLeaderException() { }

    private NotLeaderException(String message) {
        super(message);
    }

    private NotLeaderException(Throwable cause) {
        super(cause);
    }

    private NotLeaderException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor
     *
     * @param self unique id of the server
     * @param leader unique id of the current leader server (if known)
     */
    public NotLeaderException(String self, @Nullable String leader) {
        super(leader != null
                ?
                String.format("%s not leader - current leader: %s", self, leader)
                :
                String.format("%s not leader - cluster has no leader", self)
        );
        this.self = self;
        this.leader = leader;
    }

    /**
     * @return the unique id of the server that threw the exception (i.e. the follower server)
     */
    public String getSelf() {
        return self;
    }

    /**
     * @return the unique id of the current leader server, or {@code null} if the current leader is unknown
     */
    public @Nullable String getLeader() {
        return leader;
    }
}
