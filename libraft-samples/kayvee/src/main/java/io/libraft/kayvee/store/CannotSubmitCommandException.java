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

package io.libraft.kayvee.store;

import io.libraft.NotLeaderException;
import io.libraft.kayvee.configuration.ClusterMember;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * Thrown when a {@link KayVeeCommand} cannot
 * be completed because of a {@link NotLeaderException}.
 * <p/>
 * An instance of {@code CannotSubmitCommandException} <strong>must</strong>
 * be thrown whenever KayVee code catches an instance of {@code NotLeaderException}.
 * The original {@code NotLeaderException} <strong>must not</strong> be propagated.
 */
public final class CannotSubmitCommandException extends KayVeeException {

    private static final long serialVersionUID = -1841537123003976950L;

    private final String self;
    private final @Nullable String leader;
    private final Set<ClusterMember> members;

    /**
     * Constructor.
     *
     * @param cause instance of {@code NotLeaderException} that was thrown
     *              when a command was submitted to the Raft cluster
     * @param members unique ids of all the servers (including the local server) that comprise the Raft cluster
     */
    public CannotSubmitCommandException(NotLeaderException cause, Set<ClusterMember> members) {
        super(cause.getMessage());

        this.self = cause.getSelf();
        this.leader = cause.getLeader();
        this.members = members;
    }

    /**
     * Get the unique id of the local KayVee server.
     *
     * @return unique id of the local KayVee server
     */
    public String getSelf() {
        return self;
    }

    /**
     * Get the unique id of the server that is the leader of the Raft cluster.
     *
     * @return unique id of the server that is the leader of the Raft cluster, or null if the leader is not known
     */
    public @Nullable String getLeader() {
        return leader;
    }

    /**
     * Get the unique ids of all the servers that comprise the Raft cluster.
     *
     * @return unique ids of all the servers (including the local server) that comprise the Raft cluster
     */
    public Set<ClusterMember> getMembers() {
        return members;
    }
}
