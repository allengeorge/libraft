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

package io.libraft.kayvee.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.hibernate.validator.constraints.NotEmpty;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * Defines a KayVee/Raft cluster. Contains the following blocks and properties:
 * <ul>
 *     <li>self (unique id of the local Raft server).</li>
 *     <li>members (configuration block for each Raft server in the Raft cluster).
 *         {@code members} <strong>must</strong> contain a configuration block for the local
 *         Raft server as well.</li>
 * </ul>
 */
public final class ClusterConfiguration {

    private static final String SELF = "self";
    private static final String MEMBERS = "members";

    @NotEmpty
    private final String self;

    @NotEmpty
    private final Set<ClusterMember> members;

    /**
     * Constructor.
     *
     * @param self non-null, non-empty id of the local Raft server
     * @param members set of configuration blocks - one for each
     *                member of the Raft cluster to which this server belongs
     */
    @JsonCreator
    public ClusterConfiguration(@JsonProperty(SELF) String self, @JsonProperty(MEMBERS) Set<ClusterMember> members) {
        this.self = self;
        this.members = members;
    }

    /**
     * Get the id of the local Raft server.
     *
     * @return non-null, non-empty id of the local Raft server
     */
    public String getSelf() {
        return self;
    }

    /**
     * Get the configuration blocks for the members
     * that belong to the Raft cluster.
     *
     * @return set of configuration blocks - one for
     * each member of the Raft cluster to which this server belongs
     */
    public Set<ClusterMember> getMembers() {
        return members;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterConfiguration other = (ClusterConfiguration) o;

        return self.equals(other.self) && members.equals(other.members);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(self, members);
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add(SELF, self)
                .add(MEMBERS, members)
                .toString();
    }
}
