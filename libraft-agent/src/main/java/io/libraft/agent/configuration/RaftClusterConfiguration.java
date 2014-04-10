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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Objects;
import io.libraft.agent.RaftMember;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import java.util.Set;

// FIXME (AG): cluster configuration should be stored in the database
/**
 * Represents the configuration of a Raft cluster. Contains the following properties/blocks:
 * <ul>
 *     <li>self (unique id of the local Raft server).</li>
 *     <li>members (configuration block for each Raft server in the Raft cluster).
 *         {@code members} <strong>must</strong> contain a configuration block for the local
 *         Raft server as well.</li>
 * </ul>
 * See the project README.md for more on the configuration.
 */
public final class RaftClusterConfiguration {

    private static final String SELF = "self";
    private static final String MEMBERS = "members";

    @NotEmpty
    @JsonProperty(SELF)
    private final String self;

    @JsonSerialize(contentUsing = RaftMemberConverter.Serializer.class)
    @JsonDeserialize(contentUsing = RaftMemberConverter.Deserializer.class)
    @NotEmpty
    @Valid
    @JsonProperty(MEMBERS)
    private final Set<RaftMember> members;

    /**
     * Constructor.
     *
     * @param self unique, non-null, non-empty id of the local Raft server
     * @param members set of valid {@link io.libraft.agent.RaftMember} instances - one for each server in the Raft cluster.
     *                {@code members} <strong>must</strong> include a {@code RaftMember} instance for {@code self}
     */
    @JsonCreator
    public RaftClusterConfiguration(@JsonProperty(SELF) String self, @JsonProperty(MEMBERS) Set<RaftMember> members) {
        this.self = self;
        this.members = members;
    }

    /**
     * Return the unique id of the local Raft server.
     *
     * @return unique, non-null, non-empty id of the local Raft server
     */
    public String getSelf() {
        return self;
    }

    /**
     * Returns information about all the servers in the Raft cluster.
     *
     * @return set of {@link io.libraft.agent.RaftMember} instances - one for each server in the cluster.
     *         This <strong>includes</strong> an {@code RaftMember} instance for {@code self}
     */
    public Set<RaftMember> getMembers() {
        return members;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RaftClusterConfiguration other = (RaftClusterConfiguration) o;
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
