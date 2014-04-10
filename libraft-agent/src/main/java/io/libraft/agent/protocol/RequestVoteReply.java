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

package io.libraft.agent.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.validation.Valid;

/**
 * Raft RequestVoteReply message.
 */
public final class RequestVoteReply extends RaftRPC {

    private static final String VOTE_GRANTED = "voteGranted";

    @Valid
    @JsonProperty(VOTE_GRANTED)
    private final boolean voteGranted;

    /**
     * Constructor.
     *
     * @param source unique id of the Raft server that generated the message
     * @param destination unique id of the Raft server that is the intended recipient
     * @param term election term in which the message was generated
     * @param voteGranted whether the {@code source} granted {@code destination} a vote
     */
    @JsonCreator
    public RequestVoteReply(@JsonProperty(SOURCE) String source,
                            @JsonProperty(DESTINATION) String destination,
                            @JsonProperty(TERM) long term,
                            @JsonProperty(VOTE_GRANTED) boolean voteGranted) {
        super(source, destination, term);
        this.voteGranted = voteGranted;
    }

    /**
     * Get whether the vote was granted.
     *
     * @return true if {@code source} granted {@code destination} its vote, false otherwise
     */
    public boolean isVoteGranted() {
        return voteGranted;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getSource(), getDestination(), getTerm(), voteGranted);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof RequestVoteReply)) {
            return false;
        }
        if (o == this) {
            return true;
        }

        RequestVoteReply other = (RequestVoteReply) o;
        return getSource().equalsIgnoreCase(other.getSource())
                && getDestination().equals(other.getDestination())
                && getTerm() == other.getTerm()
                && voteGranted == other.voteGranted;
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add(SOURCE, getSource())
                .add(DESTINATION, getDestination())
                .add(TERM, getTerm())
                .add(VOTE_GRANTED, voteGranted)
                .toString();
    }
}
