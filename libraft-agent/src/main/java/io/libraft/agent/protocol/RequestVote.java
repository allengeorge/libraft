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

import javax.annotation.Nullable;
import javax.validation.constraints.Min;

/**
 * Raft RequestVote message.
 */
public final class RequestVote extends RaftRPC {

    private static final String LAST_LOG_TERM = "lastLogTerm";
    private static final String LAST_LOG_INDEX = "lastLogIndex";

    @Min(value = 0)
    @JsonProperty(LAST_LOG_TERM)
    private final long lastLogTerm;

    @Min(value = 0)
    @JsonProperty(LAST_LOG_INDEX)
    private final long lastLogIndex;

    /**
     * Constructor.
     *
     * @param source unique id of the Raft server that generated the message
     * @param destination unique id of the Raft server that is the intended recipient
     * @param term election term in which the message was generated
     * @param lastLogTerm election term in which the {@code LogEntry} at {@code lastLogIndex} was created
     *
     * @param lastLogIndex index of the last {@code LogEntry} in the {@code source} server's Raft log
     * @see io.libraft.algorithm.LogEntry
     */
    @JsonCreator
    public RequestVote(@JsonProperty(SOURCE) String source,
                       @JsonProperty(DESTINATION) String destination,
                       @JsonProperty(TERM) long term,
                       @JsonProperty(LAST_LOG_TERM) long lastLogTerm,
                       @JsonProperty(LAST_LOG_INDEX) long lastLogIndex) {
        super(source, destination, term);
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
    }

    /**
     * Get the last log term.
     *
     * @return election term in which the {@link io.libraft.algorithm.LogEntry}
     *         at {@link RequestVote#getLastLogIndex()} was created
     */
    public long getLastLogTerm() {
        return lastLogTerm;
    }

    /**
     * Get the last log index.
     *
     * @return index of the last {@link io.libraft.algorithm.LogEntry}
     *         in the {@code source} server's Raft log
     */
    public long getLastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getSource(), getDestination(), getTerm(), lastLogTerm, lastLogIndex);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (o == null || !(o instanceof RequestVote)) {
            return false;
        }
        if (o == this) {
            return true;
        }

        RequestVote other = (RequestVote) o;
        return getSource().equalsIgnoreCase(other.getSource())
                && getDestination().equalsIgnoreCase(other.getDestination())
                && getTerm() == other.getTerm()
                && lastLogTerm == other.lastLogTerm
                && lastLogIndex == other.lastLogIndex;
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add(SOURCE, getSource())
                .add(DESTINATION, getDestination())
                .add(TERM, getTerm())
                .add(LAST_LOG_TERM, lastLogTerm)
                .add(LAST_LOG_INDEX, lastLogIndex)
                .toString();
    }
}
