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
import javax.validation.constraints.Min;

/**
 * Raft AppendEntriesReply message.
 */
public final class AppendEntriesReply extends RaftRPC {

    private static final String PREV_LOG_INDEX = "prevLogIndex";
    private static final String ENTRY_COUNT = "entryCount";
    private static final String APPLIED = "applied";

    @Min(value = 0)
    @JsonProperty(PREV_LOG_INDEX)
    private final long prevLogIndex;

    @Min(value = 0)
    @JsonProperty(ENTRY_COUNT)
    private final long entryCount;

    @Valid
    @JsonProperty(APPLIED)
    private final boolean applied;

    /**
     * Constructor.
     *
     * @param source unique id of the Raft server that generated the message
     * @param destination unique id of the Raft server that is the intended recipient
     * @param term election term in which the message was generated
     * @param prevLogIndex index of the {@link io.libraft.algorithm.LogEntry} after
     *                     which log entries were meant to be appended
     * @param entryCount number of log entries meant to be appended to the {@code source} server's log.
     *                   0, if this is a response to a heartbeat
     * @param applied true, if the {@code LogEntry} at {@code prevLogIndex} for both the
     *                      {@code source} and {@code destination} servers matched and the
     *                      entries were appended. false otherwise
     */
    @JsonCreator
    public AppendEntriesReply(@JsonProperty(SOURCE) String source,
                              @JsonProperty(DESTINATION) String destination,
                              @JsonProperty(TERM) long term,
                              @JsonProperty(PREV_LOG_INDEX) long prevLogIndex,
                              @JsonProperty(ENTRY_COUNT) long entryCount,
                              @JsonProperty(APPLIED) boolean applied) {
        super(source, destination, term);
        this.prevLogIndex = prevLogIndex;
        this.entryCount = entryCount;
        this.applied = applied;
    }

    /**
     * Get the log index after which the entries were meant to be appended.
     *
     * @return index >= 0 of the {@link io.libraft.algorithm.LogEntry} after which log entries were meant to be appended
     */
    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    /**
     * Get the number of entries meant to be appended.
     *
     * @return number of log entries meant to be added to the {@code source} server's log.
     *         0, if this is a response to a heartbeat
     */
    public long getEntryCount() {
        return entryCount;
    }

    /**
     * Get whether the entries were appended to the {@code source} server's log.
     *
     * @return true, if the {@link io.libraft.algorithm.LogEntry} at
     *                      {@link AppendEntriesReply#getPrevLogIndex()} for both the
     *                      {@code source} and {@code destination} servers matched and the
     *                      entries were appended. false otherwise
     */
    public boolean isApplied() {
        return applied;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getSource(), getDestination(), getTerm(), prevLogIndex, entryCount, applied);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof AppendEntriesReply)) {
            return false;
        }
        if (o == this) {
            return true;
        }

        AppendEntriesReply other = (AppendEntriesReply) o;
        return getSource().equalsIgnoreCase(other.getSource())
                && getDestination().equalsIgnoreCase(other.getDestination())
                && getTerm() == other.getTerm()
                && prevLogIndex == other.prevLogIndex
                && entryCount == other.entryCount
                && applied == other.applied;
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add(SOURCE, getSource())
                .add(DESTINATION, getDestination())
                .add(TERM, getTerm())
                .add(PREV_LOG_INDEX, prevLogIndex)
                .add(ENTRY_COUNT, entryCount)
                .add(APPLIED, applied)
                .toString();
    }
}
