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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Objects;
import io.libraft.algorithm.LogEntry;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import java.util.Collection;

/**
 * Raft AppendEntries message.
 */
public final class AppendEntries extends RaftRPC {

    private static final String COMMIT_INDEX = "commitIndex";
    private static final String PREV_LOG_INDEX = "prevLogIndex";
    private static final String PREV_LOG_TERM = "prevLogTerm";
    private static final String ENTRIES = "entries";

    @Min(value = 0)
    @JsonProperty(COMMIT_INDEX)
    private final long commitIndex;

    @Min(value = 0)
    @JsonProperty(PREV_LOG_INDEX)
    private final long prevLogIndex;

    @Min(value = 0)
    @JsonProperty(PREV_LOG_TERM)
    private final long prevLogTerm;

    @SuppressWarnings("NullableProblems")
    @Valid
    @JsonSerialize(contentUsing = RaftRPCLogEntry.Serializer.class)
    @JsonDeserialize(contentUsing = RaftRPCLogEntry.Deserializer.class)
    @JsonProperty(ENTRIES)
    private final @Nullable Collection<LogEntry> entries;

    /**
     * Constructor.
     *
     * @param source unique id of the Raft server that generated the message
     * @param destination unique id of the Raft server that is the intended recipient
     * @param term election term in which the message was generated
     * @param commitIndex index of the last {@code LogEntry} that the {@code source} server believes is committed
     * @param prevLogIndex index of the {@code LogEntry} immediately before the first {@code LogEntry} in {@code entries}
     * @param prevLogTerm election term in which the {@code LogEntry} at {@code prevLogIndex} was created
     * @param entries sequence of monotonic, gapless, {@code LogEntry} instances with
     *                indices {@code prevLogIndex + 1}, {@code prevLogIndex + 2} .. {@code prevLogIndex + entries.count()}.
     *                {@code entries} may be null if this AppendEntries message is a heartbeat
     *
     * @see LogEntry
     */
    @JsonCreator
    public AppendEntries(@JsonProperty(SOURCE) String source,
                         @JsonProperty(DESTINATION) String destination,
                         @JsonProperty(TERM) long term,
                         @JsonProperty(COMMIT_INDEX) long commitIndex,
                         @JsonProperty(PREV_LOG_INDEX) long prevLogIndex,
                         @JsonProperty(PREV_LOG_TERM) long prevLogTerm,
                         @JsonProperty(ENTRIES) @Nullable Collection<LogEntry> entries) {
        super(source, destination, term);
        this.commitIndex = commitIndex;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
    }

    /**
     * Get the commit index.
     *
     * @return index >= 0 of the last {@link LogEntry} the {@code source} server believes is committed
     */
    public long getCommitIndex() {
        return commitIndex;
    }

    /**
     * Get the log index after which entries are meant to be appended.
     *
     * @return index >=0 of the {@link LogEntry} immediately before the first entry in {@link AppendEntries#getEntries()}
     */
    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    /**
     * Get the election term in which the {@code LogEntry} at {@code prevLogIndex} was created.
     *
     * @return election term >= 0 in which the {@link LogEntry} at {@link AppendEntries#getPrevLogIndex()} was created
     */
    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    /**
     * Get the sequence of {@code LogEntry} instances to be appended to the {@code destination} server's log.
     *
     * @return monotonically increasing, gapless set of {@link LogEntry} instances
     *         beginning at {@link AppendEntries#getPrevLogIndex()}. {@code null}
     *         if this AppendEntries message is a heartbeat.
     */
    public @Nullable Collection<LogEntry> getEntries() {
        return entries;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getSource(), getDestination(), getTerm(), commitIndex, prevLogIndex, prevLogTerm, entries);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof AppendEntries)) {
            return false;
        }
        if (o == this) {
            return true;
        }

        AppendEntries other = (AppendEntries) o;
        return getSource().equalsIgnoreCase(other.getSource())
                && getDestination().equalsIgnoreCase(other.getDestination())
                && getTerm() == other.getTerm()
                && commitIndex == other.commitIndex
                && prevLogIndex == other.prevLogIndex
                && prevLogTerm == other.prevLogTerm
                && (entries != null ? entries.equals(other.entries) : other.entries == null);
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add(SOURCE, getSource())
                .add(DESTINATION, getDestination())
                .add(TERM, getTerm())
                .add(COMMIT_INDEX, commitIndex)
                .add(PREV_LOG_INDEX, prevLogIndex)
                .add(PREV_LOG_TERM, prevLogTerm)
                .add(ENTRIES, entries)
                .toString();
    }
}
