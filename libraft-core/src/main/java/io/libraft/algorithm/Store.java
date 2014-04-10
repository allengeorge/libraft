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

package io.libraft.algorithm;

import javax.annotation.Nullable;

/**
 * Metadata storage service that stores metadata required by
 * a Raft server to participate in a Raft cluster.
 * <p/>
 * Implementations only have to guarantee that <strong>individual</strong>
 * operations are atomic. They <strong>do not</strong> have
 * to validate the data being stored or returned. All implementation-specific
 * checked exceptions <strong>must</strong>
 * be wrapped in a {@link StorageException} and rethrown.
 */
public interface Store {

    /**
     * Get the current election term for the local Raft server.
     *
     * @return election term >= 0 for the local Raft server
     */
    long getCurrentTerm() throws StorageException;

    /**
     * Set the current election term for the local Raft server.
     *
     * @param currentTerm election term >= 0 of the local Raft server
     */
    void setCurrentTerm(long currentTerm) throws StorageException;

    /**
     * Get the index of the last committed {@link LogEntry} in the local Raft server's Raft log.
     *
     * @return index >=0 of the last committed {@code LogEntry} in the local Raft server's Raft log
     */
    long getCommitIndex() throws StorageException;

    /**
     * Set the index of the last committed {@link LogEntry} in the local Raft server's Raft log.
     *
     * @param commitIndex index >= 0 of the last committed {@code LogEntry} in the local Raft server's Raft log
     */
    void setCommitIndex(long commitIndex) throws StorageException;

    /**
     * Get the unique id of the Raft server for whom the local Raft server voted in election term {@code term}.
     *
     * @param term election term >= 0 for which the vote record should be retrieved
     * @return unique id of the Raft server for whom the local Raft server voted in election term {@code term}.
     *         This may be the id of the local Raft server itself (if it were a candidate and voted for itself),
     *         or {@code null} if it did not vote. Implementations <strong>may</strong> choose to
     *         return {@code null} <strong>or</strong> throw a {@link java.lang.RuntimeException} if there
     *         is no vote record for {@code term}
     */
    @Nullable String getVotedFor(long term) throws StorageException;

    /**
     * Add or update a vote history record. The record has the form:
     * {@code term} (election term) => {@code server} (server voted for).
     *
     * @param term election term >=0 for which the vote history should be set
     * @param server unique id of the Raft server for whom the local Raft server
     *                 voted in election term {@code term}. This may be the id of the local
     *                 Raft server itself (if it were a candidate and voted for itself),
     *                 or {@code null} if it did not vote
     */
    void setVotedFor(long term, @Nullable String server) throws StorageException;

    /**
     * Remove <strong>all</strong> (election term, server voted for)
     * pairs from durable storage.
     * <p/>
     * This method <strong>should not</strong> be used after {@link RaftAlgorithm}
     * is first initialized: it will wipe out all metadata and cause this Raft server
     * to be in an inconsistent and <strong>dangerous</strong> state.
     */
    void clearVotedFor() throws StorageException;
}
