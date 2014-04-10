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

import com.google.common.util.concurrent.ListenableFuture;
import io.libraft.algorithm.StorageException;

import javax.annotation.Nullable;

/**
 * Interface through which a client interacts with the Raft cluster.
 */
public interface Raft {

    /**
     * Indicate that a snapshot was written using a given {@code snapshotWriter}.
     *
     * @param snapshotWriter an instance of {@code SnapshotWriter} with which the snapshot was created
     */
    void snapshotWritten(SnapshotWriter snapshotWriter) throws StorageException;

    /**
     * Get the <strong>next</strong> committed object (either a
     * {@link Snapshot} or a {@link CommittedCommand}) with a log index
     * <strong>after</strong> {@code indexToSearchFrom}.
     *
     * @param indexToSearchFrom index >= 0 after which to search for committed objects
     *                          in the <strong>local</strong> server's snapshot store or Raft log.
     *                          {@code indexToSearch} should meet the following criteria:
     *                          {@code 0 <= indexToSearch <= lastLogIndex}
     * @return an instance of {@link Committed} if the <strong>local</strong> server's
     *         state contains a committed object <strong>after</strong> {@code indexToSearchFrom}.
     *         {@code null} otherwise
     */
    @Nullable Committed getNextCommitted(long indexToSearchFrom) throws StorageException;

    /**
     * Submit an object to be replicated to the cluster.
     *
     * @param command {@code Command} instance to be replicated
     * @return a future that will be triggered when {@code command} is replicated
     *         or when the {@code command} <strong>may not</strong> have been replicated
     *         <p/>
     *         If this future is successful, the command is <strong>guaranteed</strong>
     *         to have been replicated to the Raft cluster. There are <strong>no</strong>
     *         false positives.
     *         <p/>
     *         This future may fail if the system <strong>does not know</strong>
     *         if {@code command} was durably replicated or not. There <strong>may</strong> be false
     *         negatives.
     *         <p/>
     *         Due to message losses this future may not
     *         be triggered in a timely manner. Clients should:
     *         <ul>
     *             <li>Not perform indefinite blocking waits on this future.</li>
     *             <li>Be prepared to resubmit the command in case of timeouts.</li>
     *             <li>Ensure that up-call code can detect and discard duplicate commands.</li>
     *         </ul>
     * @throws NotLeaderException if the current Raft server is not the leader. The client can call
     *                            {@link NotLeaderException#getLeader()} to find out which Raft server (if any)
     *                            is the current leader
     */
    ListenableFuture<Void> submitCommand(Command command) throws NotLeaderException;
}
