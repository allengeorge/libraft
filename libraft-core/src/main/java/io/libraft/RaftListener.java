/*
 * Copyright (c) 2013, Allen A. George <allen dot george at gmail dot com>
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
 * Implemented by up-call code that wants to be notified of
 * important events in the Raft cluster. Listeners are notified when:
 * <ul>
 *     <li>The leader of the Raft cluster changes
 *         (i.e. the current leader loses leadership, or a new leader is chosen).</li>
 *     <li>A command is committed to the Raft cluster.</li>
 * </ul>
 */
public interface RaftListener {

    /**
     * Indicates that a leadership change has occurred.
     *
     * @param leader unique id of the leader server. The client can use
     *               {@link Raft#submitCommand(Command)} to submit a {@link Command} only
     *               if {@code leader} is the unique id of the local
     *               Raft server. If {@code leader} is {@code null } this means that the cluster
     *               is experiencing interregnum or the local node does not know who
     *               the current leader is.
     */
    void onLeadershipChange(@Nullable String leader);

    /**
     * Indicates that {@code command} was committed to the Raft cluster.
     *
     * @param index unique, positive log index of {@code command}
     * @param command {@code Command} instance that was committed to the Raft cluster
     */
    void applyCommand(long index, Command command);
}
