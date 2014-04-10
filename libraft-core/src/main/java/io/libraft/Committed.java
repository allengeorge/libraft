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

/**
 * Represents state committed to the Raft cluster.
 * <p/>
 * A {@code Committed} can be <strong>one of</strong> these three types:
 * <ul>
 *     <li>{@code SKIP}: noop entry in the Raft log.</li>
 *     <li>{@code COMMAND}: client-submitted command in the Raft log.</li>
 *     <li>{@code SNAPSHOT}: snapshot representing the aggregate committed state
 *         of the Raft log up to a certain log index.</li>
 * </ul>
 * Implementations provide additional methods by which users can access type-specific state.
 * <p/>
 * Clients <strong>must</strong> store the log index returned
 * by {@link Committed#getIndex()} associated with all {@code Committed} instances.
 *
 */
public interface Committed {

    /**
     * The type of this committed state.
     */
    enum Type {

        /**
         * A noop entry in the Raft log.
         * <p/>
         * {@code Committed} instances of this type simply notify
         * users of {@link Raft} and {@link RaftListener} that the distributed
         * consensus algorithm has made progress.
         */
        SKIP,

        /**
         * A client-submitted command in the Raft log.
         * <p/>
         * Indicates that a {@link Command} submitted by the client via
         * {@link Raft#submitCommand(Command)} was successfully replicated to
         * the Raft cluster. At this point the client can safely apply the
         * command and transform its local state.
         */
        COMMAND,

        /**
         * A snapshot representing the aggregate committed state
         * of the Raft log up to a certain log index.
         * <p/>
         * When receiving a {@code Committed} instance of this type
         * clients <strong>should</strong> flush their local state and
         * replace it with the contents of the snapshot.
         */
        SNAPSHOT,
    }

    /**
     * Get the {@code Type} of this committed state.
     *
     * @return the type of this committed state
     */
    Type getType();

    /**
     * Get the Raft log index (inclusive) up to which the data is relevant.
     *
     * @return index >=0 of this committed state in the Raft log
     */
    long getIndex();
}
