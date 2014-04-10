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

import java.io.IOException;
import java.io.OutputStream;

/**
 * Object with which a client can write a snapshot
 * of its local state. The snapshot can be written in any format.
 */
public interface SnapshotWriter {

    /**
     * Get the log index of the last log entry the client applied to its local state.
     *
     * @return log index >= 0 of the last log entry the client applied to its local state
     */
    long getIndex();

    /**
     * Set the index of the last log entry the client applied to its local state.
     * This value <strong>must</strong> be written for the snapshot to
     * be persisted to durable storage.
     *
     * @param index log index >= 0 of the last log entry the client applied to its local state
     */
    void setIndex(long index);

    /**
     * Get the {@code OutputStream} to which the snapshot should be written.
     * Multiple calls to {@code getSnapshotOutputStream} <strong>will</strong>
     * return the <strong>same</strong> stream instance.
     *
     * @return an instance of {@code OutputStream} to which the snapshot is written
     * @throws IOException if the {@code OutputStream} cannot be constructed
     */
    OutputStream getSnapshotOutputStream() throws IOException;
}
