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
 * {@link LogEntry} storage service.
 * <p/>
 * Stores a sequence of {@code LogEntry} instances
 * indexed by a {@code long >= 0} referred to as a "log index".
 * Any two consecutive {@code LogEntry} instances satisfy
 * the log property LP: {@code logIndex(LogEntry0) + 1 = logIndex(LogEntry1)}.
 * Implementations need only guarantee that each
 * <strong>individual</strong> operation is atomic.
 * They <strong>must</strong> wrap all
 * implementation-specific checked exceptions in a {@link StorageException}
 * and rethrow them.
 */
public interface Log {

    /**
     * Get the {@code LogEntry} at {@code index}.
     *
     * @param index index >= 0 of the {@code LogEntry} to get
     * @return instance of {@code LogEntry} if it exists or {@code null} otherwise
     */
    @Nullable LogEntry get(long index) throws StorageException;

    /**
     * Get the first {@code LogEntry} in the log.
     *
     * @return first {@code LogEntry} in the log if the log contains <strong>any</strong> entries or {@code null} otherwise
     */
    @Nullable LogEntry getFirst() throws StorageException;

    /**
     * Get the last {@code LogEntry} in the log.
     *
     * @return last {@code LogEntry} in the log if the log contains <strong>any</strong> entries or {@code null} otherwise
     */
    @Nullable LogEntry getLast() throws StorageException;

    /**
     * Put the {@code LogEntry} at the index specified by {@link LogEntry#getIndex()}.
     * <p/>
     * {@code logEntry} may overwrite another instance
     * previously at that index. The implementation does <strong>not</strong> have to prevent this.
     * It also does <strong>not</strong> have to verify that LP is maintained.
     *
     * @param logEntry instance of {@code LogEntry} to insert at {@link LogEntry#getIndex()}
     */
    void put(LogEntry logEntry) throws StorageException;

    /**
     * Remove all {@code LogEntry} instances at log indices >= {@code index},
     * where index >=0. This operation is a noop if {@code index} is beyond
     * the end of the log. At the end of this operation the log contains
     * only entries {@code 0...(index - 1)}.
     *
     * @param index index >=0 from which to remove {@code LogEntry} instances
     */
    void truncate(long index) throws StorageException;

}
