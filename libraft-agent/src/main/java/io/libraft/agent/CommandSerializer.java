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

package io.libraft.agent;

import io.libraft.Command;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Implemented by a class that converts a {@link Command}
 * instance into its corresponding binary representation.
 * <p/>
 * Implementations <strong>must</strong> be thread-safe. Ideally, calls
 * to {@link CommandSerializer#serialize(Command, OutputStream)} would not modify
 * any internal state.
 */
public interface CommandSerializer {

    /**
     * Serialize the {@code Command} into binary to the given {@code OutputStream}.
     * <p/>
     * Implementations can assume that {@code out} contains
     * enough space to hold the <strong>fully-serialized</strong> {@code command}.
     *
     * @param command {@code Command} instance to be serialized to binary
     * @param out {@code OutputStream} the {@code Command} should be serialized to
     * @throws IOException if there was an error serializing {@code command} to {@code out}
     */
    void serialize(Command command, OutputStream out) throws IOException;
}
