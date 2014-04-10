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
import java.io.InputStream;

/**
 * Implemented by a class that converts the binary representation
 * of a {@link Command} into its corresponding Java object.
 * <p/>
 * Implementations <strong>must</strong> be thread-safe. Ideally, calls
 * to {@link CommandDeserializer#deserialize(java.io.InputStream)} would not
 * modify any internal state.
 */
public interface CommandDeserializer {

    /**
     * Deserialize the binary representation of a {@code Command} from
     * an {@code InputStream} into its corresponding Java object.
     * <p/>
     * Implementations can assume that the <strong>complete</strong>
     * binary representation of {@code Command} can be read from {@code in}
     * without blocking.
     *
     * @param in {@code InputStream} from which to read the serialized {@code Command}
     * @return a valid {@code Command} instance
     * @throws IOException if there was an error reading from {@code in}
     *         or deserializing the binary stream into a {@code Command} instance
     */
    Command deserialize(InputStream in) throws IOException;
}
