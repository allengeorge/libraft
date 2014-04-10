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

package io.libraft.kayvee.store;

import javax.annotation.Nullable;

/**
 * Thrown when a key exists in the key-value store
 * but its value does not match the excepted value
 * in a {@code CAS} operation.
 * <p/>
 * This exception <strong>should not</strong> be thrown
 * if the key does not exist - use {@link KeyNotFoundException}
 * instead.
 */
public final class ValueMismatchException extends KayVeeException {

    private static final long serialVersionUID = -8419273931405391504L;

    private final String key;

    @Nullable private final String expectedValue;
    @Nullable private final String existingValue;

    /**
     * Constructor.
     *
     * @param key non-null, non-empty key for which the CAS operation failed
     * @param expectedValue expected value (may be null) the caller specified for the CAS operation on the key
     * @param existingValue current value for the key, or null if the key does not exist
     */
    public ValueMismatchException(String key, @Nullable String expectedValue, @Nullable String existingValue) {
        super(String.format("key:%s - mismatched value: expected:%s existing:%s", key, expectedValue, existingValue));
        this.key = key;
        this.expectedValue = expectedValue;
        this.existingValue = existingValue;
    }

    /**
     * Get the key for which the CAS operation failed.
     *
     * @return non-null, non-empty key for which the CAS operation failed
     */
    public String getKey() {
        return key;
    }

    /**
     * Get the expected value the caller specified for the CAS operation on the key.
     *
     * @return expected value (may be null) the caller specified for the CAS operation on the key
     */
    public @Nullable String getExpectedValue() {
        return expectedValue;
    }

    /**
     * Get the current value for the key.
     *
     * @return current value for the key, or null if the key does not exist
     */
    public @Nullable String getExistingValue() {
        return existingValue;
    }
}
