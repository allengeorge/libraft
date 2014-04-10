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

package io.libraft.kayvee.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.hibernate.validator.constraints.NotEmpty;

import javax.annotation.Nullable;

/**
 * Java representation of a JSON {@code key=>value} pair defined in the KayVee REST API.
 */
public final class KeyValue {

    private static final String KEY = "key";
    private static final String VALUE = "value";

    @NotEmpty
    @JsonProperty(KEY)
    private final String key;

    @NotEmpty
    @JsonProperty(VALUE)
    private final String value;

    /**
     * Constructor.
     *
     * @param key non-null, non-empty key
     * @param value non-null, non-empty value associated with this key
     */
    @JsonCreator
    public KeyValue(@JsonProperty(KEY) String key, @JsonProperty(VALUE) String value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Get the key for this {@code key=>value} pair.
     *
     * @return non-null, non-empty key
     */
    public String getKey() {
        return key;
    }

    /**
     * Get the value for this {@code key=>value} pair.
     *
     * @return non-null, non-empty value
     */
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyValue other = (KeyValue) o;

        return key.equals(other.key) && value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key, value);
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add(KEY, key)
                .add(VALUE, value)
                .toString();
    }
}
