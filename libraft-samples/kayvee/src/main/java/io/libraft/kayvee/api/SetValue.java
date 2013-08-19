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

package io.libraft.kayvee.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.Nullable;

/**
 * Defines the value for a key.
 * <p/>
 * See the KayVee README.md for more on where this object is used and its valid values.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public final class SetValue {

    // it is highly, highly unlikely that someone will create a key with this value
    // if they do...then I'll figure out alternatives to this approach
    private static final String DEFAULT_VALUE_TO_AVOID_SERIALIZATION = "io.libraft.kayvee.api.SetValue.DEFAULT_VALUE_TO_AVOID_SERIALIZATION.SetValue.api.kayvee.libraft.io";

    private static final String EXPECTED_VALUE = "expectedValue";
    private static final String NEW_VALUE = "newValue";

    @JsonIgnore
    private boolean hasExpectedValue = false;

    @Nullable
    @JsonProperty(EXPECTED_VALUE)
    private String expectedValue = DEFAULT_VALUE_TO_AVOID_SERIALIZATION;

    @JsonIgnore
    private boolean hasNewValue = false;

    @Nullable
    @JsonProperty(NEW_VALUE)
    private String newValue = DEFAULT_VALUE_TO_AVOID_SERIALIZATION;

    @JsonIgnore
    public boolean hasExpectedValue() {
        return hasExpectedValue;
    }

    @JsonIgnore // have to do this, otherwise Jackson uses the value returned by this getter instead of the property (TODO (AG): Figure out why!)
    public @Nullable String getExpectedValue() {
        return hasExpectedValue ? expectedValue : null;
    }

    public void setExpectedValue(@Nullable String expectedValue) {
        this.expectedValue = expectedValue;
        this.hasExpectedValue = true;
    }

    @JsonIgnore
    public boolean hasNewValue() {
        return hasNewValue;
    }

    @JsonIgnore
    public @Nullable String getNewValue() {
        return hasNewValue ? newValue : null;
    }

    public void setNewValue(@Nullable String newValue) {
        this.newValue = newValue;
        this.hasNewValue = true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SetValue other = (SetValue) o;

        return (expectedValue == null ? other.expectedValue == null : expectedValue.equals(other.expectedValue))
            && (newValue == null ? other.newValue == null : newValue.equals(other.newValue));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(expectedValue, newValue);
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add("hasExpectedValue", hasExpectedValue)
                .add(EXPECTED_VALUE, expectedValue)
                .add("hasNewValue", hasNewValue)
                .add(NEW_VALUE, newValue)
                .toString();
    }
}
