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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Objects;
import io.libraft.Command;
import org.hibernate.validator.constraints.NotEmpty;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;

/**
 * Base class for {@link Command} subclasses that
 * transform the replicated KayVee key-value store.
 * There are 6 different commands:
 * <ul>
 *     <li>NOP</li>
 *     <li>GET</li>
 *     <li>ALL</li>
 *     <li>SET</li>
 *     <li>CAS</li>
 *     <li>DEL</li>
 * </ul>
 * All operations are <strong>atomic</strong>.
 * <p/>
 * See the KayVee README.md for more on KayVee commands, including, examples, valid values and return codes.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = KayVeeCommand.NOPCommand.class, name = "NOP"),
        @JsonSubTypes.Type(value = KayVeeCommand.GETCommand.class, name = "GET"),
        @JsonSubTypes.Type(value = KayVeeCommand.ALLCommand.class, name = "ALL"),
        @JsonSubTypes.Type(value = KayVeeCommand.SETCommand.class, name = "SET"),
        @JsonSubTypes.Type(value = KayVeeCommand.CASCommand.class, name = "CAS"),
        @JsonSubTypes.Type(value = KayVeeCommand.DELCommand.class, name = "DEL")
})
public abstract class KayVeeCommand implements Command { // public so that we can reference it for setup with RaftAgent

    private static final String COMMAND_ID = "commandId";
    private static final String KEY = "key";
    private static final String NEW_VALUE = "newValue";
    private static final String EXPECTED_VALUE = "expectedValue";

    /**
     * KayVee command types.
     */
    enum Type {
        NOP,
        GET,
        ALL,
        SET,
        CAS,
        DEL
    }

    //----------------------------------------------------------------------------------------------------------------//

    @JsonIgnore
    private final Type type;

    @Min(0)
    @JsonProperty(COMMAND_ID)
    private final long commandId;

    KayVeeCommand(Type type, long commandId) {
        this.type = type;
        this.commandId = commandId;
    }

    public Type getType() {
        return type;
    }

    public long getCommandId() {
        return commandId;
    }

    //----------------------------------------------------------------------------------------------------------------//

    /**
     * Noop.
     * <p/>
     * Does not modify any keys or values. Safe to apply at any time.
     */
    static final class NOPCommand extends KayVeeCommand {

        NOPCommand(@JsonProperty(COMMAND_ID) long commandId) {
            super(Type.NOP, commandId);
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NOPCommand other = (NOPCommand) o;

            return getCommandId() == other.getCommandId();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getType(), getCommandId());
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add(COMMAND_ID, getCommandId())
                    .toString();
        }
    }

    /**
     * Atomic get.
     * <p/>
     * Get the value of a {@code key} if it exists.
     */
    static final class GETCommand extends KayVeeCommand {

        @NotEmpty
        @JsonProperty(KEY)
        private final String key;

        @JsonCreator
        public GETCommand(@JsonProperty(COMMAND_ID) long commandId, @JsonProperty(KEY) String key) {
            super(Type.GET, commandId);
            this.key = key;
        }

        String getKey() {
            return key;
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GETCommand other = (GETCommand) o;

            return getCommandId() == other.getCommandId() && key.equals(other.key);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getType(), getCommandId(), key);
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add(COMMAND_ID, getCommandId())
                    .add(KEY, key)
                    .toString();
        }
    }

    /**
     * Atomic "get all".
     * <p/>
     * Return a snapshot of all committed {@code key=>value} pairs.
     */
    static final class ALLCommand extends KayVeeCommand {

        ALLCommand(@JsonProperty(COMMAND_ID) long commandId) {
            super(Type.ALL, commandId);
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ALLCommand other = (ALLCommand) o;

            return getCommandId() == other.getCommandId();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getType(), getCommandId());
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add(COMMAND_ID, getCommandId())
                    .toString();
        }
    }

    /**
     * Atomic set.
     * <p/>
     * Update {@code key} to have {@code value}. Creates {@code key} if it does not exist.
     */
    static final class SETCommand extends KayVeeCommand {

        @NotEmpty
        @JsonProperty(KEY)
        private final String key;

        @NotEmpty
        @JsonProperty(NEW_VALUE)
        private final String newValue;

        @JsonCreator
        public SETCommand(@JsonProperty(COMMAND_ID) long commandId, @JsonProperty(KEY) String key, @JsonProperty(NEW_VALUE) String newValue) {
            super(Type.SET, commandId);
            this.key = key;
            this.newValue = newValue;
        }

        String getKey() {
            return key;
        }

        String getNewValue() {
            return newValue;
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SETCommand other = (SETCommand) o;

            return getCommandId() == other.getCommandId() && key.equals(other.key) && newValue.equals(other.newValue);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getType(), getCommandId(), key, newValue);
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add(COMMAND_ID, getCommandId())
                    .add(KEY, key)
                    .add(NEW_VALUE, newValue)
                    .toString();
        }
    }

    /**
     * Atomic "compare and set".
     * <p/>
     * Update {@code key} to have {@code newValue} if the
     * <strong>current</strong> value for {@code key} is {@code expectedValue}.
     * May create or delete key depending on the
     * contents of {@code expectedValue}, {@code newValue} and current value.
     */
    static final class CASCommand extends KayVeeCommand {

        @JsonProperty(KEY)
        private final String key;

        @Nullable
        @JsonProperty(EXPECTED_VALUE)
        private final String expectedValue;

        @Nullable
        @JsonProperty(NEW_VALUE)
        private final String newValue;

        @JsonCreator
        public CASCommand(
                @JsonProperty(COMMAND_ID) long commandId,
                @JsonProperty(KEY) String key,
                @JsonProperty(EXPECTED_VALUE) @Nullable String expectedValue,
                @JsonProperty(NEW_VALUE) @Nullable String newValue) {
            super(Type.CAS, commandId);
            this.key = key;
            this.expectedValue = expectedValue;
            this.newValue = newValue;
        }

        String getKey() {
            return key;
        }

        @Nullable String getExpectedValue() {
            return expectedValue;
        }

        @Nullable String getNewValue() {
            return newValue;
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CASCommand other = (CASCommand) o;

            return getCommandId() == other.getCommandId()
                && key.equals(other.key)
                && (expectedValue == null ? other.expectedValue == null : expectedValue.equals(other.expectedValue))
                && (newValue == null ? other.newValue == null : newValue.equals(other.newValue));
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getType(), getCommandId(), key, expectedValue, newValue);
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add(COMMAND_ID, getCommandId())
                    .add(KEY, key)
                    .add(EXPECTED_VALUE, expectedValue)
                    .add(NEW_VALUE, newValue)
                    .toString();
        }
    }

    /**
     * Atomic delete.
     * <p/>
     * Remove a {@code key=>value} pair.
     */
    static final class DELCommand extends KayVeeCommand {

        @NotEmpty
        @JsonProperty(KEY)
        private final String key;

        @JsonCreator
        public DELCommand(@JsonProperty(COMMAND_ID) long commandId, @JsonProperty(KEY) String key) {
            super(Type.DEL, commandId);
            this.key = key;
        }

        String getKey() {
            return key;
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DELCommand other = (DELCommand) o;

            return getCommandId() == other.getCommandId() && key.equals(other.key);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getType(), getCommandId(), key);
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add(COMMAND_ID, getCommandId())
                    .add(KEY, key)
                    .toString();
        }
    }
}