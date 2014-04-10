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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Objects;
import io.libraft.Command;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AgentTestCommand.NOPCommand.class, name = "NOP"),
        @JsonSubTypes.Type(value = AgentTestCommand.GETCommand.class, name = "GET"),
        @JsonSubTypes.Type(value = AgentTestCommand.ALLCommand.class, name = "ALL"),
        @JsonSubTypes.Type(value = AgentTestCommand.SETCommand.class, name = "SET"),
        @JsonSubTypes.Type(value = AgentTestCommand.DELCommand.class, name = "DEL")
})
abstract class AgentTestCommand implements Command {

    private static final String ID = "id";
    private static final String KEY = "key";
    private static final String VALUE = "value";

    enum Type {
        NOP,
        GET,
        ALL,
        SET,
        DEL
    }

    //----------------------------------------------------------------------------------------------------------------//

    @JsonIgnore
    private final Type type;

    @Min(0)
    @JsonProperty(ID)
    private final long id;

    AgentTestCommand(Type type, long id) {
        this.type = type;
        this.id = id;
    }

    public Type getType() {
        return type;
    }

    public long getId() {
        return id;
    }

    //----------------------------------------------------------------------------------------------------------------//

    /**
     * Simple specialization that adds no extra fields over the base command
     */
    static final class NOPCommand extends AgentTestCommand {

        NOPCommand(@JsonProperty(ID) long id) {
            super(Type.NOP, id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NOPCommand other = (NOPCommand) o;

            return getId() == other.getId();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getType(), getId());
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add(ID, getId())
                    .toString();
        }
    }

    /**
     * Simple command with one extra field more than the base command
     */
    static final class GETCommand extends AgentTestCommand {

        @NotEmpty
        @JsonProperty(KEY)
        private final String key;

        @JsonCreator
        public GETCommand(@JsonProperty(ID) long id, @JsonProperty(KEY) String key) {
            super(Type.GET, id);
            this.key = key;
        }

        String getKey() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GETCommand other = (GETCommand) o;

            return getId() == other.getId() && key.equals(other.key);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getType(), getId(), key);
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add(ID, getId())
                    .add(KEY, key)
                    .toString();
        }
    }

    /**
     * Identical to the NOP command (except for type)
     */
    static final class ALLCommand extends AgentTestCommand {

        ALLCommand(@JsonProperty(ID) long id) {
            super(Type.ALL, id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ALLCommand other = (ALLCommand) o;

            return getId() == other.getId();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getType(), getId());
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add(ID, getId())
                    .toString();
        }
    }

    /**
     * Command with more fields than either GET or DEL
     */
    static final class SETCommand extends AgentTestCommand {

        @NotEmpty
        @JsonProperty(KEY)
        private final String key;

        @NotEmpty
        @JsonProperty(VALUE)
        private final String value;

        @JsonCreator
        public SETCommand(@JsonProperty(ID) long id, @JsonProperty(KEY) String key, @JsonProperty(VALUE) String value) {
            super(Type.SET, id);
            this.key = key;
            this.value = value;
        }

        String getKey() {
            return key;
        }

        String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SETCommand other = (SETCommand) o;

            return getId() == other.getId() && key.equals(other.key) && value.equals(other.value);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getType(), getId(), key, value);
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add(ID, getId())
                    .add(KEY, key)
                    .add(VALUE, value)
                    .toString();
        }
    }

    /**
     * Similar to the GET command
     */
    static final class DELCommand extends AgentTestCommand {

        @NotEmpty
        @JsonProperty(KEY)
        private final String key;

        @JsonCreator
        public DELCommand(@JsonProperty(ID) long id, @JsonProperty(KEY) String key) {
            super(Type.DEL, id);
            this.key = key;
        }

        String getKey() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DELCommand other = (DELCommand) o;

            return getId() == other.getId() && key.equals(other.key);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getType(), getId(), key);
        }

        @Override
        public String toString() {
            return Objects
                    .toStringHelper(this)
                    .add(ID, getId())
                    .add(KEY, key)
                    .toString();
        }
    }
}