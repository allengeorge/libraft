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

package io.libraft.agent.protocol;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.Sets;
import io.libraft.Command;
import io.libraft.algorithm.LogEntry;

import java.io.IOException;

/**
 * Defines Jackson {@link JsonSerializer} and
 * {@link JsonDeserializer} implementations that
 * transform a {@link LogEntry} to/from the
 * {@link io.libraft.agent.RaftAgent} JSON wire protocol.
 */
public abstract class RaftRPCLogEntry {

    /**
     * JSON property that contains the {@link LogEntry#getType()} value.
     */
    public static final String LOG_ENTRY_TYPE_FIELD = "type";

    /**
     * JSON property that contains the {@link LogEntry#getIndex()} value.
     */
    public static final String LOG_ENTRY_INDEX_FIELD = "index";

    /**
     * JSON property that contains the {@link LogEntry#getTerm()} value.
     */
    public static final String LOG_ENTRY_TERM_FIELD = "term";

    /**
     * JSON property that contains the {@link io.libraft.algorithm.LogEntry.ClientEntry#getCommand()} value
     * of a {@link io.libraft.algorithm.LogEntry.ClientEntry}.
     */
    public static final String CLIENT_ENTRY_COMMAND_FIELD = "command";

    private RaftRPCLogEntry() { } // to prevent instantiation

    /**
     * {@code JsonSerializer} that converts {@link LogEntry} and
     * its subclasses into a JSON object.
     */
    public static final class Serializer extends JsonSerializer<LogEntry> {

        @SuppressWarnings("DuplicateThrows")
        @Override
        public void serialize(LogEntry logEntry, JsonGenerator generator, SerializerProvider provider) throws IOException, JsonProcessingException {
            generator.writeStartObject();

            LogEntry.Type type = logEntry.getType();

            provider.defaultSerializeField(LOG_ENTRY_TYPE_FIELD, logEntry.getType().name(), generator);
            provider.defaultSerializeField(LOG_ENTRY_INDEX_FIELD, logEntry.getIndex(), generator);
            provider.defaultSerializeField(LOG_ENTRY_TERM_FIELD, logEntry.getTerm(), generator);

            switch (type) {
                case CLIENT:
                    Command command = ((LogEntry.ClientEntry) logEntry).getCommand();
                    provider.defaultSerializeField(CLIENT_ENTRY_COMMAND_FIELD, command, generator); // TODO (AG): interesting...is this correct?
                    break;
                default: // nothing
                    break;
            }

            generator.writeEndObject();
        }
    }

    /**
     * {@code JsonDeserializer} that converts
     * a JSON object into a subclass of {@link LogEntry}.
     */
    public static final class Deserializer extends JsonDeserializer<LogEntry> {

        @SuppressWarnings("DuplicateThrows")
        @Override
        public LogEntry deserialize(JsonParser parser, DeserializationContext context) throws IOException, JsonProcessingException {
            ObjectCodec codec = parser.getCodec();
            JsonNode logEntryNode = codec.readTree(parser);

            long index = logEntryNode.get(LOG_ENTRY_INDEX_FIELD).asLong();
            long term = logEntryNode.get(LOG_ENTRY_TERM_FIELD).asLong();

            String stringType = logEntryNode.get(LOG_ENTRY_TYPE_FIELD).textValue();
            if (stringType.equals(LogEntry.Type.NOOP.name())) {
                return new LogEntry.NoopEntry(index, term);

            } else if (stringType.equals(LogEntry.Type.CLIENT.name())) {
                JsonNode commandNode = logEntryNode.get(CLIENT_ENTRY_COMMAND_FIELD);
                Command command = codec.treeToValue(commandNode, Command.class);
                return new LogEntry.ClientEntry(index, term, command);

            } else if (stringType.equals(LogEntry.Type.CONFIGURATION.name())) {
                return new LogEntry.ConfigurationEntry(index, term, Sets.<String>newHashSet(), Sets.<String>newHashSet());

            } else {
                throw new JsonMappingException("unknown type " + stringType);
            }
        }
    }
}
