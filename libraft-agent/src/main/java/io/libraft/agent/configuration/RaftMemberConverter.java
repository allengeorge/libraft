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

package io.libraft.agent.configuration;

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
import com.google.common.net.HostAndPort;
import io.libraft.agent.RaftMember;

import java.io.IOException;
import java.net.InetSocketAddress;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides Jackson {@link JsonSerializer} and {@link JsonDeserializer}
 * implementations that convert a {@link RaftMember} to/from its JSON representation.
 */
public abstract class RaftMemberConverter {

    /**
     * JSON property that contains the {@link RaftMember#getId()} value.
     */
    public static final String MEMBER_ID_FIELD = "id";

    /**
     * JSON property that contains the {@link RaftMember#getAddress()} value.
     */
    public static final String MEMBER_ENDPOINT_FIELD = "endpoint";

    private RaftMemberConverter() { } // to prevent instantiation

    /**
     * {@code JsonSerializer} implementation that converts a {@link RaftMember}
     * instance into its corresponding JSON representation.
     */
    public static final class Serializer extends JsonSerializer<RaftMember> {

        /**
         * {@inheritDoc}
         * <p/>
         * The {@link RaftMember#getAddress()} call to {@code raftMember}
         * <strong>must</strong> return an instance of {@link InetSocketAddress}.
         * All other address forms are <strong>unsupported</strong>.
         */
        @SuppressWarnings("DuplicateThrows")
        @Override
        public void serialize(RaftMember raftMember, JsonGenerator generator, SerializerProvider provider) throws IOException, JsonProcessingException {
            generator.writeStartObject();

            // id
            provider.defaultSerializeField(MEMBER_ID_FIELD, raftMember.getId(), generator);

            // address (we only support InetSocketAddress)
            InetSocketAddress listenAddress = (InetSocketAddress) raftMember.getAddress();
            provider.defaultSerializeField(MEMBER_ENDPOINT_FIELD, String.format("%s:%d", listenAddress.getHostName(), listenAddress.getPort()), generator);

            generator.writeEndObject();
        }
    }

    /**
     * {@code JsonDeserializer} implementation that converts a JSON object
     * into a corresponding {@link RaftMember} instance.
     */
    public static final class Deserializer extends JsonDeserializer<RaftMember> {

        @SuppressWarnings("DuplicateThrows")
        @Override
        public RaftMember deserialize(JsonParser parser, DeserializationContext context) throws IOException, JsonProcessingException {
            ObjectCodec objectCodec = parser.getCodec();
            JsonNode memberNode = objectCodec.readTree(parser);

            JsonNode node;
            try {
                node = memberNode.get(MEMBER_ID_FIELD);
                node = checkNotNull(node, "%s field missing", MEMBER_ID_FIELD);
                String id = node.textValue();

                node = memberNode.get(MEMBER_ENDPOINT_FIELD);
                node = checkNotNull(node, "%s field missing", MEMBER_ENDPOINT_FIELD);
                String endpoint = node.textValue();

                HostAndPort raftHostAndPort = Endpoints.getValidHostAndPortFromString(endpoint);
                return new RaftMember(id, InetSocketAddress.createUnresolved(raftHostAndPort.getHostText(), raftHostAndPort.getPort()));
            } catch (Exception e) {
                throw new JsonMappingException("invalid configuration", e);
            }
        }
    }
}