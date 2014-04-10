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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import io.libraft.agent.RaftRPCFixture;
import io.libraft.agent.TestLoggingRule;
import io.libraft.agent.UnitTestCommandDeserializer;
import io.libraft.agent.UnitTestCommandSerializer;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public final class RaftRPCConversionTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftRPCConversionTest.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final UnitTestCommandSerializer COMMAND_SERIALIZER = new UnitTestCommandSerializer();
    private static final UnitTestCommandDeserializer COMMAND_DESERIALIZER = new UnitTestCommandDeserializer();

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    private static <T extends RaftRPC> T fromJSONAsRaftRPC(String filename, Class<T> klass) throws IOException {
        return MAPPER.readValue(Resources.getResource(filename), klass);
    }

    private static String fromJSONAsString(String filename) throws IOException {
        return MAPPER.writeValueAsString(MAPPER.readValue(Resources.getResource(filename), JsonNode.class)).trim();
    }

    private static String toJSONAsString(RaftRPC rpc) throws JsonProcessingException {
        return MAPPER.writeValueAsString(rpc).trim();
    }

    @BeforeClass
    public static void setupCommandDeserializer() {
        RaftRPC.setupCustomCommandSerializationAndDeserialization(MAPPER, COMMAND_SERIALIZER, COMMAND_DESERIALIZER);
    }

    @Test
    public void shouldSerializeRequestVote() throws Exception {
        assertThat(toJSONAsString(RaftRPCFixture.REQUEST_VOTE), is(fromJSONAsString("fixtures/request_vote.json")));
    }

    @Test
    public void shouldDeserializeRequestVote() throws Exception {
        assertThat(fromJSONAsRaftRPC("fixtures/request_vote.json", RequestVote.class), is(RaftRPCFixture.REQUEST_VOTE));
    }

    @Test
    public void shouldDeserializeRequestVoteReply() throws Exception {
        assertThat(fromJSONAsRaftRPC("fixtures/request_vote_reply.json", RequestVoteReply.class), is(RaftRPCFixture.REQUEST_VOTE_REPLY));
    }

    @Test
    public void shouldSerializeRequestVoteReply() throws Exception {
        assertThat(toJSONAsString(RaftRPCFixture.REQUEST_VOTE_REPLY), is(fromJSONAsString("fixtures/request_vote_reply.json")));
    }

    @Test
    public void shouldSerializeAppendEntries() throws Exception {
        assertThat(toJSONAsString(RaftRPCFixture.APPEND_ENTRIES), is(fromJSONAsString("fixtures/append_entries.json")));
    }

    @Test
    public void shouldDeserializeAppendEntries() throws Exception {
        assertThat(fromJSONAsRaftRPC("fixtures/append_entries.json", AppendEntries.class), is(RaftRPCFixture.APPEND_ENTRIES));
    }

    @Test
    public void shouldSerializeAppendEntriesReply() throws Exception {
        assertThat(toJSONAsString(RaftRPCFixture.APPEND_ENTRIES_REPLY), is(fromJSONAsString("fixtures/append_entries_reply.json")));
    }

    @Test
    public void shouldDeserializeAppendEntriesReply() throws Exception {
        assertThat(fromJSONAsRaftRPC("fixtures/append_entries_reply.json", AppendEntriesReply.class), is(RaftRPCFixture.APPEND_ENTRIES_REPLY));
    }
}
