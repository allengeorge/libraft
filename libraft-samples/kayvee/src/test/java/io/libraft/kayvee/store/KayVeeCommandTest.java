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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;
import io.libraft.Command;
import io.libraft.agent.protocol.AppendEntries;
import io.libraft.agent.protocol.RaftRPC;
import io.libraft.algorithm.LogEntry;
import io.libraft.kayvee.TestLoggingRule;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public final class KayVeeCommandTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KayVeeCommandTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ObjectWriter WRITER = MAPPER.writerWithDefaultPrettyPrinter();

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Test
    public void shouldSerializeAndDeserializeKayVeeCommand() throws IOException {
        RaftRPC.setupJacksonAnnotatedCommandSerializationAndDeserialization(MAPPER, KayVeeCommand.class);

        KayVeeCommand.SETCommand submittedCommand = new KayVeeCommand.SETCommand(1, "key", "value");
        AppendEntries submittedAppendEntries = new AppendEntries(
                "S_01",
                "S_02",
                3, 0, 0, 0,
                Lists.<LogEntry>newArrayList(
                        new LogEntry.ClientEntry(1, 1, submittedCommand)
                )
        );

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        MAPPER.writeValue(os, submittedAppendEntries);

        LOGGER.info("serialized to json:{}", new String(os.toByteArray()));

        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        AppendEntries deserializedAppendEntries = MAPPER.readValue(is, AppendEntries.class);

        LOGGER.info("submitted:{} deserialized:{}", WRITER.writeValueAsString(submittedAppendEntries), WRITER.writeValueAsString(deserializedAppendEntries));

        assertThat(deserializedAppendEntries, equalTo(submittedAppendEntries));

        Collection<LogEntry> deserializedEntries = checkNotNull(deserializedAppendEntries.getEntries());
        LogEntry.ClientEntry submittedEntry = (LogEntry.ClientEntry) deserializedEntries.iterator().next();

        assertThat(submittedEntry.getCommand(), Matchers.<Command>equalTo(submittedCommand));
    }
}
