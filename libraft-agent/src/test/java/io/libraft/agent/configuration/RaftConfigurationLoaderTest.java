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

package io.libraft.agent.configuration;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.io.Resources;
import io.libraft.agent.TestLoggingRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public final class RaftConfigurationLoaderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftConfigurationLoaderTest.class);
    private static final ObjectWriter WRITER = new ObjectMapper().writerWithDefaultPrettyPrinter();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Test
    public void shouldDeserializeConfiguration() throws IOException, RaftConfigurationException {
        String configFilePath = Resources.getResource("fixtures/config.required_fields_only.good.json").getPath();
        RaftConfiguration configuration = RaftConfigurationLoader.loadFromFile(configFilePath);
        assertThat(WRITER.writeValueAsString(configuration), configuration, equalTo(RaftConfigurationFixture.RAFT_REQUIRED_FIELDS_ONLY_CONFIGURATION));
    }

    @Test
    public void shouldDeserializeConfigurationWithRequiredFieldsOnlyAndEmptyPassword() throws IOException, RaftConfigurationException {
        String configFilePath = Resources.getResource("fixtures/config.required_fields_only_empty_password.good.json").getPath();
        RaftConfiguration configuration = RaftConfigurationLoader.loadFromFile(configFilePath);
        assertThat(WRITER.writeValueAsString(configuration), configuration, equalTo(RaftConfigurationFixture.RAFT_REQUIRED_FIELDS_ONLY_EMPTY_PASSWORD_CONFIGURATION));
    }

    @Test
    public void shouldDeserializeConfigurationWithRequiredFieldsOnlyAndNoPassword() throws IOException, RaftConfigurationException {
        String configFilePath = Resources.getResource("fixtures/config.required_fields_only_no_password.good.json").getPath();
        RaftConfiguration configuration = RaftConfigurationLoader.loadFromFile(configFilePath);
        assertThat(WRITER.writeValueAsString(configuration), configuration, equalTo(RaftConfigurationFixture.RAFT_REQUIRED_FIELDS_ONLY_NO_PASSWORD_CONFIGURATION));
    }

    @Test
    public void shouldDeserializeConfigurationWithAllOptionalFields() throws IOException, RaftConfigurationException {
        String configFilePath = Resources.getResource("fixtures/config.all_fields.good.json").getPath();
        RaftConfiguration configuration = RaftConfigurationLoader.loadFromFile(configFilePath);
        assertThat(WRITER.writeValueAsString(configuration), configuration, equalTo(RaftConfigurationFixture.RAFT_ALL_FIELDS_CONFIGURATION));
    }

    @Test
    public void shouldFailToLoaConfigurationThatFailsValidation() throws IOException, RaftConfigurationException {
        String configFilePath = Resources.getResource("fixtures/config.bad.1.json").getPath();
        expectedException.expect(RaftConfigurationException.class);
        RaftConfigurationLoader.loadFromFile(configFilePath);
    }

    @Test
    public void shouldFailToLoadConfigurationThatHasUnexpectedJsonFields() throws IOException, RaftConfigurationException {
        String configFilePath = Resources.getResource("fixtures/config.bad.2.json").getPath();
        expectedException.expect(UnrecognizedPropertyException.class);
        RaftConfigurationLoader.loadFromFile(configFilePath);
    }

    @Test
    public void shouldFailToLoadConfigurationThatIsMissingMemberFields() throws IOException, RaftConfigurationException {
        String configFilePath = Resources.getResource("fixtures/config.missing_member_fields.bad.json").getPath();
        expectedException.expect(JsonMappingException.class);
        RaftConfigurationLoader.loadFromFile(configFilePath);
    }

    @Test
    public void shouldFailToLoadConfigurationThatHasInvalidMemberEndpoint() throws IOException, RaftConfigurationException {
        String configFilePath = Resources.getResource("fixtures/config.invalid_member_endpoint.bad.json").getPath();
        expectedException.expect(JsonMappingException.class);
        RaftConfigurationLoader.loadFromFile(configFilePath);
    }
}
