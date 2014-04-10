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

package io.libraft.kayvee.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.io.Resources;
import com.yammer.dropwizard.config.ConfigurationException;
import com.yammer.dropwizard.config.ConfigurationFactory;
import com.yammer.dropwizard.validation.Validator;
import io.libraft.kayvee.KayVeeConfigurationFixture;
import io.libraft.kayvee.TestLoggingRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public final class KayVeeConfigurationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KayVeeConfigurationTest.class);
    private static final ObjectWriter WRITER = new ObjectMapper().writerWithDefaultPrettyPrinter();

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private ConfigurationFactory<KayVeeConfiguration> configurationFactory;

    @Before
    public void setup() {
        configurationFactory = ConfigurationFactory.forClass(KayVeeConfiguration.class, new Validator());
    }

    @Test
    public void shouldLoadValidConfiguration() throws IOException, ConfigurationException {
        URL kayVeeConfigUrl = Resources.getResource("fixtures/kayvee.good.yml");
        KayVeeConfiguration kayVeeConfiguration = configurationFactory.build(new File(kayVeeConfigUrl.getPath()));
        LOGGER.info("{}, {}", WRITER.writeValueAsString(kayVeeConfiguration), WRITER.writeValueAsString(KayVeeConfigurationFixture.KAYVEE_CONFIGURATION));
        assertThat(kayVeeConfiguration, equalTo(KayVeeConfigurationFixture.KAYVEE_CONFIGURATION));
    }

    @Test
    public void shouldFailToLoadInvalidConfiguration() throws IOException, ConfigurationException {
        expectedException.expect(JsonProcessingException.class);
        configurationFactory.build(new File(Resources.getResource("fixtures/kayvee.bad.yml").getPath()));
    }
}