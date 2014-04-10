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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import io.libraft.kayvee.TestLoggingRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith(Parameterized.class)
public final class KayVeeSetValueConversionTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KayVeeSetValueConversionTest.class);

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {"fixtures/set_value.empty_object.json", null, false, null, false},
                {"fixtures/set_value.both_fields_null.json", null, true, null, true},
                {"fixtures/set_value.non_empty_expected_value_null_new_value.json", "expected", true, null, true},
                {"fixtures/set_value.null_expected_value_non_empty_new_value.json", null, true, "new", true},
                {"fixtures/set_value.both_fields_non_empty.json", "expected", true, "new", true}
        });
    }

    private final ObjectMapper MAPPER = new ObjectMapper();
    private final String resourceFilename;
    private final @Nullable String expectedValue;
    private final boolean shouldHasExpectedValueBeenSet;
    private final @Nullable String newValue;
    private final boolean shouldHasNewValueBeenSet;

    @Rule
    public final TestLoggingRule loggingRule = new TestLoggingRule(LOGGER);

    public KayVeeSetValueConversionTest(
            String resourceFilename,
            @Nullable String expectedValue,
            boolean shouldHasExpectedValueBeenSet,
            @Nullable String newValue,
            boolean shouldHasNewValueBeenSet) {
        this.resourceFilename = resourceFilename;
        this.expectedValue = expectedValue;
        this.shouldHasExpectedValueBeenSet = shouldHasExpectedValueBeenSet;
        this.newValue = newValue;
        this.shouldHasNewValueBeenSet = shouldHasNewValueBeenSet;
    }

    @Test
    public void shouldDeserializeJSONObjectAndSetAppropriateParameters()
            throws URISyntaxException, IOException
    {
        SetValue setValue = MAPPER.readValue(new FileInputStream(new File(Resources.getResource(resourceFilename).toURI())), SetValue.class);

        assertThat(setValue.hasExpectedValue(), equalTo(shouldHasExpectedValueBeenSet));
        assertThat(setValue.getExpectedValue(), equalTo(expectedValue));

        assertThat(setValue.hasNewValue(), equalTo(shouldHasNewValueBeenSet));
        assertThat(setValue.getNewValue(), equalTo(newValue));
    }
}
