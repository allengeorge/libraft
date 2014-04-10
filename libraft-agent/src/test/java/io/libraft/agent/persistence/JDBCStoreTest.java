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

package io.libraft.agent.persistence;

import io.libraft.agent.TestLoggingRule;
import io.libraft.algorithm.StorageException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public final class JDBCStoreTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCStore.class);

    private final Random random = new Random();

    private JDBCStore jdbcStore;

    @Rule
    public TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Before
    public void setupDatabase() throws StorageException {
        jdbcStore = new JDBCStore("jdbc:h2:mem:", "test", null);
        jdbcStore.initialize();
    }

    private long getRandomLong() {
        long value = random.nextInt(100);
        LOGGER.info("random:{}", value);
        return value;
    }

    @Test
    public void shouldSetAndGetCurrentTerm() throws StorageException {
        long currentTerm = getRandomLong();

        jdbcStore.setCurrentTerm(currentTerm);

        assertThat(jdbcStore.getCurrentTerm(), equalTo(currentTerm));
    }

    @Test
    public void shouldSetAndGetCurrentTermEvenWhenCalledMultipleTimes() throws StorageException {
        long currentTerm = getRandomLong();

        jdbcStore.setCurrentTerm(currentTerm);
        jdbcStore.setCurrentTerm(currentTerm);
        jdbcStore.setCurrentTerm(currentTerm);

        assertThat(jdbcStore.getCurrentTerm(), equalTo(currentTerm));
    }

    @Test
    public void shouldSetAndGetCommitIndex() throws StorageException {
        long commitIndex = getRandomLong();

        jdbcStore.setCommitIndex(commitIndex);

        assertThat(jdbcStore.getCommitIndex(), equalTo(commitIndex));
    }

    @Test
    public void shouldSetAndGetCommitIndexEvenWhenCalledMultipleTimes() throws StorageException {
        long commitIndex = getRandomLong();

        jdbcStore.setCommitIndex(commitIndex);
        jdbcStore.setCommitIndex(commitIndex);
        jdbcStore.setCommitIndex(commitIndex);

        assertThat(jdbcStore.getCommitIndex(), equalTo(commitIndex));
    }

    @Test
    public void shouldSetAndGetVotedFor() throws StorageException {
        long term = getRandomLong();

        jdbcStore.setVotedFor(term , "SERVER_01");

        assertThat(jdbcStore.getVotedFor(term), equalTo("SERVER_01"));
    }

    @Test
    public void shouldSetAndGetVotedForEvenWhenCalledMultipleTimes() throws StorageException {
        long term = getRandomLong();

        jdbcStore.setVotedFor(term, "SERVER_01");
        jdbcStore.setVotedFor(term, "SERVER_01");
        jdbcStore.setVotedFor(term, "SERVER_01");

        assertThat(jdbcStore.getVotedFor(term), equalTo("SERVER_01"));
    }

    @Test
    public void shouldReturnNullIfNoOneWasVotedFor() throws StorageException {
        long term = getRandomLong();

        assertThat(jdbcStore.getVotedFor(term), nullValue());
    }

    @Test
    public void shouldClearVotedFor() throws StorageException {
        long term = getRandomLong();

        jdbcStore.setVotedFor(term, "SERVER_01");

        assertThat(jdbcStore.getVotedFor(term), equalTo("SERVER_01"));

        jdbcStore.clearVotedFor();

        // TODO (AG): make this test stronger by asserting that the actual table is empty
        assertThat(jdbcStore.getVotedFor(term), nullValue());
    }
}
