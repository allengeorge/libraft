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

import com.google.common.collect.Sets;
import io.libraft.agent.TestLoggingRule;
import io.libraft.agent.UnitTestCommand;
import io.libraft.agent.UnitTestCommandDeserializer;
import io.libraft.agent.UnitTestCommandSerializer;
import io.libraft.algorithm.LogEntry;
import io.libraft.algorithm.StorageException;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public final class JDBCLogTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCLogTest.class);

    private final Random random = new Random();
    private final UnitTestCommandSerializer commandSerializer = new UnitTestCommandSerializer();
    private final UnitTestCommandDeserializer commandDeserializer = new UnitTestCommandDeserializer();

    private JDBCLog jdbcLog;

    @Rule
    public TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Before
    public void setup() throws StorageException {
        jdbcLog = new JDBCLog("jdbc:h2:mem:", "test", null, commandSerializer, commandDeserializer);
        jdbcLog.initialize();
    }

    @Test
    public void shouldInsertAndRetrieveSentinel() throws StorageException {
        jdbcLog.put(LogEntry.SENTINEL);

        assertThat(jdbcLog.get(0), equalTo(LogEntry.SENTINEL));
    }

    @Test
    public void shouldInsertAndRetrieveNoopEntry() throws StorageException {
        long index = random.nextInt(100);
        long term = random.nextInt(100);
        LOGGER.info("index:{} term:{}", index, term);

        LogEntry logEntry = new LogEntry.NoopEntry(index, term);
        jdbcLog.put(logEntry);

        assertThat(jdbcLog.get(index), equalTo(logEntry));
    }

    @Test
    public void shouldInsertAndRetrieveConfigurationEntry() throws StorageException {
        long index = random.nextInt(100);
        long term = random.nextInt(100);
        LOGGER.info("index:{} term:{}", index, term);

        LogEntry logEntry = new LogEntry.ConfigurationEntry(index, term, Sets.<String>newHashSet(), Sets.<String>newHashSet());
        jdbcLog.put(logEntry);

        assertThat(jdbcLog.get(index), equalTo(logEntry));
    }

    @Test
    public void shouldInsertAndRetrieveClientEntry() throws StorageException {
        long index = random.nextInt(100);
        long term = random.nextInt(100);
        String data = "TEST_01";
        LOGGER.info("index:{} term:{} data:{}", index, term, data);

        LogEntry logEntry = new LogEntry.ClientEntry(index, term, new UnitTestCommand(data));
        jdbcLog.put(logEntry);

        assertThat(jdbcLog.get(index), equalTo(logEntry));
    }

    @Test
    public void shouldInsertAndRetrieveEntryEvenWhenUpdatedMultipleTimesV1() throws StorageException {
        long index = random.nextInt(100);
        long term = random.nextInt(100);
        LOGGER.info("index:{} term:{}", index, term);

        LogEntry lastInsertedEntry = new LogEntry.NoopEntry(index, term);

        // start by putting a client entry, and end off with a NOOP entry
        jdbcLog.put(new LogEntry.ClientEntry(index, term, new UnitTestCommand("TEST_01")));
        jdbcLog.put(new LogEntry.ConfigurationEntry(index, term, Sets.<String>newHashSet(), Sets.<String>newHashSet()));
        jdbcLog.put(new LogEntry.ClientEntry(index, term, new UnitTestCommand("TEST_02")));
        jdbcLog.put(lastInsertedEntry);

        assertThat(jdbcLog.get(index), equalTo(lastInsertedEntry));
    }

    @Test
    public void shouldInsertAndRetrieveEntryEvenWhenUpdatedMultipleTimesV2() throws StorageException {
        long index = random.nextInt(100);
        long term = random.nextInt(100);
        String data = "TEST_01";
        LOGGER.info("index:{} term:{} data:{}", index, term, data);

        LogEntry lastInsertedEntry = new LogEntry.ClientEntry(index, term, new UnitTestCommand("TEST_01"));

        // start off with a NOOP entry and end with a client entry
        jdbcLog.put(new LogEntry.NoopEntry(index, term));
        jdbcLog.put(new LogEntry.ConfigurationEntry(index, term, Sets.<String>newHashSet(), Sets.<String>newHashSet()));
        jdbcLog.put(new LogEntry.ClientEntry(index, term, new UnitTestCommand("TEST_02")));
        jdbcLog.put(lastInsertedEntry);

        assertThat(jdbcLog.get(index), equalTo(lastInsertedEntry));
    }

    @Test
    public void shouldReturnNullWhenGetFirstIsCalledOnEmptyTable() throws StorageException {
        assertThat(jdbcLog.getFirst(), nullValue());
    }

    @Test
    public void shouldReturnFirstValueWhenGetFirstIsCalled() throws StorageException {
        LogEntry firstEntry = new LogEntry.NoopEntry(1, 3);

        // notice that I've inserted the rows out of order
        jdbcLog.put(new LogEntry.ClientEntry(3, 3, new UnitTestCommand("LAST")));
        jdbcLog.put(firstEntry);
        jdbcLog.put(new LogEntry.ClientEntry(2, 3, new UnitTestCommand("SECOND_LAST")));

        assertThat(jdbcLog.getFirst(), equalTo(firstEntry));
    }

    @Test
    public void shouldReturnNullWhenGetLastIsCalledOnEmptyTable() throws StorageException {
        assertThat(jdbcLog.getLast(), nullValue());
    }

    @Test
    public void shouldReturnLastValueWhenGetLastIsCalled() throws StorageException {
        LogEntry lastEntry = new LogEntry.ClientEntry(3, 3, new UnitTestCommand("LAST"));

        // notice that I've inserted the rows out of order
        jdbcLog.put(new LogEntry.NoopEntry(1, 3));
        jdbcLog.put(lastEntry);
        jdbcLog.put(new LogEntry.ClientEntry(2, 3, new UnitTestCommand("SECOND_LAST")));

        assertThat(jdbcLog.getLast(), equalTo(lastEntry));
    }

    @Test
    public void shouldTruncateLogWhenIndexOfExistingEntryIsSpecified() throws StorageException {
        jdbcLog.put(LogEntry.SENTINEL);
        jdbcLog.put(new LogEntry.NoopEntry(1, 1));
        jdbcLog.put(new LogEntry.NoopEntry(2, 1));
        jdbcLog.put(new LogEntry.NoopEntry(3, 1)); // <--- expect this to be the last entry after truncation
        jdbcLog.put(new LogEntry.NoopEntry(7, 1));
        jdbcLog.put(new LogEntry.NoopEntry(8, 1));
        jdbcLog.put(new LogEntry.NoopEntry(9, 1));

        jdbcLog.truncate(7);

        assertThat(jdbcLog.getLast(), Matchers.<LogEntry>equalTo(new LogEntry.NoopEntry(3, 1)));
    }

    @Test
    public void shouldTruncateLogIfIndexNotEqualToExistingEntryIndexIsSpecified() throws StorageException {
        jdbcLog.put(LogEntry.SENTINEL);
        jdbcLog.put(new LogEntry.NoopEntry(1, 1));
        jdbcLog.put(new LogEntry.NoopEntry(2, 1));
        jdbcLog.put(new LogEntry.NoopEntry(3, 1)); // <--- expect this to be the last entry after truncation
        jdbcLog.put(new LogEntry.NoopEntry(7, 1));
        jdbcLog.put(new LogEntry.NoopEntry(8, 1));
        jdbcLog.put(new LogEntry.NoopEntry(9, 1));

        jdbcLog.truncate(6); // notice how I specified an index that doesn't exist

        assertThat(jdbcLog.getLast(), Matchers.<LogEntry>equalTo(new LogEntry.NoopEntry(3, 1)));
    }

    @Test
    public void shouldNotTruncateLogIfIndexGreaterThanLastLogEntryIsSpecified() throws StorageException {
        jdbcLog.put(LogEntry.SENTINEL);
        jdbcLog.put(new LogEntry.NoopEntry(1, 1));
        jdbcLog.put(new LogEntry.NoopEntry(2, 1));
        jdbcLog.put(new LogEntry.NoopEntry(3, 1));
        jdbcLog.put(new LogEntry.NoopEntry(7, 1));
        jdbcLog.put(new LogEntry.NoopEntry(8, 1));
        jdbcLog.put(new LogEntry.NoopEntry(9, 1)); // <--- expect this to be the last entry after truncation

        jdbcLog.truncate(11);

        assertThat(jdbcLog.getLast(), Matchers.<LogEntry>equalTo(new LogEntry.NoopEntry(9, 1)));
    }
}
