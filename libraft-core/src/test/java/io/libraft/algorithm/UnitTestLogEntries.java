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

package io.libraft.algorithm;

import io.libraft.Command;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.theInstance;

abstract class UnitTestLogEntries {

    protected UnitTestLogEntries() { } // private to prevent instantiation

    //================================================================================================================//
    //
    // Convenience Constructors
    //
    //================================================================================================================//

    @SuppressWarnings("SameReturnValue")
    static LogEntry SENTINEL() {
        return LogEntry.SENTINEL;
    }

    static LogEntry.NoopEntry NOOP(long index, long term) {
        return new LogEntry.NoopEntry(index, term);
    }

    static LogEntry.ClientEntry CLIENT(long index, long term, Command command) {
        return new LogEntry.ClientEntry(index, term, command);
    }

    //================================================================================================================//
    //
    // Manipulation
    //
    //================================================================================================================//

    static void clearLog(Log log) throws StorageException {
        log.truncate(0);
    }

    static void insertIntoLog(Log log, LogEntry... entries) throws StorageException {
        checkArgument(entries.length > 0);

        for (LogEntry entry : entries) {
            log.put(entry);
        }
    }

    //================================================================================================================//
    //
    // Asserts
    //
    //================================================================================================================//

    static void assertThatLogIsEmpty(Log log) throws StorageException {
        LogEntry lastLog = log.getLast();
        assertThat(lastLog, nullValue());
    }

    static void assertThatLogContainsOnlySentinel(Log log) throws StorageException {
        LogEntry lastLog = log.getLast();
        assertThat(lastLog, notNullValue());
        assertThat(lastLog, theInstance(SENTINEL()));
    }

    static void assertThatLogContains(Log log, LogEntry... entries) throws StorageException {
        entries = checkNotNull(entries);
        checkArgument(entries.length > 0);

        // ensure that all the log entries before the first given entry are null (i.e. don't exist)
        long entryIndex = entries[0].getIndex();
        for (int i = 0; i < entryIndex; i++) {
            assertThat(log.get(i), nullValue());
        }

        // all the entries in the log match those we're asked to compare against
        for(LogEntry entry : entries) {
            assertThat(entry, equalTo(log.get(entryIndex++)));
        }

        // the last entry in the log is the last one given to us
        // note that this will always be valid because we'll always have
        // at least one entry to compare in the foreach above
        assertThat(log.getLast().getIndex() + 1, equalTo(entryIndex));
    }
}
