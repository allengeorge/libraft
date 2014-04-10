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

package io.libraft.agent;

import io.libraft.algorithm.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public final class WrappedTimerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(WrappedTimerTest.class);

    private WrappedTimer timer;

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Before
    public void setup() {
        timer = new WrappedTimer();
    }

    @After
    public void teardown() {
        timer.stop();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailToCreateTimeoutIfTimerNotRunning() throws Exception {
        timer.newTimeout(new Timer.TimeoutTask() {
            @Override
            public void run(Timer.TimeoutHandle timeoutHandle) {
                // empty
            }
        }, 5, TimeUnit.SECONDS);
    }

    @Test
    public void shouldTriggerTimeout() throws Exception {
        timer.start();

        long startTime = System.currentTimeMillis();
        final AtomicLong stopTime = new AtomicLong(-1);
        timer.newTimeout(new Timer.TimeoutTask() {
            @Override
            public void run(Timer.TimeoutHandle timeoutHandle) {
                stopTime.set(System.currentTimeMillis());
            }
        }, 10, TimeUnit.MILLISECONDS);

        // sleep for longer than the timer interval, just to give it more than enough time to fire

        Thread.sleep(20);

        assertNotEquals(stopTime.get(), -1);
        assertTrue(stopTime.get() - startTime >= 10);
    }

    @Test
    public void shouldCancelPendingTasksWhenStopped() throws Exception {
        timer.start();

        final AtomicBoolean ran = new AtomicBoolean(false);
        timer.newTimeout(new Timer.TimeoutTask() {
            @Override
            public void run(Timer.TimeoutHandle timeoutHandle) {
                ran.set(true);
            }
        }, 20, TimeUnit.MILLISECONDS);

        // sleep for less time than the timeout specifies and then stop the timer

        Thread.sleep(5);

        timer.stop();

        // sleep again, and see if the task still triggers

        Thread.sleep(20);

        assertFalse(ran.get());
    }
}
