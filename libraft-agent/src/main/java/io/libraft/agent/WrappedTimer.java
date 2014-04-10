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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * A {@link Timer} implementation that wraps a {@link java.util.Timer}.
 * Tasks submitted to this implementation are delegated to the underlying
 * {@code java.util.Timer} instance.
 * <p/>
 * The underlying {@code java.util.Timer} uses a <strong>single</strong>
 * thread to run all submitted {@link io.libraft.algorithm.Timer.TimeoutTask} instances. This
 * means a long-running task may block other tasks from being
 * executed when their timeout expires. To avoid starving other
 * tasks callers <strong>must</strong> either:
 * <ul>
 *     <li>Not run long-running operations on the timer thread.</li>
 *     <li>Break up their task into smaller, less-time-intensive pieces.</li>
 * </ul>
 * The underlying {@code java.util.Timer} <strong>will</strong>
 * terminate if a task throws an exception. This means that callers
 * <strong>should</strong> wrap their execution code in a {@code try-catch}
 * that handles all exceptions.
 */
public final class WrappedTimer implements Timer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WrappedTimer.class);

    private final class WrappedTimeoutHandle implements TimeoutHandle {

        private final TimeoutTask task;
        private final TimerTask timerTask;

        private WrappedTimeoutHandle(TimeoutTask task) {
            this.task = task;
            this.timerTask = new TimerTask() {
                @Override
                public void run() {
                    WrappedTimeoutHandle.this.task.run(WrappedTimeoutHandle.this);
                }
            };
        }

        @Override
        public final boolean cancel() {
            return timerTask.cancel();
        }

        private TimerTask getTimerTask() {
            return timerTask;
        }
    }

    private boolean running;
    private java.util.Timer timer;

    /**
     * Start the timer.
     * <p/>
     * Creates the underlying {@code java.util.Timer} and its task-running daemon thread.
     * Following a successful call to {@code start()} subsequent calls are noops.
     */
    public synchronized void start() {
        if (!running) {
            checkState(timer == null);

            timer = new java.util.Timer(true); // use daemon thread internally
            schedulePurging(); // schedule timeout to purge cancelled tasks from the timer task queue

            running = true;
        }
    }

    // NOTE: I explicitly chose not to execute any outstanding tasks
    // when stop() is called. stop() may be called in both normal and
    // error conditions. If its called in error conditions the system
    // may be in an undefined state, and running any additional tasks
    // may exacerbate the situation, and are unlikely to succeed.
    /**
     * Stop the timer.
     * <p/>
     * Terminates the task-running daemon thread. Outstanding tasks
     * are <strong>not</strong> executed. Following a successful call
     * to {@code stop()} subsequent calls are noops.
     */
    public synchronized void stop() {
        if (running) {
            timer.cancel();
            timer = null;
            running = false;
        }
    }

    @Override
    public synchronized TimeoutHandle newTimeout(final TimeoutTask task, long timeout, TimeUnit timeUnit) {
        checkArgument(timeout >= 0, "negative timeout:%s", timeout);

        WrappedTimeoutHandle timeoutHandle = new WrappedTimeoutHandle(task);

        checkState(running, "timer not running");
        timer.schedule(timeoutHandle.getTimerTask(), timeUnit.toMillis(timeout));

        return timeoutHandle;
    }

    private void schedulePurging() {
        try {
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        timer.purge();
                    } catch (IllegalStateException e) {
                        LOGGER.warn("purging failed - timer cancelled");
                    }
                }
            }, 0, TimeUnit.SECONDS.toMillis(30));
        } catch (IllegalStateException e) {
            LOGGER.warn("purge scheduling failed - timer cancelled");
        }
    }
}
