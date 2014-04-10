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

import java.util.concurrent.TimeUnit;

/**
 * Timer service used by {@link RaftAlgorithm} to schedule
 * one-time tasks for future execution at a specified timeout.
 * <p/>
 * Implementations <strong>must</strong> execute
 * tasks sequentially, in order. If multiple tasks
 * are scheduled for execution at the same
 * timeout, they <strong>may</strong>
 * execute them in any arbitrary order.
 * Implementations <strong>only</strong> have to offer
 * best-effort timing accuracy. Due to scheduling delays or
 * long-running prior tasks, they
 * <strong>may</strong> execute tasks <strong>after</strong>
 * their requested timeout. Despite delays late tasks
 * <strong>must</strong> still be executed sequentially,
 * in order.
 * <p/>
 * Implementations have considerable flexibility over
 * various other implementation details, including:
 * <ul>
 *     <li>Threading.</li>
 *     <li>Handling task failures.</li>
 * </ul>
 */
public interface Timer {

    /**
     * A task that should be run by a {@link Timer} at a future timeout.
     * <p/>
     * Specializations <strong>should not</strong> assume
     * that the {@code Timer} will continue operating if
     * {@link Timer.TimeoutTask#run(io.libraft.algorithm.Timer.TimeoutHandle)}
     * throws an exception: {@code Timer} implementations
     * <strong>may</strong> terminate silently.
     */
    public interface TimeoutTask {

        /**
         * Execute the actions defined by the {@code TimeoutTask} instance.
         *
         * @param timeoutHandle {@link TimeoutHandle} assigned to this task when it was
         *                      scheduled for execution using {@link Timer#newTimeout(Timer.TimeoutTask, long, java.util.concurrent.TimeUnit)}
         */
        void run(TimeoutHandle timeoutHandle);
    }

    /**
     * Opaque handle bound to an instance of {@link TimeoutTask} that
     * can be used to control and/or query the status of that task.
     */
    public interface TimeoutHandle {

        /**
         * Cancel a task.
         *
         * @return true if the task was cancelled before it was executed, false otherwise.
         */
        boolean cancel();
    }

    /**
     * Schedule a {@code TimeoutTask} for future execution at
     * a timeout, defined by {@code currentTime + relativeTimeout}.
     *
     * @param task instance of {@code TimeoutTask} to run at, or after, the specified timeout
     * @param relativeTimeout offset >= 0 from {@code currentTime} at which the {@code task} should be run
     * @param timeUnit time unit in which {@code relativeTimeout} is specified
     * @return instance of {@code TimeoutHandle} bound to {@code task} that can be used to control it and/or query its status
     */
    TimeoutHandle newTimeout(TimeoutTask task, long relativeTimeout, TimeUnit timeUnit);
}
