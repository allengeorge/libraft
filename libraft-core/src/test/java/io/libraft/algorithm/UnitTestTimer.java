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

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@SuppressWarnings("unused")
final class UnitTestTimer implements Timer {

    private enum TaskState {
        NEW,
        EXECUTED,
        CANCELLED,
    }

    private final class TaskWrapper implements TimeoutHandle {

        private final TimeoutTask task;
        private final long executionTime;

        private TaskState state = TaskState.NEW;

        private TaskWrapper(TimeoutTask task, long executionTime) {
            this.task = task;
            this.executionTime = executionTime;
        }

        @Override
        public boolean cancel() {
            if (state == TaskState.NEW) {
                state = TaskState.CANCELLED;
            }

            return state == TaskState.CANCELLED;
        }

        private boolean isCancelled() {
            return state == TaskState.CANCELLED;
        }

        private void runTask() {
            checkState(state == TaskState.NEW);

            task.run(this);
            state = TaskState.EXECUTED;
        }
    }

    private final PriorityBlockingQueue<TaskWrapper> wrappers = new PriorityBlockingQueue<TaskWrapper>(100, new Comparator<TaskWrapper>() {
        @Override
        public int compare(TaskWrapper taskWrapper0, TaskWrapper taskWrapper1) {
            if (taskWrapper0.executionTime == taskWrapper1.executionTime) {
                return 0;
            }

            return (taskWrapper0.executionTime < taskWrapper1.executionTime ? -1 : 1);
        }
    });

    private long tick = 0;

    @Override
    public TimeoutHandle newTimeout(TimeoutTask task, long relativeTimeout, TimeUnit timeUnit) {
        checkArgument(relativeTimeout >= 0, "negative timeout:%s", relativeTimeout);

        TaskWrapper taskWrapper = new TaskWrapper(task, tick + timeUnit.toMillis(relativeTimeout));

        boolean added = wrappers.offer(taskWrapper);
        checkState(added);

        return taskWrapper;
    }

    boolean hasTasks() {
        return !wrappers.isEmpty();
    }

    long getTick() {
        return tick;
    }

    long getTickForHandle(TimeoutHandle handle) {
        TaskWrapper wrapper = getTaskWrapper(handle);
        return wrapper.executionTime;
    }

    /**
     * <strong>DO NOT</strong> call if {@link UnitTestTimer#hasTasks} returns {@code false}.
     * @return tick for the last scheduled task
     */
    long getTickForLastScheduledTask() {
        checkState(!wrappers.isEmpty());

        long lastTaskTick = 0;

        for (TaskWrapper wrapper : wrappers) {
            // see javadoc for PriorityBlockingQueue
            // iterator is not guaranteed to traverse this
            // priority queue in priority order
            if (!wrapper.isCancelled() && wrapper.executionTime > lastTaskTick) {
                lastTaskTick = wrapper.executionTime;
            }
        }

        return lastTaskTick;
    }

    /**
     * Moves time forward as much as necessary to execute the
     * <strong>single</strong> next non-cancelled task. All
     * cancelled tasks are skipped. This method does not block
     * (i.e. returns immediately) if there are no pending tasks.
     */
    void fastForward() {
        boolean ranAtLeastOneTask = false;
        TaskWrapper taskWrapper;
        while ((taskWrapper = wrappers.peek()) != null && !ranAtLeastOneTask) {
            ranAtLeastOneTask = fastForward(taskWrapper.executionTime - tick); // find out how much to increment time by to execute the next task
        }
    }

    /**
     * Moves time forward an arbitrary number of ticks
     *
     * @param ticks number of clock 'ticks' to move time forward by
     * @return {@code true}, if at least one task was executed as a result of this time increment, {@code false}
     * otherwise
     */
    boolean fastForward(long ticks) {
        checkArgument(ticks >= 0);

        long finalTick = tick + ticks;
        boolean ranAtLeastOneTask = false;

        TaskWrapper taskWrapper;
        while (((taskWrapper = wrappers.peek()) != null) && (taskWrapper.executionTime <= finalTick)) {
            tick = taskWrapper.executionTime;

            try {
                taskWrapper = wrappers.take();
            } catch (InterruptedException e) {
                checkArgument(false, "interrupted while attempting to take timeout task from queue");
            }

            if (!taskWrapper.isCancelled()) {
                taskWrapper.runTask();

                if (!ranAtLeastOneTask) {
                    ranAtLeastOneTask = true;
                }
            }
        }

        // now that we've executed every task that
        // could have happened before the tick rolled around,
        // simply move time up to that tick
        tick = finalTick;

        return ranAtLeastOneTask;
    }

    boolean fastForwardTillTaskExecutes(TimeoutHandle handle) {
        TaskWrapper wrapper = getTaskWrapper(handle);
        checkArgument(wrapper.executionTime > tick, "task already executed: scheduled:%s current:%s", wrapper.executionTime, tick);
        return fastForward(wrapper.executionTime - tick);
    }

    @SuppressWarnings("ConstantConditions")
    private static TaskWrapper getTaskWrapper(TimeoutHandle handle) {
        checkArgument(handle instanceof TaskWrapper, "unsupported handle type:%s", handle.getClass().getSimpleName());
        return (TaskWrapper) handle;
    }
}
