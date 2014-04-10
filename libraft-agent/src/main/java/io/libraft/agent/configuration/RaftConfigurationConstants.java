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

package io.libraft.agent.configuration;

import java.util.concurrent.TimeUnit;

/**
 * Default constants used in {@link RaftConfiguration}.
 * These constants control network and snapshot timeouts,
 * Raft algorithm timeouts, snapshot storage directory,
 * <em>etc.</em>
 * <p/>
 * All time constants are defined in milliseconds (ms).
 */
abstract class RaftConfigurationConstants {

    /**
     * Default time unit in which all configuration
     * timeouts and intervals are specified.
     */
    static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

    /**
     * One second in milliseconds.
     */
    static final int ONE_SECOND = 1000;

    /**
     * Sixty seconds in milliseconds.
     */
    static final long SIXTY_SECONDS = 60 * ONE_SECOND;

    /**
     * One hour in milliseconds.
     */
    static final long ONE_HOUR = 60 * SIXTY_SECONDS;

    /**
     * Twelve hours in milliseconds.
     */
    static final long TWELVE_HOURS = 12 * ONE_HOUR;

    /**
     * Default directory in which snapshots are stored.
     */
    static final String DEFAULT_SNAPSHOTS_DIRECTORY = "."; // cwd

    // to prevent instantiation of RaftConfigurationConstants
    private RaftConfigurationConstants() { }
}
