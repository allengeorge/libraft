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

package io.libraft.kayvee.health;

import com.google.common.util.concurrent.ListenableFuture;
import com.yammer.metrics.core.HealthCheck;
import io.libraft.NotLeaderException;
import io.libraft.kayvee.store.DistributedStore;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link HealthCheck} specialization that checks if the local
 * KayVee server is connected to the Raft cluster. Returns {@code OK} if:
 * <ul>
 *     <li>The local server is not the leader but knows that the cluster
 *         <strong>has</strong> a valid leader. There <strong>can</strong>
 *         be a <strong>false positive</strong> in the period
 *         between a leader failure and the local server's election timeout.</li>
 *     <li>The local server is the leader of the cluster. In this case it
 *         will only return {@code OK} if it can issue and commit a
 *         KayVee "NOP" command. There <strong>can</strong> be a
 *         <strong>false negative</strong> if the command cannot be committed
 *         due to a transient timeout.</li>
 * </ul>
 */
public final class DistributedStoreCheck extends HealthCheck {

    private static final int TIMEOUT = 500;
    private static final TimeUnit TIMEOUT_TIME_UNIT = TimeUnit.MILLISECONDS;

    private final DistributedStore distributedStore;

    /**
     * Constructor.
     *
     * @param distributedStore instance of {@code DistributedStore} through
     *                         which a "NOP" command is submitted to the Raft cluster
     */
    public DistributedStoreCheck(DistributedStore distributedStore) {
        super("distributed-store");
        this.distributedStore = distributedStore;
    }

    @Override
    protected Result check() {
        try {
            ListenableFuture<Void> nopFuture = distributedStore.nop();
            nopFuture.get(TIMEOUT, TIMEOUT_TIME_UNIT);
            return Result.healthy("server is leader");
        } catch (TimeoutException e) {
            return Result.unhealthy(String.format("timed out after attempting to reach cluster for %d %s", TIMEOUT, TIMEOUT_TIME_UNIT));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NotLeaderException) {
                NotLeaderException leaderException = (NotLeaderException) e.getCause();
                String leader = leaderException.getLeader();

                if (leader != null) {
                    return Result.healthy(String.format("server not leader, but cluster has leader: %s", leader));
                } else {
                    return Result.unhealthy("cluster does not have a leader");
                }
            } else {
                return getDefaultUnhealthyResult(e.getCause());
            }
        } catch (Exception e) {
            return getDefaultUnhealthyResult(e);
        }
    }

    private static Result getDefaultUnhealthyResult(Throwable t) {
        return Result.unhealthy(t);
    }
}
