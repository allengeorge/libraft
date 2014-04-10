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

package io.libraft.kayvee.store;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import io.libraft.Committed;
import io.libraft.CommittedCommand;
import io.libraft.NotLeaderException;
import io.libraft.RaftListener;
import io.libraft.Snapshot;
import io.libraft.SnapshotWriter;
import io.libraft.agent.RaftAgent;
import io.libraft.kayvee.api.KeyValue;
import io.libraft.kayvee.api.SetValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;

/**
 * Interface to the Raft cluster.
 * <p/>
 * Contains an instance of {@link RaftAgent} and uses it to replicate
 * {@link KayVeeCommand} instances to the Raft cluster. When these instances
 * are committed they are locally applied to {@link LocalStore}. Since
 * each server in the cluster applies the committed {@code KayVeeCommand}
 * to {@code LocalStore} this transforms the cluster's distributed key-value state.
 * <p/>
 * This component is thread-safe.
 */
public class DistributedStore implements Managed, RaftListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedStore.class);
    private static final Histogram SUBMIT_COMMAND_HISTOGRAM = Metrics.newHistogram(DistributedStore.class, "submit-command");
    private static final Histogram APPLY_COMMAND_HISTOGRAM = Metrics.newHistogram(DistributedStore.class, "apply-command");

    private class PendingCommandData {

        private final SettableFuture<?> commandFuture;
        private final long startTime;

        private PendingCommandData(SettableFuture<?> commandFuture) {
            this.commandFuture = commandFuture;
            this.startTime = System.currentTimeMillis();
        }

        public SettableFuture<?> getCommandFuture() {
            return commandFuture;
        }

        public long getStartTime() {
            return startTime;
        }
    }

    private final ConcurrentMap<Long, PendingCommandData> pendingCommands = Maps.newConcurrentMap();
    private final Random random = new Random();
    private final LocalStore localStore;

    private volatile boolean running; // set during start/stop and accessed by multiple threads

    // these values are set _once_ during startup
    // safe to access by multiple threads following call to start()
    private RaftAgent raftAgent;
    private boolean initialized;

    /**
     * Constructor.
     *
     * @param localStore backing store to which {@link KayVeeCommand} operations are applied
     */
    public DistributedStore(LocalStore localStore) {
        this.localStore = localStore;
    }

    /**
     * Set the {@code RaftAgent} that functions as the local server's
     * interface to the Raft cluster.
     *
     * @param raftAgent instance of {@code RaftAgent} used to interact with the Raft cluster
     * @throws IllegalStateException if this method is called twice
     */
    public void setRaftAgent(RaftAgent raftAgent) {
        checkState(this.raftAgent == null);
        this.raftAgent = raftAgent;
    }

    /**
     * Initialize this component. This method:
     * <ul>
     *     <li>Initializes the underlying {@code RaftAgent} instance.</li>
     *     <li>Bootstraps the local server's key-value state by applying all committed {@link KayVeeCommand} operations.</li>
     * </ul>
     * Prior to calling this method, the underlying {@link RaftAgent}
     * <strong>must</strong> be set using {@link DistributedStore#setRaftAgent(RaftAgent)}.
     * Following a successful call to {@code initialize()} subsequent calls will result
     * in an {@link IllegalStateException}.
     *
     * @throws Exception if the {@code RaftAgent} cannot be initialized or the
     * local server's key-value state cannot be updated
     * @throws IllegalStateException if {@link DistributedStore#setRaftAgent(RaftAgent)} has not been
     * called, or this method is called multiple times
     */
    public synchronized void initialize() throws Exception {
        checkState(raftAgent != null);
        checkState(!initialized);

        raftAgent.initialize();
        locallyApplyUnappliedCommittedState();

        initialized = true;
    }

    private void locallyApplyUnappliedCommittedState() throws IOException {
        while(true) {
            Committed committed = raftAgent.getNextCommitted(localStore.getLastAppliedIndex());

            if (committed == null) {
                break;
            }

            applyCommittedInternal(committed);
        }
    }

    /**
     * {@inheritDoc}
     *
     * Prior to calling this method, the underlying {@link RaftAgent}
     * <strong>must</strong> be set using {@link DistributedStore#setRaftAgent(RaftAgent)},
     * and this component <strong>must</strong> be initialized using {@link DistributedStore#initialize()}.
     * Following a successful call to {@code start()} subsequent calls are noops.
     *
     * @throws IllegalStateException if either
     * {@link DistributedStore#setRaftAgent(RaftAgent)} or
     * {@link DistributedStore#initialize()} have not been called
     */
    @Override
    public synchronized void start() {
        if (running) {
            return;
        }

        checkState(raftAgent != null);
        checkState(initialized);

        raftAgent.start();

        running = true;
    }

    /**
     * {@inheritDoc}
     *
     * Shuts down the underlying {@link RaftAgent}. Following a successful call to
     * {@code stop()} subsequent calls are noops.
     * Once this method is called further operations on {@code DistributedStore}
     * <strong>will</strong> result in a {@link IllegalStateException}.
     */
    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }

        raftAgent.stop();

        running = false;
    }

    // IMPORTANT: DO NOT HOLD A LOCK WHEN CALLING issueCommandToCluster OR IN THE onLeadershipChange CALLBACK

    @Override
    public void onLeadershipChange(@Nullable String leader) {
        // noop - I don't care
    }

    @Override
    public void writeSnapshot(SnapshotWriter snapshotWriter) {
        try {
            long lastAppliedIndex = localStore.dumpState(snapshotWriter.getSnapshotOutputStream());
            snapshotWriter.setIndex(lastAppliedIndex);
            raftAgent.snapshotWritten(snapshotWriter);
        } catch (IOException e) {
            LOGGER.warn("fail create snapshot:{}", snapshotWriter);
            throw new IllegalStateException("failed to create a snapshot", e);
        }
    }

    @Override
    public void applyCommitted(Committed committed) {
        if (!running) {
            LOGGER.warn("store no longer active - not applying {}", committed);
            return;
        }

        applyCommittedInternal(committed);
    }

    private void applyCommittedInternal(Committed committed) {
        if (committed.getType() == Committed.Type.SKIP) {
            applySkipInternal(committed);
        } else if (committed.getType() == Committed.Type.SNAPSHOT) {
            Snapshot snapshot = (Snapshot) committed;
            applySnapshotInternal(snapshot);
        } else if (committed.getType() == Committed.Type.COMMAND) {
            CommittedCommand committedCommand = (CommittedCommand) committed;
            applyCommandInternal(committedCommand.getIndex(), (KayVeeCommand) committedCommand.getCommand());
        } else {
            throw new IllegalArgumentException("unsupported type:" + committed.getType().name());
        }
    }

    private void applySkipInternal(Committed committed) {
        localStore.skip(committed.getIndex());
    }

    private void applySnapshotInternal(Snapshot snapshot) {
        try {
            localStore.loadState(snapshot.getIndex(), snapshot.getSnapshotInputStream());
        } catch (IOException e) {
            LOGGER.error("fail apply snapshot {}", snapshot);
            throw new IllegalStateException("failed to apply a snapshot", e);
        }
    }

    private void applyCommandInternal(long index, KayVeeCommand kayVeeCommand) {
        SettableFuture<?> removed;

        // check if this command has not failed already
        PendingCommandData pendingCommandData = pendingCommands.remove(kayVeeCommand.getCommandId());
        if (pendingCommandData != null) {
            removed = pendingCommandData.getCommandFuture();
        } else {
            removed = SettableFuture.create(); // create a fake future just to avoid if (removed ...)
        }

        // metrics
        long applyCommandInternalStartTime = System.currentTimeMillis();
        if (pendingCommandData != null) {
            SUBMIT_COMMAND_HISTOGRAM.update(applyCommandInternalStartTime - pendingCommandData.getStartTime());
        }

        // actually apply the command
        try {
            LOGGER.info("apply {} at index {}", kayVeeCommand, index);

            switch (kayVeeCommand.getType()) {
                case NOP:
                    applyNOPCommand(index, removed);
                    break;
                case GET:
                    KayVeeCommand.GETCommand getCommand = (KayVeeCommand.GETCommand) kayVeeCommand;
                    applyGETCommand(index, getCommand, removed);
                    break;
                case ALL:
                    applyALLCommand(index, removed);
                    break;
                case SET:
                    KayVeeCommand.SETCommand setCommand = (KayVeeCommand.SETCommand) kayVeeCommand;
                    applySETCommand(index, setCommand, removed);
                    break;
                case CAS:
                    KayVeeCommand.CASCommand casCommand = (KayVeeCommand.CASCommand) kayVeeCommand;
                    applyCASCommand(index, casCommand, removed);
                    break;
                case DEL:
                    KayVeeCommand.DELCommand delCommand = (KayVeeCommand.DELCommand) kayVeeCommand;
                    applyDELCommand(index, delCommand, removed);
                    break;
                default:
                    throw new IllegalArgumentException("unsupported type:" + kayVeeCommand.getType().name());
            }
        } catch (Exception e) {
            removed.setException(e);
        } finally {
            if (pendingCommandData != null) {
                APPLY_COMMAND_HISTOGRAM.update(System.currentTimeMillis() - applyCommandInternalStartTime);
            }
        }
    }

    private void applyNOPCommand(long index, SettableFuture<?> removed) {
        localStore.nop(index);
        setRemoved(removed, null);
    }

    private void applyGETCommand(long index, KayVeeCommand.GETCommand getCommand, SettableFuture<?> removed) throws KayVeeException {
        KeyValue keyValue = localStore.get(index, getCommand.getKey());
        setRemoved(removed, keyValue);
    }

    private void applyALLCommand(long index, SettableFuture<?> removed) {
        Collection<KeyValue> keyValues = localStore.getAll(index);
        setRemoved(removed, keyValues);
    }

    private void applySETCommand(long index, KayVeeCommand.SETCommand setCommand, SettableFuture<?> removed) throws KayVeeException {
        KeyValue keyValue = localStore.set(index, setCommand.getKey(), setCommand.getNewValue());
        setRemoved(removed, keyValue);
    }

    private void applyCASCommand(long index, KayVeeCommand.CASCommand casCommand, SettableFuture<?> removed) throws KayVeeException {
        KeyValue keyValue = localStore.compareAndSet(index, casCommand.getKey(), casCommand.getExpectedValue(), casCommand.getNewValue());
        setRemoved(removed, keyValue);
    }

    private void applyDELCommand(long index, KayVeeCommand.DELCommand delCommand, SettableFuture<?> removed) throws KayVeeException {
        localStore.delete(index, delCommand.getKey());
        setRemoved(removed, null);
    }

    @SuppressWarnings("unchecked")
    private <T> void setRemoved(SettableFuture<?> removed, @Nullable T value) {
        SettableFuture<T> commandFuture = (SettableFuture<T>) removed;
        commandFuture.set(value);
    }

    //----------------------------------------------------------------------------------------------------------------//

    // client-issued

    /**
     * Issue a {@link KayVeeCommand.NOPCommand}.
     * This command will not modify the replicated storage's key-value state.
     *
     * @return future that will be triggered when the command is successfully
     * <strong>committed</strong> to replicated storage or is <strong>known</strong>
     * to have failed. This future <strong>may not</strong> be triggered. Callers
     * are advised <strong>not</strong> to wait indefinitely, and instead, use
     * timed waits.
     */
    public ListenableFuture<Void> nop() {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.NOPCommand nopCommand = new KayVeeCommand.NOPCommand(getCommandId());
        return issueCommandToCluster(nopCommand);
    }

    /**
     * Issue a {@link KayVeeCommand.GETCommand}.
     * Returns the value associated with a key.
     *
     * @param key key for which to get the value
     * @return future that will be triggered when the command is successfully
     * <strong>committed</strong> to replicated storage or is <strong>known</strong>
     * to have failed. This future <strong>may not</strong> be triggered. Callers
     * are advised <strong>not</strong> to wait indefinitely, and instead, use
     * timed waits.
     */
    public ListenableFuture<KeyValue> get(String key) {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.GETCommand getCommand = new KayVeeCommand.GETCommand(getCommandId(), key);
        return issueCommandToCluster(getCommand);
    }

    /**
     * Issue a {@link KayVeeCommand.ALLCommand}.
     * Returns all the replicated {@code key=>value} pairs in the system.
     *
     * @return future that will be triggered when the command is successfully
     * <strong>committed</strong> to replicated storage or is <strong>known</strong>
     * to have failed. This future <strong>may not</strong> be triggered. Callers
     * are advised <strong>not</strong> to wait indefinitely, and instead, use
     * timed waits.
     */
    public ListenableFuture<Collection<KeyValue>> getAll() {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.ALLCommand allCommand = new KayVeeCommand.ALLCommand(getCommandId());
        return issueCommandToCluster(allCommand);
    }

    /**
     * Issue a {@link KayVeeCommand.SETCommand}.
     * Set the value for a key.
     *
     * @param key key for which to set a value
     * @param setValue instance of {@code SetValue} from which the new value is extracted
     *                 the rules for a SET are specified in the KayVee README.md
     * @return future that will be triggered when the command is successfully
     * <strong>committed</strong> to replicated storage or is <strong>known</strong>
     * to have failed. This future <strong>may not</strong> be triggered. Callers
     * are advised <strong>not</strong> to wait indefinitely, and instead, use
     * timed waits.
     */
    public ListenableFuture<KeyValue> set(String key, SetValue setValue) {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.SETCommand setCommand = new KayVeeCommand.SETCommand(getCommandId(), key, setValue.getNewValue());
        return issueCommandToCluster(setCommand);
    }

    /**
     * Issue a {@link KayVeeCommand.CASCommand}.
     * Set the new value for a key iff its current value matches the expected value.
     * This method can be used to create keys if the expected
     * value is null, or delete keys if the new value is null.
     *
     * @param key key for which the CAS is performed
     * @param setValue instance of {@code SetValue} from which the expected value and new value are extracted
     *                 the rules for a CAS are specified in the KayVee README.md
     * @return future that will be triggered when the command is successfully
     * <strong>committed</strong> to replicated storage or is <strong>known</strong>
     * to have failed. This future <strong>may not</strong> be triggered. Callers
     * are advised <strong>not</strong> to wait indefinitely, and instead, use
     * timed waits.
     */
    public ListenableFuture<KeyValue> compareAndSet(String key, SetValue setValue) {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.CASCommand casCommand = new KayVeeCommand.CASCommand(getCommandId(), key, setValue.getExpectedValue(), setValue.getNewValue());
        return issueCommandToCluster(casCommand);
    }

    /**
     * Issue a {@link KayVeeCommand.DELCommand}.
     *
     * @param key key to delete
     * @return future that will be triggered when the command is successfully
     * <strong>committed</strong> to replicated storage or is <strong>known</strong>
     * to have failed. This future <strong>may not</strong> be triggered. Callers
     * are advised <strong>not</strong> to wait indefinitely, and instead, use
     * timed waits.
     */
    public ListenableFuture<Void> delete(String key) {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.DELCommand delCommand = new KayVeeCommand.DELCommand(getCommandId(), key);
        return issueCommandToCluster(delCommand);
    }

    private void checkThatDistributedStoreIsActive() {
        checkState(running);
    }

    private long getCommandId() {
        synchronized (random) {
            return random.nextLong();
        }
    }

    // IMPORTANT: DO NOT HOLD A LOCK WHEN CALLING issueCommandToCluster OR IN THE onLeadershipChange CALLBACK
    private <T> ListenableFuture<T> issueCommandToCluster(final KayVeeCommand kayVeeCommand) {
        final SettableFuture<T> returned = SettableFuture.create();

        try {
            PendingCommandData previous = pendingCommands.put(kayVeeCommand.getCommandId(), new PendingCommandData(returned));
            checkState(previous == null, "existing command:%s", previous);

            Futures.addCallback(returned, new FutureCallback<Object>() {
                @Override
                public void onSuccess(Object result) {
                    // success handled in the apply command block
                }

                @Override
                public void onFailure(Throwable t) {
                    pendingCommands.remove(kayVeeCommand.getCommandId());
                }
            });

            // it's possible for this to throw an IllegalStateException
            // if another thread calls "stop" while a request is just about to be submitted
            // since access to raftAgent itself is not synchronized here
            ListenableFuture<Void> consensusFuture = raftAgent.submitCommand(kayVeeCommand);
            Futures.addCallback(consensusFuture, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result) {
                    // noop - we wait until we attempt to apply the command locally
                }

                @Override
                public void onFailure(Throwable t) {
                    returned.setException(t);
                }
            });
        } catch (NotLeaderException e) {
            returned.setException(e);
        }

        return returned;
    }
}
