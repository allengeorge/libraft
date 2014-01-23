/*
 * Copyright (c) 2013, Allen A. George <allen dot george at gmail dot com>
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
import io.libraft.Command;
import io.libraft.CommittedCommand;
import io.libraft.NotLeaderException;
import io.libraft.RaftListener;
import io.libraft.agent.RaftAgent;
import io.libraft.algorithm.StorageException;
import io.libraft.kayvee.api.KeyValue;
import io.libraft.kayvee.api.SetValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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

    private final ConcurrentMap<Long, SettableFuture<?>> pendingCommands = Maps.newConcurrentMap();
    private final Random random = new Random();
    private final LocalStore localStore;

    private volatile boolean running; // set during start/stop and accessed by multiple threads

    // these values are set _once_ during startup
    // safe to access by multiple threads following call to start()
    private RaftAgent raftAgent;
    private boolean initialized;

    public DistributedStore(LocalStore localStore) {
        this.localStore = localStore;
    }

    public void setRaftAgent(RaftAgent raftAgent) {
        checkState(this.raftAgent == null);
        this.raftAgent = raftAgent;
    }

    public synchronized void initialize() throws StorageException {
        checkState(raftAgent != null);
        checkState(!initialized);

        raftAgent.initialize();
        locallyApplyUnappliedCommittedCommands();

        initialized = true;
    }

    private void locallyApplyUnappliedCommittedCommands() {
        long lastAppliedCommandIndex = localStore.getLastAppliedCommandIndex();

        while(true) {
            CommittedCommand committedCommand = raftAgent.getNextCommittedCommand(lastAppliedCommandIndex);

            if (committedCommand == null) {
                break;
            }

            applyCommandInternal(committedCommand.getIndex(), (KayVeeCommand) committedCommand.getCommand());
            lastAppliedCommandIndex = committedCommand.getIndex();
        }
    }

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

    @Override
    public synchronized void stop() throws Exception {
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

    //----------------------------------------------------------------------------------------------------------------//

    // cluster-committed

    @Override
    public void applyCommand(long index, Command command) {
        if (!running) {
            LOGGER.warn("store no longer active - not applying {} at index {}", command, index);
            return;
        }

        applyCommandInternal(index, (KayVeeCommand) command);
    }

    private void applyCommandInternal(long index, KayVeeCommand kayVeeCommand) {
        SettableFuture<?> removed = pendingCommands.remove(kayVeeCommand.getCommandId());
        if (removed == null) {
            removed = SettableFuture.create(); // create a fake future just to avoid if (removed ...)
        }

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
        }
    }

    private void applyNOPCommand(long commandIndex, SettableFuture<?> removed) {
        localStore.nop(commandIndex);
        setRemoved(removed, null);
    }

    private void applyGETCommand(long commandIndex, KayVeeCommand.GETCommand getCommand, SettableFuture<?> removed) throws KayVeeException {
        KeyValue keyValue = localStore.get(commandIndex, getCommand.getKey());
        setRemoved(removed, keyValue);
    }

    private void applyALLCommand(long commandIndex, SettableFuture<?> removed) {
        Collection<KeyValue> keyValues = localStore.getAll(commandIndex);
        setRemoved(removed, keyValues);
    }

    private void applySETCommand(long commandIndex, KayVeeCommand.SETCommand setCommand, SettableFuture<?> removed) throws KayVeeException {
        KeyValue keyValue = localStore.set(commandIndex, setCommand.getKey(), setCommand.getNewValue());
        setRemoved(removed, keyValue);
    }

    private void applyCASCommand(long commandIndex, KayVeeCommand.CASCommand casCommand, SettableFuture<?> removed) throws KayVeeException {
        KeyValue keyValue = localStore.compareAndSet(commandIndex, casCommand.getKey(), casCommand.getExpectedValue(), casCommand.getNewValue());
        setRemoved(removed, keyValue);
    }

    private void applyDELCommand(long commandIndex, KayVeeCommand.DELCommand delCommand, SettableFuture<?> removed) throws KayVeeException {
        localStore.delete(commandIndex, delCommand.getKey());
        setRemoved(removed, null);
    }

    @SuppressWarnings("unchecked")
    private <T> void setRemoved(SettableFuture<?> removed, T value) {
        SettableFuture<T> commandFuture = (SettableFuture<T>) removed;
        commandFuture.set(value);
    }

    //----------------------------------------------------------------------------------------------------------------//

    // client-issued

    public ListenableFuture<Void> nop() {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.NOPCommand nopCommand = new KayVeeCommand.NOPCommand(getCommandId());
        return issueCommandToCluster(nopCommand);
    }

    public ListenableFuture<KeyValue> get(String key) {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.GETCommand getCommand = new KayVeeCommand.GETCommand(getCommandId(), key);
        return issueCommandToCluster(getCommand);
    }

    public ListenableFuture<Collection<KeyValue>> getAll() {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.ALLCommand allCommand = new KayVeeCommand.ALLCommand(getCommandId());
        return issueCommandToCluster(allCommand);
    }

    public ListenableFuture<KeyValue> set(String key, SetValue setValue) {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.SETCommand setCommand = new KayVeeCommand.SETCommand(getCommandId(), key, setValue.getNewValue());
        return issueCommandToCluster(setCommand);
    }

    public ListenableFuture<KeyValue> compareAndSet(String key, SetValue setValue) {
        checkThatDistributedStoreIsActive();
        KayVeeCommand.CASCommand casCommand = new KayVeeCommand.CASCommand(getCommandId(), key, setValue.getExpectedValue(), setValue.getNewValue());
        return issueCommandToCluster(casCommand);
    }

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
            SettableFuture<?> previous = pendingCommands.put(kayVeeCommand.getCommandId(), returned);
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
