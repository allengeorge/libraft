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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.libraft.Committed;
import io.libraft.RaftListener;
import io.libraft.Snapshot;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static io.libraft.algorithm.StoringSender.SnapshotChunk;
import static io.libraft.algorithm.UnitTestLogEntries.NOOP;
import static io.libraft.algorithm.UnitTestLogEntries.SENTINEL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
public final class SnapshotChunkRandomizedInputTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotChunkRandomizedInputTest.class);

    private static final int NUM_RANDOMIZED_ITERATIONS = 50;

    private static final String SELF = "S_00";
    private static final String LEADER = "S_03";
    private static final Set<String> CLUSTER = ImmutableSet.of(SELF, "S_01", "S_02", LEADER, "S_04");

    private static final long CURRENT_TERM = 9;
    private static final long INITIAL_COMMIT_INDEX = 5;

    private static final int CHUNK_SIZE = 4096;
    private static final int NUM_SNAPSHOT_CHUNKS = 10;
    private static final int NUM_SNAPSHOT_CHUNK_MESSAGES = 300;

    private static final long SNAPSHOT_TERM = 8;
    private static final long SNAPSHOT_INDEX = 11;

    private static final LogEntry[] SELF_LOG = {
            SENTINEL(),
            NOOP(1, 1),
            NOOP(1, 2),
            NOOP(2, 3),
            NOOP(2, 4),
            NOOP(2, 5),
            NOOP(2, 6),
            NOOP(3, 7),
            NOOP(3, 8),
            NOOP(3, 9),
            NOOP(4, 10),
            NOOP(4, 11)
    };

    private static final String DIGEST_ALGORITHM = "MD5";

    private final byte[][] chunks = new byte[NUM_SNAPSHOT_CHUNKS][];
    private byte[] digest;

    @Parameterized.Parameters
    public static java.util.Collection<Object[]> generateRandomSeeds() {
        int testIterations = NUM_RANDOMIZED_ITERATIONS;

        Random random = new Random(System.nanoTime());
        List<Object[]> randomSeeds = Lists.newArrayListWithCapacity(testIterations);
        for (int i = 0; i < testIterations; i++) {
            randomSeeds.add(new Object[]{random.nextInt()});
        }

        return randomSeeds;
    }

    private final Random random;

    private final int randomSeed;
    private final Random raftAlgorithmRandom;
    private final Timer timer;
    private final RPCSender sender;
    private final Store store;
    private final Log log;
    private final SnapshotsStore snapshotsStore;
    private final RaftListener listener;

    private RaftAlgorithm algorithm;

    @Rule
    public final TestRule randomizedTestRule = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            LOGGER.info("random seed:{}", randomSeed);
            super.starting(description);
        }
    };

    public SnapshotChunkRandomizedInputTest(int randomSeed) {
        this.randomSeed = randomSeed;

        // create the Random that's used to randomize the input messages
        random = new Random(randomSeed);

        // create all objects required by algorithm
        raftAlgorithmRandom = new Random(random.nextLong());
        timer = new UnitTestTimer();
        sender = new StoringSender();
        store = spy(new InMemoryStore());
        log = spy(new InMemoryLog());
        snapshotsStore = spy(new TempFileSnapshotsStore(Files.createTempDir()));
        listener = mock(RaftListener.class);
    }

    @Before
    public void setup() throws Exception {
        DigestOutputStream digestOutputStream = new DigestOutputStream(ByteStreams.nullOutputStream(), MessageDigest.getInstance(DIGEST_ALGORITHM));

        // setup the snapshot chunks that
        // will be transferred from the leader to SELF
        for (int i = 0; i < NUM_SNAPSHOT_CHUNKS; i++) {
            // create the chunk
            byte[] chunk = new byte[CHUNK_SIZE];
            random.nextBytes(chunk);
            chunks[i] = chunk;

            // digest it
            digestOutputStream.write(chunk);
        }

        // calculate the digest for the chunks
        digest = digestOutputStream.getMessageDigest().digest();

        // setup starting state for SELF
        // done here instead of constructor because I don't believe in throwing inside a constructor
        store.setCommitIndex(INITIAL_COMMIT_INDEX);
        store.setCurrentTerm(CURRENT_TERM);
        store.setVotedFor(CURRENT_TERM, LEADER);

        // setup the log (no snapshots)
        for (LogEntry entry : SELF_LOG) {
            log.put(entry);
        }

        // create the algorithm instance
        algorithm = new RaftAlgorithm(raftAlgorithmRandom, timer, sender, store, log, snapshotsStore, listener, SELF, CLUSTER);
        algorithm.initialize();
        algorithm.start();
    }

    @Test
    public void shouldConsumeSnapshotChunksUntilACompleteSnapshotIsAssembledAndApplied() throws Exception {
        // create the snapshot chunks in order from 0 -> last
        List<SnapshotChunk> snapshotChunkRequests = generateSnapshotChunks();

        // now pump them in random order into the algorithm
        for (int i = 0; i < NUM_SNAPSHOT_CHUNK_MESSAGES; i++) {
            SnapshotChunk snapshotChunk = snapshotChunkRequests.get(random.nextInt(snapshotChunkRequests.size()));
            algorithm.onSnapshotChunk(snapshotChunk.server, snapshotChunk.term, snapshotChunk.snapshotTerm, snapshotChunk.snapshotIndex, snapshotChunk.seqnum, snapshotChunk.chunkInputStream);
        }

        // check the final state
        ArgumentCaptor<Committed> committedCaptor = ArgumentCaptor.forClass(Committed.class);

        // we attempt to truncate the log
        verify(log).removeSuffix(INITIAL_COMMIT_INDEX + 1);

        // we attempt to apply the snapshot
        verify(listener).applyCommitted(committedCaptor.capture());

        // the snapshot is correct
        assertThat(committedCaptor.getValue(), instanceOf(Snapshot.class));
        Snapshot snapshot = (Snapshot) committedCaptor.getValue();
        DigestInputStream snapshotInputStream = new DigestInputStream(snapshot.getSnapshotInputStream(), MessageDigest.getInstance(DIGEST_ALGORITHM));
        ByteStreams.readFully(snapshotInputStream, new byte[NUM_SNAPSHOT_CHUNKS * CHUNK_SIZE]);
        assertThat(snapshotInputStream.getMessageDigest().digest(), equalTo(digest));

        // we set the commit index to the snapshot index
        verify(store).setCommitIndex(SNAPSHOT_INDEX);

        // there should be nothing in the log
        assertThat(log.getLast(), nullValue());

        // our term and commit index should be correct
        assertThat(store.getCurrentTerm(), equalTo(CURRENT_TERM));
        assertThat(store.getCommitIndex(), equalTo(SNAPSHOT_INDEX));
    }

    private List<SnapshotChunk> generateSnapshotChunks() {
        List<SnapshotChunk> snapshotChunks = Lists.newArrayListWithCapacity(NUM_SNAPSHOT_CHUNKS + 1); // number of snapshot chunks plus the last null chunk

        // all except the last chunk
        for (int i = 0; i < NUM_SNAPSHOT_CHUNKS; i++) {
            snapshotChunks.add(new SnapshotChunk(LEADER, CURRENT_TERM, SNAPSHOT_TERM, SNAPSHOT_INDEX, i, new ByteArrayInputStream(chunks[i])));
        }

        // last chunk - null
        snapshotChunks.add(new SnapshotChunk(LEADER, CURRENT_TERM, SNAPSHOT_TERM, SNAPSHOT_INDEX, NUM_SNAPSHOT_CHUNKS, null));

        return snapshotChunks;
    }
}
