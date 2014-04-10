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

package io.libraft.agent.snapshots;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import io.libraft.Snapshot;
import io.libraft.agent.TestLoggingRule;
import io.libraft.algorithm.StorageException;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.libraft.algorithm.SnapshotsStore.ExtendedSnapshot;
import static io.libraft.algorithm.SnapshotsStore.ExtendedSnapshotWriter;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// I'm really sorry. I jumped the shark here :(
// this may test the class, but it is now highly coupled to the implementation and how many DAO calls are made, etc.
@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class OnDiskSnapshotsStoreTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(OnDiskSnapshotsStoreTest.class);

    private final class OneResultIterator implements ResultIterator<SnapshotMetadata> {

        private boolean hasValue = true;
        private SnapshotMetadata snapshotMetadata;

        @Override
        public void close() {
            hasValue = false;
            snapshotMetadata = null;
        }

        @Override
        public boolean hasNext() {
            if (hasValue) {
                hasValue = false;
                snapshotMetadata = new SnapshotMetadata("asdjkfas", System.currentTimeMillis(), 1, 1);
                return true;
            } else {
                snapshotMetadata = null;
                return false;
            }
        }

        @Override
        public SnapshotMetadata next() {
            SnapshotMetadata returned = snapshotMetadata;
            snapshotMetadata = null;
            return returned;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    // test with both sqlite and h2 to make sure the DAO SQL is compatible
    @Parameterized.Parameters
    public static Collection<Object[]> getDataSourceParameters() {
        return Arrays.asList(new Object[][] {
                {"org.h2.Driver", "jdbc:h2:mem:snaptest"},
                {"org.sqlite.JDBC", "jdbc:sqlite::memory:"}
        });
    }

    private static final int NUM_RANDOM_BYTES = 5 * 1024 * 1024;
    private static final String DIGEST_ALGORITHM = "MD5";

    private final Random random;
    private final String databaseClass;
    private final String databaseUrl;

    private final File snapshotsDirectory = com.google.common.io.Files.createTempDir();
    private DataSource dataSource;
    private DBI dbi;

    private OnDiskSnapshotsStore snapshotsStore; // SUT

    @Rule
    public final TestLoggingRule loggingRule = new TestLoggingRule(LOGGER);

    private interface SnapshotStoreMethodCallable {

        void setup(OnDiskSnapshotsStore onDiskSnapshotsStore, SnapshotsDAO mockDAO, Throwable toThrow) throws Exception;

        void runThrowingMethod(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception;
    }

    private interface SnapshotStoreMethodSimpleCallable {

        void setup(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception;

        void runThrowingMethod(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception;
    }

    public OnDiskSnapshotsStoreTest(String databaseClass, String databaseUrl) {
        long currentTime = System.currentTimeMillis();

        LOGGER.info("seed:{}", currentTime);

        this.random = new Random(currentTime);
        this.databaseClass = databaseClass;
        this.databaseUrl = databaseUrl;
    }

    @Before
    public void setup() throws SQLException, StorageException {
        dataSource = new DataSource();
        dataSource.setDriverClassName(databaseClass);
        dataSource.setUrl(databaseUrl);
        dataSource.createPool();

        dbi = new DBI(dataSource);

        snapshotsStore = new OnDiskSnapshotsStore(dbi, snapshotsDirectory.getAbsolutePath());
        snapshotsStore.initialize();
    }

    @After
    public void teardown() {
        snapshotsStore.teardown();
        dataSource.close();
    }

    //
    // functionality tests
    //

    @Test
    public void shouldThrowStorageExceptionIfDatabaseCallFailsInCallToInitialize() {
        // create a broken snapshotsStore that we will use to test the DB failure out on
        DBI brokenDBI = mock(DBI.class);
        Handle brokenHandle = mock(Handle.class);
        OnDiskSnapshotsStore brokenSnapshotStore = new OnDiskSnapshotsStore(brokenDBI, snapshotsDirectory.getAbsolutePath());

        // return the callback object but fail to do the attach!
        IllegalArgumentException failure = new IllegalArgumentException("failed!");
        when(brokenDBI.open()).thenReturn(brokenHandle);
        when(brokenDBI.withHandle(any(HandleCallback.class))).thenCallRealMethod(); // calling the real method will trigger the call to open() above and cause execute the 'catch' etc. logic
        when(brokenHandle.attach(any(Class.class))).thenThrow(failure);

        // attempt to initialize
        // we should throw an exception the moment we attempt to create the table
        boolean exceptionThrown = false;
        try {
            brokenSnapshotStore.initialize();
        } catch (StorageException e) {
            exceptionThrown = true;
            assertThat(e.getCause(), Matchers.<Throwable>sameInstance(failure)); // we shouldn't return the CallbackFailedException - instead the underlying exception
        }

        // the initialization should have failed
        assertThat(exceptionThrown, is(true));
    }

    @Test
    public void shouldThrowStorageExceptionIfExceptionThrownInCallToInitialize() {
        // create a broken snapshotsStore that we will use to test the DB failure out on
        DBI brokenDBI = mock(DBI.class);
        OnDiskSnapshotsStore brokenSnapshotStore = new OnDiskSnapshotsStore(brokenDBI, snapshotsDirectory.getAbsolutePath());

        // fail the moment a handle is requested
        IllegalArgumentException failure = new IllegalArgumentException("failed!");
        when(brokenDBI.open()).thenThrow(failure);
        when(brokenDBI.withHandle(any(HandleCallback.class))).thenCallRealMethod(); // calling the real method will trigger the call to open() above and cause execute the 'catch' etc. logic

        // attempt to initialize
        // we should throw an exception the moment we attempt to create the table
        boolean exceptionThrown = false;
        try {
            brokenSnapshotStore.initialize();
        } catch (StorageException e) {
            exceptionThrown = true;
            assertThat(e.getCause(), Matchers.<Throwable>sameInstance(failure));
        }

        // the initialization should have failed
        assertThat(exceptionThrown, is(true));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionIfSnapshotStoreInitializedMultipleTimes() throws StorageException {
        snapshotsStore.initialize();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfSnapshotsDirectoryDoesNotExist() throws StorageException {
        new OnDiskSnapshotsStore(mock(DBI.class), "doesNotExist");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfSnapshotsDirectoryIsAFile() throws StorageException, IOException {
        new OnDiskSnapshotsStore(mock(DBI.class), Files.createTempFile("tmp", "tmp").toAbsolutePath().toString());
    }

    @Test
    public void shouldStoreAndLoadSnapshot() throws StorageException, IOException, NoSuchAlgorithmException {
        long lastAppliedTerm = 3;
        long lastAppliedIndex = 1;
        byte[] md5Out = null;
        byte[] md5In = null;

        // create the data to snapshot
        byte[] snapshotBytes = new byte[NUM_RANDOM_BYTES];
        random.nextBytes(snapshotBytes);

        // write the data to the stream
        ExtendedSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        DigestOutputStream out = null;
        try {
            out = new DigestOutputStream(snapshotWriter.getSnapshotOutputStream(), MessageDigest.getInstance("MD5"));
            out.write(snapshotBytes);
            md5Out = out.getMessageDigest().digest();
        } finally {
            Closeables.close(out, false);
        }

        // add the fields for the snapshot
        snapshotWriter.setTerm(lastAppliedTerm);
        snapshotWriter.setIndex(lastAppliedIndex);

        // store the snapshot
        snapshotsStore.storeSnapshot(snapshotWriter);

        // now, get the latest snapshot (there should only be one)
        ExtendedSnapshot snapshot = snapshotsStore.getLatestSnapshot();
        if (snapshot == null) {
            throw new IllegalStateException("a snapshot should exist");
        }

        // check metadata
        assertThat(snapshot.getTerm(), equalTo(lastAppliedTerm));
        assertThat(snapshot.getIndex(), equalTo(lastAppliedIndex));

        // check the snapshot data itself
        DigestInputStream in = null;
        try {
            in = new DigestInputStream(snapshot.getSnapshotInputStream(), MessageDigest.getInstance(DIGEST_ALGORITHM));
            // noinspection StatementWithEmptyBody
            while (in.read() != -1);
            md5In = in.getMessageDigest().digest();
        } finally {
            Closeables.close(in, false);
        }

        assertThat(MessageDigest.isEqual(md5Out, md5In), is(true));
    }

    @Test
    public void shouldThrowStorageExceptionIfDatabaseCallFailsInCallToAddSnapshot() throws Exception {
        throwWhenDAOMethodCalled(new SnapshotStoreMethodCallable() {

            private ExtendedSnapshotWriter snapshotWriter;

            @Override
            public void setup(OnDiskSnapshotsStore onDiskSnapshotsStore, SnapshotsDAO mockDAO, Throwable toThrow) throws Exception {
                // create a snapshot
                snapshotWriter = onDiskSnapshotsStore.newSnapshotWriter();
                snapshotWriter.setTerm(7);
                snapshotWriter.setIndex(8);

                // write the snapshot
                OutputStream os = snapshotWriter.getSnapshotOutputStream();
                try {
                    os.write("TEST".getBytes(Charset.forName("ASCII")));
                } finally {
                    Closeables.close(os, true);
                }

                // setup our DAO to throw when we attempt to add a snapshot
                doThrow(toThrow).when(mockDAO).addSnapshot(any(SnapshotMetadata.class));
            }

            @Override
            public void runThrowingMethod(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception {
                checkNotNull(snapshotWriter);
                onDiskSnapshotsStore.storeSnapshot(snapshotWriter);
            }
        });
    }

    @Test
    public void shouldThrowStorageExceptionIfExceptionThrownInCallToAddSnapshot() throws Exception {
        throwOnDBIOpen(new SnapshotStoreMethodSimpleCallable() {

            private ExtendedSnapshotWriter snapshotWriter;

            @Override
            public void setup(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception {
                // create a snapshot
                snapshotWriter = onDiskSnapshotsStore.newSnapshotWriter();
                snapshotWriter.setTerm(7);
                snapshotWriter.setIndex(8);

                // write the snapshot
                OutputStream os = snapshotWriter.getSnapshotOutputStream();
                try {
                    os.write("TEST".getBytes(Charset.forName("ASCII")));
                } finally {
                    Closeables.close(os, true);
                }
            }

            @Override
            public void runThrowingMethod(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception {
                checkNotNull(snapshotWriter);
                onDiskSnapshotsStore.storeSnapshot(snapshotWriter);
            }
        });
    }

    @Test
    public void shouldThrowStorageExceptionIfSnapshotCannotBeStoredInSnapshotDirectory() throws IOException, StorageException {
        long snapshotTerm = 6;
        long snapshotIndex = 7;

        // start by getting a snapshot
        ExtendedSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setTerm(snapshotTerm);
        snapshotWriter.setIndex(snapshotIndex);

        // write the snapshot
        OutputStream os = snapshotWriter.getSnapshotOutputStream();
        try {
            os.write("TEST".getBytes(Charset.forName(Charsets.US_ASCII.toString())));
        } finally {
            Closeables.close(os, true);
        }

        // now, before you save the snapshot, delete the snapshot directory!
        boolean deleted = snapshotsDirectory.delete();
        assertThat(deleted, is(true));

        boolean exceptionThrown = false;
        try {
            // then, attempt to store the snapshot :)
            snapshotsStore.storeSnapshot(snapshotWriter);
        } catch (StorageException e) {
            exceptionThrown = true;
            assertThat(e.getCause(), instanceOf(IOException.class)); // should be an IOException because we couldn't save the file
        }

        // check that we actually threw the exception
        assertThat(exceptionThrown, is(true));

        // check that no metadata was stored in the database
        int entries = dbi.withHandle(new HandleCallback<Integer>() {
            @Override
            public Integer withHandle(Handle handle) throws Exception {
                SnapshotsDAO dao = handle.attach(SnapshotsDAO.class);
                return dao.getNumSnapshots();
            }
        });

        assertThat(entries, is(0));
    }

    @Test
    public void shouldReturnNullIfNoSnapshotsExist() throws StorageException {
        Snapshot snapshot = snapshotsStore.getLatestSnapshot();
        assertThat(snapshot, nullValue());
    }

    @Test
    public void shouldReturnTheLatestSnapshotIfSnapshotsExist() throws StorageException, IOException, InterruptedException {
        createSnapshots(10);

        // by the contract above, this is the data we should have
        long finalSnapshotTerm = 10;
        long finalSnapshotIndex = 10;
        byte[] finalSnapshotData = Integer.toString(10).getBytes(Charsets.US_ASCII.toString());

        // ok, now let's ask for the latest snapshot
        ExtendedSnapshot snapshot = snapshotsStore.getLatestSnapshot();
        if (snapshot == null) {
            throw new IllegalStateException("a snapshot should exist");
        }

        // check the term and index
        assertThat(snapshot.getTerm(), equalTo(finalSnapshotTerm));
        assertThat(snapshot.getIndex(), equalTo(finalSnapshotIndex));

        // read the data from the snapshot
        InputStream is = null;
        try {
            is = snapshot.getSnapshotInputStream();

            int bytesAvailable = is.available();
            byte[] buffer = new byte[bytesAvailable];

            int read = is.read(buffer);
            assertThat(read, equalTo(bytesAvailable));
            assertThat(Arrays.equals(buffer, finalSnapshotData), is(true));
        } finally {
            Closeables.close(is, true);
        }
    }

    @Test
    public void shouldThrowStorageExceptionIfSnapshotIsMissingFromDisk() throws IOException, StorageException {
        ExtendedSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();

        // write the snapshot
        OutputStream os = null;
        try {
            os = snapshotWriter.getSnapshotOutputStream();
            os.write("TEST".getBytes(Charset.forName(Charsets.US_ASCII.toString())));

            snapshotWriter.setTerm(1);
            snapshotWriter.setIndex(1);

            snapshotsStore.storeSnapshot(snapshotWriter);
        } finally {
            Closeables.close(os, true);
        }

        // access the db directly, find the filename of the snapshot and delete the file!
        // FIXME (AG): this is vulnerable to the table schema changing
        String filename = dbi.withHandle(new HandleCallback<String>() {
            @Override
            public String withHandle(Handle handle) throws Exception {
                List<Map<String, Object>> values = handle.select("select filename from snapshots where ts = (select max(ts) from snapshots)");

                assertThat(values, hasSize(1));

                return (String) values.get(0).values().iterator().next();
            }
        });

        // check that the snapshot file exists
        File snapshotFile = new File(snapshotsDirectory, filename);
        assertThat(snapshotFile.exists(), is(true));

        // delete the file!
        boolean deleted = snapshotFile.delete();
        assertThat(deleted, equalTo(true));

        // now, use the snapshotsStore to access the latest snapshot - this should trigger the error
        boolean exceptionCaught = false;
        try {
            snapshotsStore.getLatestSnapshot(); // don't care about the return
        } catch (StorageException e) {
            exceptionCaught = true;
            assertThat(e.getCause(), Matchers.instanceOf(IOException.class));
        }

        // check that we actually threw an exception
        assertThat(exceptionCaught, is(true));
    }

    @Test
    public void shouldThrowStorageExceptionIfDatabaseCallFailsInCallToGetLatestSnapshot() throws Exception {
        throwWhenDAOMethodCalled(new SnapshotStoreMethodCallable() {
            @Override
            public void setup(OnDiskSnapshotsStore onDiskSnapshotsStore, SnapshotsDAO mockDAO, Throwable toThrow) throws Exception {
                when(mockDAO.getLatestSnapshot()).thenThrow(toThrow);
            }

            @Override
            public void runThrowingMethod(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception {
                onDiskSnapshotsStore.getLatestSnapshot();
            }
        });
    }

    @Test
    public void shouldThrowStorageExceptionIfExceptionThrownInCallToGetLatestSnapshot() throws Exception {
        throwOnDBIOpen(new SnapshotStoreMethodSimpleCallable() {

            @Override
            public void setup(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception {
                // noop
            }

            @Override
            public void runThrowingMethod(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception {
                onDiskSnapshotsStore.getLatestSnapshot();
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfPruneCallIsMadeWithInvalidNumSnapshotsToKeep() throws StorageException {
        List<SnapshotMetadata> snapshotsBeforePrune = snapshotsStore.getAllSnapshotsFromLatestToOldest();
        assertThat(snapshotsBeforePrune, emptyCollectionOf(SnapshotMetadata.class));

        snapshotsStore.pruneSnapshots(-1);
    }

    @Test
    public void shouldNoopIfNoSnapshotsExistToBePruned() throws StorageException {
        List<SnapshotMetadata> snapshotsBeforePrune = snapshotsStore.getAllSnapshotsFromLatestToOldest();
        assertThat(snapshotsBeforePrune, emptyCollectionOf(SnapshotMetadata.class));

        snapshotsStore.pruneSnapshots(0);

        List<SnapshotMetadata> snapshotsAfterPrune = snapshotsStore.getAllSnapshotsFromLatestToOldest();
        assertThat(snapshotsAfterPrune, emptyCollectionOf(SnapshotMetadata.class));
    }

    @Test
    public void shouldNoopIfNoSnapshotsCanBePruned() throws IOException, StorageException, InterruptedException {
        final int numSnapshots = 3;

        // create the snapshots
        createSnapshots(numSnapshots);

        // iterate and check that they're there in the db and the filesystem
        List<SnapshotMetadata> snapshotsBeforePrune = snapshotsStore.getAllSnapshotsFromLatestToOldest();
        assertThat(snapshotsBeforePrune, hasSize(numSnapshots));
        checkSnapshotsExistOnFilesystem(snapshotsBeforePrune);

        // now, run the prune (notice that we specify exactly the number of snapshots that exist!)
        snapshotsStore.pruneSnapshots(numSnapshots);

        // check again that the snapshots are still there in the DB and the filesystem
        List<SnapshotMetadata> snapshotsAfterPrune = snapshotsStore.getAllSnapshotsFromLatestToOldest();
        assertThat(snapshotsAfterPrune, hasSize(numSnapshots));
        assertThat(snapshotsAfterPrune, containsInAnyOrder(snapshotsBeforePrune.toArray()));
        checkSnapshotsExistOnFilesystem(snapshotsAfterPrune);
    }

    @Test
    public void shouldNoopIfNumSnapshotsToBePrunedGreaterThanNumSnapshots() throws IOException, StorageException, InterruptedException {
        final int numSnapshots = 3;

        // create the snapshots
        createSnapshots(numSnapshots);

        // iterate and check that they're there in the db and the filesystem
        List<SnapshotMetadata> snapshotsBeforePrune = snapshotsStore.getAllSnapshotsFromLatestToOldest();
        assertThat(snapshotsBeforePrune, hasSize(numSnapshots));
        checkSnapshotsExistOnFilesystem(snapshotsBeforePrune);

        // now, run the prune (notice that we specify much, much more than the snapshots that exist)
        int numSnapshotsToKeep = 1000;
        assertThat(numSnapshots, lessThan(numSnapshotsToKeep));
        snapshotsStore.pruneSnapshots(numSnapshotsToKeep);

        // check again that the snapshots are still there in the DB and the filesystem
        List<SnapshotMetadata> snapshotsAfterPrune = snapshotsStore.getAllSnapshotsFromLatestToOldest();
        assertThat(snapshotsAfterPrune, hasSize(numSnapshots));
        assertThat(snapshotsAfterPrune, containsInAnyOrder(snapshotsBeforePrune.toArray()));
        checkSnapshotsExistOnFilesystem(snapshotsAfterPrune);
    }

    @Test
    public void shouldDeleteExtraneousSnapshotsWhenPruneCallMade() throws IOException, StorageException, InterruptedException {
        // create more snapshots than we actually want
        int numSnapshots = 10;
        createSnapshots(numSnapshots);

        // check that they're there in the database
        List<SnapshotMetadata> snapshotsBeforePrune = snapshotsStore.getAllSnapshotsFromLatestToOldest();
        assertThat(snapshotsBeforePrune, hasSize(numSnapshots));

        // check there there are indeed those snapshots on the filesystem
        checkSnapshotsExistOnFilesystem(snapshotsBeforePrune);

        // alright - now, let's prune these guys
        int numSnapshotsToKeep = 4;
        snapshotsStore.pruneSnapshots(numSnapshotsToKeep);
        List<SnapshotMetadata> snapshotsAfterPrune = snapshotsStore.getAllSnapshotsFromLatestToOldest();

        // check that the DB only has the snapshots we're supposed to keep
        assertThat(snapshotsAfterPrune, hasSize(numSnapshotsToKeep));
        assertThat(snapshotsAfterPrune, contains(snapshotsAfterPrune.subList(0, numSnapshotsToKeep).toArray()));

        // check that the filesystem no longer has the deleted snapshots
        for (SnapshotMetadata metadata : snapshotsBeforePrune.subList(numSnapshots, snapshotsBeforePrune.size())) {
            assertThat(snapshotExistsOnFilesystem(snapshotsDirectory, metadata.getFilename()), is (false));
        }
    }

    @Test
    public void shouldPruneSnapshotMetadataEvenIfSnapshotCannotBeDeletedFromDisk() throws IOException, StorageException, InterruptedException {
        // create more snapshots than we actually want
        int numSnapshots = 10;
        createSnapshots(numSnapshots);

        // check that they're there in the database
        List<SnapshotMetadata> snapshotsBeforePrune = snapshotsStore.getAllSnapshotsFromLatestToOldest();
        assertThat(snapshotsBeforePrune, hasSize(numSnapshots));

        // check there there are indeed those snapshots on the filesystem
        checkSnapshotsExistOnFilesystem(snapshotsBeforePrune);

        // we only want to keep 5 snapshots
        int numSnapshotsToKeep = 4;

        // now...let's make one of the snapshots undeletable by...deleting it ourself :)
        Path snapshotPath = getSnapshotPath(snapshotsDirectory, snapshotsBeforePrune.get(4).getFilename());
        boolean deleted = Files.deleteIfExists(snapshotPath);
        assertThat(deleted, is(true));

        // alright - now, let's prune these guys
        // nothing should fail!
        snapshotsStore.pruneSnapshots(numSnapshotsToKeep);
        List<SnapshotMetadata> snapshotsAfterPrune = snapshotsStore.getAllSnapshotsFromLatestToOldest();

        // check that the DB only has the snapshots we're supposed to keep
        assertThat(snapshotsAfterPrune, hasSize(numSnapshotsToKeep));
        assertThat(snapshotsAfterPrune, contains(snapshotsAfterPrune.subList(0, numSnapshotsToKeep).toArray()));

        // check that the filesystem no longer has the deleted snapshots
        for (SnapshotMetadata metadata : snapshotsBeforePrune.subList(numSnapshots, snapshotsBeforePrune.size())) {
            assertThat(snapshotExistsOnFilesystem(snapshotsDirectory, metadata.getFilename()), is (false));
        }
    }

    @Test
    public void shouldThrowStorageExceptionIfDatabaseCallFailsInCallToPruneSnapshots1() throws Exception {

        throwWhenDAOMethodCalled(new SnapshotStoreMethodCallable() {

            @Override
            public void setup(OnDiskSnapshotsStore onDiskSnapshotsStore, SnapshotsDAO mockDAO, Throwable toThrow) throws Exception {
                when(mockDAO.getAllSnapshotsFromLatestToOldest()).thenThrow(toThrow); // we throw even before making the actual remove DB call
            }

            @Override
            public void runThrowingMethod(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception {
                onDiskSnapshotsStore.pruneSnapshots(0);
            }
        });
    }

    @Test
    public void shouldThrowStorageExceptionIfDatabaseCallFailsInCallToPruneSnapshots2() throws Exception {

        throwWhenDAOMethodCalled(new SnapshotStoreMethodCallable() {

            @Override
            public void setup(OnDiskSnapshotsStore onDiskSnapshotsStore, SnapshotsDAO mockDAO, Throwable toThrow) throws Exception {
                when(mockDAO.getAllSnapshotsFromLatestToOldest()).thenReturn(new OneResultIterator()); // we have one result
                doThrow(toThrow).when(mockDAO).removeSnapshotWithTimestamp(anyLong());
            }

            @Override
            public void runThrowingMethod(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception {
                onDiskSnapshotsStore.pruneSnapshots(0);
            }
        });
    }

    @Test
    public void shouldReturnSnapshotsInDescendingTimestampOrder() throws InterruptedException, StorageException, IOException {
        // create a large number of snapshots
        int numSnapshots = 20;
        createSnapshots(numSnapshots);

        // check that they're there in the database
        List<SnapshotMetadata> snapshots = snapshotsStore.getAllSnapshotsFromLatestToOldest();
        assertThat(snapshots, hasSize(numSnapshots));

        // check there there are indeed those snapshots on the filesystem
        checkSnapshotsExistOnFilesystem(snapshots);

        // now, let's loop through and see if they're ordered correctly
        long prevTimestamp = Long.MAX_VALUE;
        for (SnapshotMetadata metadata : snapshots) {
            assertThat(prevTimestamp, greaterThan(metadata.getTimestamp()));
            prevTimestamp = metadata.getTimestamp();
        }
    }

    @Test
    public void shouldReturnEmptyListWhenNoSnapshotsExist() throws InterruptedException, StorageException, IOException {
        List<SnapshotMetadata> snapshots = snapshotsStore.getAllSnapshotsFromLatestToOldest();
        assertThat(snapshots, empty());
    }

    @Test
    public void shouldThrowStorageExceptionIfDatabaseCallFailsInCallToGetAllSnapshots() throws Exception {
        throwWhenDAOMethodCalled(new SnapshotStoreMethodCallable() {
            @Override
            public void setup(OnDiskSnapshotsStore onDiskSnapshotsStore, SnapshotsDAO mockDAO, Throwable toThrow) throws Exception {
                when(mockDAO.getAllSnapshotsFromLatestToOldest()).thenThrow(toThrow);
            }

            @Override
            public void runThrowingMethod(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception {
                onDiskSnapshotsStore.getAllSnapshotsFromLatestToOldest();
            }
        });
    }

    @Test
    public void shouldThrowStorageExceptionIfExceptionThrownInCallToGetAllSnapshots() throws Exception {
        throwOnDBIOpen(new SnapshotStoreMethodSimpleCallable() {

            @Override
            public void setup(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception {
                // noop
            }

            @Override
            public void runThrowingMethod(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception {
                onDiskSnapshotsStore.getAllSnapshotsFromLatestToOldest();
            }
        });
    }

    @Test
    public void shouldDeleteSnapshotMetadataFromDatabaseIfSnapshotDoesNotExistOnFilesystem() throws InterruptedException, StorageException, IOException {
        // create a large number of snapshots
        int numSnapshotsBeforeReconcile = 20;
        createSnapshots(numSnapshotsBeforeReconcile);

        // check that they're there in the database
        List<SnapshotMetadata> snapshotsBeforeReconcile = snapshotsStore.getAllSnapshotsFromLatestToOldest();
        assertThat(snapshotsBeforeReconcile, hasSize(numSnapshotsBeforeReconcile));

        // check there there are indeed those snapshots on the filesystem
        checkSnapshotsExistOnFilesystem(snapshotsBeforeReconcile);

        // pick 5 random snapshots for which we shall delete the snapshot file
        int i = 0;
        int numBrokenSnapshots = 5;
        Set<SnapshotMetadata> brokenSnapshots = Sets.newHashSetWithExpectedSize(numBrokenSnapshots);
        while (i < numBrokenSnapshots) {
            // store the metadata
            SnapshotMetadata metadata = snapshotsBeforeReconcile.get(random.nextInt(numSnapshotsBeforeReconcile));
            boolean added = brokenSnapshots.add(metadata);

            // if this was a unique snapshot, delete the file and increment the counter
            if (added) {
                boolean deleted = Files.deleteIfExists(getSnapshotPath(snapshotsDirectory, metadata.getFilename()));
                assertThat(deleted, is(true));
                i++;
            }
        }

        // now...reconcile the snapshots
        snapshotsStore.reconcileSnapshots();

        // let's get the new list of snapshots
        List<SnapshotMetadata> snapshotsAfterReconcile = snapshotsStore.getAllSnapshotsFromLatestToOldest();
        assertThat(snapshotsAfterReconcile, hasSize(numSnapshotsBeforeReconcile - numBrokenSnapshots));

        // check there there are indeed those snapshots on the filesystem
        checkSnapshotsExistOnFilesystem(snapshotsAfterReconcile);

        // and verify that none of the deleted snapshots exists in the new set
        for (SnapshotMetadata missing : brokenSnapshots) {
            assertThat(snapshotsAfterReconcile.contains(missing), is(false));
        }
    }

    @Test
    public void shouldThrowStorageExceptionIfDatabaseCallFailsInCallToReconcileSnapshots1() throws Exception {

        throwWhenDAOMethodCalled(new SnapshotStoreMethodCallable() {

            @Override
            public void setup(OnDiskSnapshotsStore onDiskSnapshotsStore, SnapshotsDAO mockDAO, Throwable toThrow) throws Exception {
                when(mockDAO.getAllSnapshotsFromLatestToOldest()).thenThrow(toThrow); // we throw even before making the actual remove DB call
            }

            @Override
            public void runThrowingMethod(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception {
                onDiskSnapshotsStore.reconcileSnapshots();
            }
        });
    }

    @Test
    public void shouldThrowStorageExceptionIfDatabaseCallFailsInCallToReconcileSnapshots2() throws Exception {

        throwWhenDAOMethodCalled(new SnapshotStoreMethodCallable() {

            @Override
            public void setup(OnDiskSnapshotsStore onDiskSnapshotsStore, SnapshotsDAO mockDAO, Throwable toThrow) throws Exception {
                when(mockDAO.getAllSnapshotsFromLatestToOldest()).thenReturn(new OneResultIterator()); // we have one result
                doThrow(toThrow).when(mockDAO).removeSnapshotWithTimestamp(anyLong());
            }

            @Override
            public void runThrowingMethod(OnDiskSnapshotsStore onDiskSnapshotsStore) throws Exception {
                onDiskSnapshotsStore.reconcileSnapshots();
            }
        });
    }

    //
    // utility functions
    //

    private void throwWhenDAOMethodCalled(SnapshotStoreMethodCallable callable) throws Exception {
        // create a broken snapshotsStore that we will use to test the DB failure out on
        DBI mockDBI = mock(DBI.class);
        Handle mockHandle = mock(Handle.class);
        SnapshotsDAO mockDAO = mock(SnapshotsDAO.class);
        OnDiskSnapshotsStore brokenSnapshotStore = new OnDiskSnapshotsStore(mockDBI, snapshotsDirectory.getAbsolutePath());

        // basic setup:
        // - return the mock handle for every open() call
        // - call the real "withHandle" method for any handle callback
        // - return the mock dao for every attach
        // - ensure that we do nothing for the initialization
        when(mockDBI.open()).thenReturn(mockHandle);
        when(mockDBI.withHandle(any(HandleCallback.class))).thenCallRealMethod(); // calling the real method will trigger the call to open() above and cause execute the 'catch' etc. logic
        when(mockHandle.attach(SnapshotsDAO.class)).thenReturn(mockDAO);
        doNothing().when(mockDAO).createSnapshotsTableWithIndex();

        // initialize
        brokenSnapshotStore.initialize();

        // run any tasks the caller wants before the method to test
        Throwable toThrow = new IllegalArgumentException("bad argument - very boring");
        callable.setup(brokenSnapshotStore, mockDAO, toThrow);

        // run the method we want to test
        boolean exceptionThrown = false;
        try {
            callable.runThrowingMethod(brokenSnapshotStore);
        } catch (StorageException e) {
            exceptionThrown = true;
            assertThat(e.getCause(), sameInstance(toThrow)); // we shouldn't return the CallbackFailedException - instead the underlying exception
        }

        // we should have thrown the exception
        assertThat(exceptionThrown, is(true));
    }

    private void throwOnDBIOpen(SnapshotStoreMethodSimpleCallable callable) throws Exception {
        // create a broken snapshotsStore that we will use to test the DB failure out on
        DBI brokenDBI = mock(DBI.class);
        Handle mockHandle = mock(Handle.class);
        SnapshotsDAO mockDAO = mock(SnapshotsDAO.class);
        OnDiskSnapshotsStore brokenSnapshotStore = new OnDiskSnapshotsStore(brokenDBI, snapshotsDirectory.getAbsolutePath());

        // basic setup:
        // - return the mock handle for the first open() call (i.e. initialize)
        // - throw on subsequent calls
        // - call the real "withHandle" method for any handle callback
        // - return the mock dao for every attach (should only be one attach)
        // - ensure that we do nothing for the initialization
        IllegalArgumentException failure = new IllegalArgumentException("failed!");
        when(brokenDBI.open()).thenReturn(mockHandle).thenThrow(failure); // first return the real handle to be used in initialization, and then throw when the tested method is called
        when(brokenDBI.withHandle(any(HandleCallback.class))).thenCallRealMethod(); // calling the real method will trigger the call to open() above and cause execute the 'catch' etc. logic
        when(mockHandle.attach(SnapshotsDAO.class)).thenReturn(mockDAO);
        doNothing().when(mockDAO).createSnapshotsTableWithIndex();

        // initialize
        brokenSnapshotStore.initialize();

        // run any tasks the caller wants before the method to test
        callable.setup(brokenSnapshotStore);

        // run the method we want to test
        boolean exceptionThrown = false;
        try {
            callable.runThrowingMethod(brokenSnapshotStore);
        } catch (StorageException e) {
            exceptionThrown = true;
            assertThat(e.getCause(), Matchers.<Throwable>sameInstance(failure)); // we shouldn't return the CallbackFailedException - instead the underlying exception
        }

        // we should have thrown the exception
        assertThat(exceptionThrown, is(true));
    }

    // content of the last snapshot = numSnapshots
    // term of last snapshot = numSnapshots
    // index of lastSnapshot = numSnapshots
    private void createSnapshots(int numSnapshots) throws IOException, StorageException, InterruptedException {
        for (int i = 0; i < numSnapshots; i++) {
            // new snapshot snapshotWriter
            ExtendedSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();

            // write the snapshot
            OutputStream os = null;
            try {
                os = snapshotWriter.getSnapshotOutputStream();
                os.write(Integer.toString(i + 1).getBytes(Charset.forName(Charsets.US_ASCII.toString())));
            } finally {
                Closeables.close(os, true);
            }

            // write the term/index
            snapshotWriter.setTerm(i + 1);
            snapshotWriter.setIndex(i + 1);

            // finalize the snapshot
            snapshotsStore.storeSnapshot(snapshotWriter);

            Thread.sleep(20); // FIXME (AG): we have to sleep between each snapshot because the timestamp is supposed to be unique :(
        }
    }

    private void checkSnapshotsExistOnFilesystem(List<SnapshotMetadata> allSnapshots) {
        for (SnapshotMetadata metadata : allSnapshots) {
            assertThat(snapshotExistsOnFilesystem(snapshotsDirectory, metadata.getFilename()), is(true));
        }
    }

    private static boolean snapshotExistsOnFilesystem(File snapshotsDirectory, String snapshotFilename) {
        Path snapshotPath = getSnapshotPath(snapshotsDirectory, snapshotFilename);
        return Files.exists(snapshotPath);
    }

    private static Path getSnapshotPath(File snapshotsDirectory, String snapshotFilename) {
        return Paths.get(snapshotsDirectory.getAbsolutePath(), snapshotFilename);
    }
}
