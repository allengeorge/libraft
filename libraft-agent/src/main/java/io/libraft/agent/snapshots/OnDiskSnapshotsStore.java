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

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import io.libraft.algorithm.SnapshotsStore;
import io.libraft.algorithm.StorageException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

// FIXME (AG): consider using only files on disk for snapshot metadata (reduce mapping maintenance hassles)
/**
 * An implementation of {@code SnapshotStore} that stores snapshots
 * on the filesystem and snapshot metadata in a JDBC-compliant database.
 * Snapshots are represented as a combination of:
 * <ul>
 *     <li>Snapshot metadata.</li>
 *     <li>Serialized application state stored in a snapshot file.</li>
 * </ul>
 * Snapshot metadata is represented using a {@link SnapshotMetadata}
 * object and stored in the snapshots database via a {@link SnapshotsDAO}.
 * Serialized application state is written in an application-defined
 * format using a {@link SnapshotFileWriter} and read using a {@link SnapshotFileReader}.
 * <p/>
 * This implementation treats the <strong>snapshot metadata</strong>
 * stored in the snapshots database as the canonical source of snapshot
 * information. As a result it is possible for the database and the
 * filesystem to disagree (for example, if the database has a metadata
 * entry for a snapshot but the corresponding snapshot file does not exist).
 * Users <strong>should</strong> call {@link OnDiskSnapshotsStore#reconcileSnapshots()}
 * periodically to clean out any metadata entries for which the
 * corresponding snapshot file does not exist or is inaccessible.
 * <p/>
 * With the exception of {@link OnDiskSnapshotsStore#initialize()} and
 * {@link OnDiskSnapshotsStore#teardown()} all methods in this class are thread-safe.
 *
 * @see SnapshotMetadata
 * @see SnapshotsDAO
 * @see SnapshotFileReader
 * @see SnapshotFileWriter
 */
public final class OnDiskSnapshotsStore implements SnapshotsStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(OnDiskSnapshotsStore.class);

    private static final Predicate<SnapshotMetadata> MATCH_EVERYTHING_PREDICATE = new Predicate<SnapshotMetadata>() {
        @Override
        public boolean apply(@Nullable SnapshotMetadata input) {
            return true;
        }
    };

    private final DBI dbi;
    private final String snapshotsDirectory;

    private volatile boolean initialized;

    /**
     * Constructor.
     *
     * @param dbi initialized instance of {@code DBI} through which all accesses to the snapshot-metadata database are made
     * @param snapshotsDirectory directory to which all snapshots are written and from which all snapshots are read
     */
    public OnDiskSnapshotsStore(DBI dbi, String snapshotsDirectory) {
        checkValidSnapshotsDirectory(snapshotsDirectory);

        this.dbi = dbi;
        this.snapshotsDirectory = snapshotsDirectory;
    }

    private void checkValidSnapshotsDirectory(String path) {
        File file = new File(path);

        checkArgument(file.exists(), "%s does not exist", snapshotsDirectory);
        checkArgument(file.isDirectory(), "%s is not a directory", snapshotsDirectory);
        checkArgument(file.canRead(), "%s is not readable", snapshotsDirectory);
        checkArgument(file.canWrite(), "%s is not writable", snapshotsDirectory);
        checkArgument(file.canExecute(), "%s is not executable", snapshotsDirectory);
    }

    /**
     * Initialize this component.
     * <p/>
     * This method must be called before any other methods in this component.
     * Once successful, subsequent calls to this method will throw an {@link IllegalStateException}.
     * <p/>
     * This method is <strong>not</strong> thread-safe. It cannot be
     * called simultaneously by multiple threads. Doing so will result
     * in undefined behavior.
     *
     * @throws StorageException if this component cannot be initialized. If this exception is thrown this component
     * is in a <strong>undefined</strong> state and cannot be used safely.
     */
    public void initialize() throws StorageException {
        checkState(!initialized, "store already initialized");

        try {
            dbi.withHandle(new HandleCallback<Void>() {
                @Override
                public Void withHandle(Handle handle) throws Exception {
                    SnapshotsDAO dao = handle.attach(SnapshotsDAO.class);
                    dao.createSnapshotsTableWithIndex();
                    return null;
                }
            });

            initialized = true;
        } catch (CallbackFailedException e) {
            throw new StorageException("fail create snapshots table", e.getCause());
        } catch (Exception e) {
            throw new StorageException("fail create snapshots table", e);
        }
    }

    /**
     * Teardown this component.
     * </p>
     * Once successful, subsequent calls to any other method in this component will throw an {@link IllegalStateException}.
     * <p/>
     * This method is <strong>not</strong> thread-safe. It cannot be
     * called simultaneously by multiple threads. Doing so will result
     * in undefined behavior.
     */
    public void teardown() {
        checkInitialized();

        initialized = false;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if this method is called before {@link OnDiskSnapshotsStore#initialize()}
     */
    @Override
    public ExtendedSnapshotWriter newSnapshotWriter() throws StorageException {
        checkInitialized();

        try {
            return new SnapshotFileWriter(snapshotsDirectory);
        } catch (Exception e) {
            throw new StorageException("fail create snapshot request", e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if this method is called before {@link OnDiskSnapshotsStore#initialize()}
     */
    @SuppressWarnings("ConstantConditions")
    @Override
    public void storeSnapshot(ExtendedSnapshotWriter snapshotWriter) throws StorageException {
        checkInitialized();

        checkArgument(snapshotWriter instanceof SnapshotFileWriter, "unknown snapshot request type:%s", snapshotWriter.getClass().getSimpleName());

        final SnapshotFileWriter snapshotFileWriter = (SnapshotFileWriter) snapshotWriter;
        checkArgument(snapshotFileWriter.snapshotStarted(), "snapshot was never started");

        try {
            // first close the output stream (the snapshotFileWriter may already have done this, but nbd)
            snapshotFileWriter.getSnapshotOutputStream().close();

            // the time we'll assign to this snapshot (both in the filename and the db)
            long snapshotTimestamp = System.currentTimeMillis();

            // setup the name/path of the final snapshot file
            String snapshotFilename = String.format("%d-%s.snap", snapshotTimestamp, UUID.randomUUID().toString());
            File snapshotFile = new File(snapshotsDirectory, snapshotFilename);

            // move the temporary file to the final location
            File tempSnapshotFile = snapshotFileWriter.getSnapshotFile();
            Files.move(tempSnapshotFile.toPath(), snapshotFile.toPath(), StandardCopyOption.ATOMIC_MOVE);

            // create the metadata entry for this snapshot
            // note that this filename does _not_ include the path, only the filename itself
            final SnapshotMetadata metadata = new SnapshotMetadata(
                            snapshotFilename,
                            snapshotTimestamp,
                            snapshotFileWriter.getTerm(),
                            snapshotFileWriter.getIndex());

            // actually add this snapshot to the db
            dbi.withHandle(new HandleCallback<Void>() {
                @Override
                public Void withHandle(Handle handle) throws Exception {
                    SnapshotsDAO dao = handle.attach(SnapshotsDAO.class);
                    dao.addSnapshot(metadata);
                    return null;
                }
            });
        } catch (IOException e) {
            throw new StorageException("fail finalize snapshot file req:" + snapshotFileWriter, e);
        } catch (CallbackFailedException e) {
            throw new StorageException("fail store snapshot metadata req:" + snapshotFileWriter, e.getCause());
        } catch (Exception e) {
            throw new StorageException("fail store snapshot req:" + snapshotFileWriter, e);
        }
    }

    // FIXME (AG): should I get the latest snapshot for which I have an associated snapshot file?
    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if this method is called before {@link OnDiskSnapshotsStore#initialize()}
     */
    @Override
    public @Nullable ExtendedSnapshot getLatestSnapshot() throws StorageException {
        checkInitialized();

        try {
            SnapshotMetadata metadata = dbi.withHandle(new HandleCallback<SnapshotMetadata>() {
                @Override
                public SnapshotMetadata withHandle(Handle handle) throws Exception {
                    SnapshotsDAO dao = handle.attach(SnapshotsDAO.class);
                    return dao.getLatestSnapshot();
                }
            });

            if (metadata == null) {
                return null;
            }

            Path snapshot  = Paths.get(snapshotsDirectory, metadata.getFilename());
            if (Files.notExists(snapshot)) {
                throw new StorageException("snapshot not found", new FileNotFoundException(snapshot.toAbsolutePath().toString()));
            }

            return new SnapshotFileReader(snapshotsDirectory, metadata.getFilename(), metadata.getLastTerm(), metadata.getLastIndex());
        } catch (StorageException e) {
            throw e;
        } catch (CallbackFailedException e) {
            throw new StorageException("fail get latest snapshot", e.getCause());
        } catch (Exception e) {
            throw new StorageException("fail get latest snapshot", e);
        }
    }

    /**
     * Get a list of snapshot metadata for all snapshots, ordered
     * from the latest snapshot to the oldest. This method <strong>does not</strong>
     * verify that each snapshot file is present on the filesystem.
     *
     * @return ordered list of all snapshot metadata from latest snapshot to the oldest
     * @throws IllegalStateException if this method is called before {@link OnDiskSnapshotsStore#initialize()}
     * @throws StorageException if the snapshot database cannot be accessed
     * or there was a problem constructing the ordered list
     */
    public List<SnapshotMetadata> getAllSnapshotsFromLatestToOldest() throws StorageException {
        checkInitialized();

        return getMatchingOrderedSnapshots(MATCH_EVERYTHING_PREDICATE);
    }

    private List<SnapshotMetadata> getMatchingOrderedSnapshots(final Predicate<SnapshotMetadata> predicate) throws StorageException {
        try {
            return dbi.withHandle(new HandleCallback<List<SnapshotMetadata>>() {
                    @Override
                    public List<SnapshotMetadata> withHandle(Handle handle) throws Exception {
                        List<SnapshotMetadata> matching = Lists.newLinkedList();

                        SnapshotsDAO dao = handle.attach(SnapshotsDAO.class);
                        Iterator<SnapshotMetadata> iterator = dao.getAllSnapshotsFromLatestToOldest();
                        while (iterator.hasNext()) {
                            SnapshotMetadata metadata = iterator.next();
                            if (predicate.apply(metadata)) {
                                matching.add(metadata);
                            }
                        }

                        return matching;
                    }
                });
        } catch (CallbackFailedException e) {
            throw new StorageException("fail get ordered snapshots", e.getCause());
        } catch (Exception e) {
            throw new StorageException("fail get ordered snapshots", e);
        }
    }

    /**
     * Remove all but the latest {@code j} snapshots.
     * <p/>
     * Given a list of snapshots ordered from newest to oldest
     * ({@code 0, 1, 2, ... j ... n}), a call to {@code pruneSnapshots}
     * will remove all {@code >= j} snapshot metadata and their
     * corresponding files leaving only snapshots {@code 0, 1, 2, ... j - 1}.
     * If {@code numSnapshotsToKeep} is 0 then all snapshot metadata
     * and their corresponding files are removed. This method is a
     * noop if {@code j > n}.
     * <p/>
     * This method <strong>ignores</strong> failures to delete
     * snapshot files from the filesystem. This does not affect correctness
     * since snapshot metadata in the database is the canonical
     * source of snapshot information.
     *
     * @param numSnapshotsToKeep number of latest {@code j} snapshots to keep
     * @throws IllegalStateException if this method is called before {@link OnDiskSnapshotsStore#initialize()}
     * @throws StorageException if the snapshot database cannot be
     * accessed or there was a problem removing snapshot metadata from the database
     */
    public void pruneSnapshots(final int numSnapshotsToKeep) throws StorageException {
        checkInitialized();

        checkArgument(numSnapshotsToKeep >= 0, "invalid number of snapshots to keep:%s", numSnapshotsToKeep);

        try {
            // first, get the suffix of the snapshots in the db, namely the snapshots we want to delete
            List<SnapshotMetadata> snapshotsToDelete = getMatchingOrderedSnapshots(new Predicate<SnapshotMetadata>() {

                private int numSnapshotsToSkip = numSnapshotsToKeep;

                @Override
                public boolean apply(@Nullable SnapshotMetadata input) {
                    checkArgument(input != null);

                    // as long as we still have snapshots to keep, do not include them in the list of snapshots to delete
                    if (numSnapshotsToSkip > 0) {
                        numSnapshotsToSkip--;
                        return false;
                    }

                    // we have no more snapshots to keep, so include them in the list of snapshots to delete
                    return true;
                }
            });

            // now that we have the suffix, remove these snapshots from the db and the filesystem
            for (final SnapshotMetadata metadata : snapshotsToDelete) {
                // first, remove the snapshot metadata from the db
                dbi.withHandle(new HandleCallback<Void>() {
                    @Override
                    public Void withHandle(Handle handle) throws Exception {
                        SnapshotsDAO dao = handle.attach(SnapshotsDAO.class);
                        dao.removeSnapshotWithTimestamp(metadata.getTimestamp());
                        return null;
                    }
                });

                // then, delete it from the filesystem
                // even if this fails its nbd because as long as the metadata entry is gone we won't use it
                Path snapshot = Paths.get(snapshotsDirectory, metadata.getFilename());
                try {
                    if (!Files.deleteIfExists(snapshot)) {
                        LOGGER.warn("{} does not exist", snapshot);
                    }
                } catch (IOException e) {
                    LOGGER.warn("fail delete snapshot {}", snapshot, e);
                }
            }
        } catch (StorageException e) {
            throw e;
        } catch (CallbackFailedException e) {
            throw new StorageException("fail prune snapshot", e.getCause());
        } catch (Exception e) {
            throw new StorageException("fail prune snapshot", e);
        }
    }

    /**
     * Remove all snapshot metadata entries from the database
     * for which there are no corresponding snapshot files on the filesystem.
     *
     * @throws IllegalStateException if this method is called before {@link OnDiskSnapshotsStore#initialize()}
     * @throws StorageException if the snapshot database cannot be
     * accessed or there was a problem removing snapshot metadata from the database
     */
    public void reconcileSnapshots() throws StorageException {
        checkInitialized();

        try {
            // return all snapshots that _do not_ have a corresponding file on the filesystem
            List<SnapshotMetadata> snapshotsToDelete = getMatchingOrderedSnapshots(new Predicate<SnapshotMetadata>() {
                @Override
                public boolean apply(@Nullable SnapshotMetadata metadata) {
                    checkArgument(metadata != null);
                    return !snapshotExistsOnFilesystem(snapshotsDirectory, metadata.getFilename());
                }
            });

            // for each of these broken rows, delete the snapshot entry from the db
            for (final SnapshotMetadata metadata : snapshotsToDelete) {
                dbi.withHandle(new HandleCallback<Void>() {
                    @Override
                    public Void withHandle(Handle handle) throws Exception {
                        SnapshotsDAO dao = handle.attach(SnapshotsDAO.class);
                        dao.removeSnapshotWithTimestamp(metadata.getTimestamp());
                        return null;
                    }
                });
            }
        } catch (StorageException e) {
            throw e;
        } catch (CallbackFailedException e) {
            throw new StorageException("fail reconcile snapshot", e.getCause());
        } catch (Exception e) {
            throw new StorageException("fail reconcile snapshot", e);
        }
    }

    private void checkInitialized() {
        checkState(initialized, "store not initialized");
    }

    private static boolean snapshotExistsOnFilesystem(String snapshotsDirectory, String snapshotFilename) {
        Path snapshotPath = Paths.get(snapshotsDirectory, snapshotFilename);
        return Files.exists(snapshotPath);
    }
}
