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

import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * DAO for creating, storing and accessing snapshot
 * metadata from a JDBC-compliant database.
 * <p/>
 * Snapshot metadata is stored in a table named {@code snapshots}
 * with the following format:
 * <pre>
 *     +--------------------------------------------------+
 *     |  filename |     ts     | last_term  | last_index |
 *     +--------------------------------------------------+
 *     |    ...    |    ...     |    ...     |    ...     |
 *     |           |            |            |            |
 * </pre>
 * Callers interact with table rows through instances
 * of {@link SnapshotMetadata}, where each {@code SnapshotMetadata}
 * instance maps to a single row in the {@code snapshots} table.
 *
 * @see SnapshotMetadata
 */
abstract class SnapshotsDAO {

    /**
     * Create the {@code snapshots} table and its associated index.
     */
    @Transaction
    void createSnapshotsTableWithIndex() {
        createSnapshotsTable();
        createTimestampIndexForSnapshotsTable();
    }

    /**
     * Create the {@code snapshots} table.
     */
    // FIXME (AG): I'm essentially using the timestamp as a primary key
    // I did this is because auto-increment indices have different syntaxes in different dbs - might be best to create an explicit index
    @SqlUpdate("create table if not exists snapshots(filename varchar(255) not null, ts bigint unique, last_term bigint not null, last_index bigint not null)")
    abstract void createSnapshotsTable();

    /**
     * Create the index for the {@code snapshots} table.
     */
    @SqlUpdate("create index if not exists ts_index on snapshots(ts)")
    abstract void createTimestampIndexForSnapshotsTable();

    /**
     * Add the snapshot metadata for an application-generated
     * snapshot to the {@code snapshots} table.
     *
     * @param metadata instance of {@code SnapshotMetadata} containing
     *                 values for all the columns in the snapshots table
     */
    @SqlUpdate("insert into snapshots(filename, ts, last_term, last_index) values(:m.filename, :m.timestamp, :m.lastTerm, :m.lastIndex)")
    abstract void addSnapshot(@BindBean("m") SnapshotMetadata metadata);

    /**
     * Remove the snapshot metadata row with the given timestamp.
     *
     * @param snapshotTimestamp epoch timestamp >= 0 for the snapshot row to be removed
     */
    @SqlUpdate("delete from snapshots where ts=:snapshotTimestamp")
    abstract void removeSnapshotWithTimestamp(@Bind("snapshotTimestamp") long snapshotTimestamp);

    /**
     * Get the latest snapshot metadata entry in the {@code snapshots} table.
     *
     * @return instance of {@code SnapshotMetadata} containing values for the latest
     * snapshot metadata entry in the {@code snapshots} table, or null if the table is empty
     */
    @SqlQuery("select filename, ts, last_term, last_index from snapshots where ts in (select max(ts) from snapshots)")
    @Mapper(SnapshotMetadataResultMapper.class)
    abstract @Nullable SnapshotMetadata getLatestSnapshot();

    /**
     * Get the number of snapshot metadata entries in the {@code snapshots} table
     *
     * @return number >= 0 of snapshot metadata entries in the {@code snapshots} table
     */
    @SqlQuery("select count(*) from snapshots")
    abstract int getNumSnapshots();

    /**
     * Get an iterator that can be used to iterate over
     * all the snapshot metadata entries in the {@code snapshots} table
     *
     * @return instance of {@code ResultIterator} that can be used
     * to iterate over all the snapshot metadata entries in the {@code snapshots} table
     */
    @SqlQuery("select filename, ts, last_term, last_index from snapshots order by ts desc")
    @Mapper(SnapshotMetadataResultMapper.class)
    abstract ResultIterator<SnapshotMetadata> getAllSnapshotsFromLatestToOldest();

    /**
     * Closes the JDBC connection used by this {@code SnapshotsDAO} instance.
     */
    @SuppressWarnings("unused")
    abstract void close();

    /**
     * Maps a row in the {@code snapshots} table to its Java representation.
     */
    public static final class SnapshotMetadataResultMapper implements ResultSetMapper<SnapshotMetadata> { // NOTE: needs to be public to be used by JDBI

        @Override
        public SnapshotMetadata map(int index, ResultSet r, StatementContext ctx) throws SQLException {
            if (!r.isAfterLast()) {
                return new SnapshotMetadata(r.getString("filename"), r.getLong("ts"), r.getLong("last_term"), r.getLong("last_index"));
            } else {
                return null;
            }
        }
    }
}
