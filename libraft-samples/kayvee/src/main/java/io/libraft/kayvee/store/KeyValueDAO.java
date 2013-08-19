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

import io.libraft.kayvee.api.KeyValue;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

/**
 * DAO interface to the {@code kv_store} table.
 * <p/>
 * The table has the following structure:
 * <pre>
 * +----------+----------+
 * |  kv_key  | kv_value |
 * +----------+----------+
 * |          |          |
 * +----------+----------+
 * </pre>
 * It maps a {@code key} to a {@code value}, where {@code key} is a
 * unique, non-null, non-empty string, and {@code value} is a non-null, non-empty string.
 * The {@code kv_store} table is a snapshot of the last known
 * <strong>committed</strong> KayVee key-value state. As a result,
 * key-value transformations are only applied to this table after they
 * are durably replicated to the Raft cluster.
 */
@RegisterMapper(KeyValueDAO.KeyValueMapper.class)
interface KeyValueDAO {

    /**
     * A specialization of {@link org.skife.jdbi.v2.tweak.ResultSetMapper}
     * that maps a row from the {@code kv_store} table to a {@link io.libraft.kayvee.api.KeyValue} object.
     */
    static final class KeyValueMapper implements ResultSetMapper<KeyValue> {

        /**
         * @return {@code null} if the {@link java.sql.ResultSet} is empty
         */
        @Override
        public KeyValue map(int index, ResultSet resultSet, StatementContext ctx) throws SQLException {
            return new KeyValue(resultSet.getString("kv_key"), resultSet.getString("kv_value"));
        }
    }

    @SqlUpdate("create table if not exists kv_store(kv_key varchar(500) primary key, kv_value varchar(500) not null)")
    void createTable();

    @SqlUpdate("drop table if exists kv_store")
    void dropTable();

    @SqlQuery("select kv_key, kv_value from kv_store where kv_key = :kv_key")
    @Nullable KeyValue get(@Bind("kv_key") String key);

    @SqlQuery("select kv_key, kv_value from kv_store")
    Collection<KeyValue> getAll();

    @SqlUpdate("insert into kv_store (kv_key, kv_value) values(:kv_key, :kv_value)")
    void add(@Bind("kv_key") String key, @Bind("kv_value") String value);

    @SqlUpdate("update kv_store set kv_value = :kv_value where kv_key = :kv_key")
    void update(@Bind("kv_key") String key, @Bind("kv_value") String value);

    @SqlUpdate("delete from kv_store where kv_key = :kv_key")
    void delete(@Bind("kv_key") String key);
}
