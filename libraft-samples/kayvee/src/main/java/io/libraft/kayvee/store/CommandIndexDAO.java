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

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;

/**
 * DAO interface to the {@code kv_last_applied_command} table.
 * <p/>
 * The table has the following structure:
 * <pre>
 * +-----------------+
 * | kv_command_index|
 * +-----------------+
 * |                 | <---- <strong>must</strong> contain only 1 row
 * +-----------------+
 * </pre>
 * It contains a <strong>single</strong> row that holds the
 * log index of the last committed command that the local server
 * received and successfully processed. As a result, the value in
 * this table is only updated <strong>after</strong> a {@link KayVeeCommand}
 * is committed to the Raft cluster.
 */
interface CommandIndexDAO {

    // FIXME (AG): set a constraint within the DB to ensure that this table cannot have multiple rows

    @SqlUpdate("create table if not exists kv_last_applied_command(kv_command_index bigint not null)")
    void createTable();

    @SqlUpdate("drop table if exists kv_last_applied_command")
    void dropTable();

    //
    // Insert a new value for {@code kv_command_index}, the unique
    // {@code Raft
    // log index of the last successfully received and processed
    // {@link io.libraft.kayvee.store.KayVeeCommand}.
    // <p/>
    // It is the caller's responsibility to call this method
    // <strong>only</strong> when {@code kv_last_applied_command} is empty.
    // Doing so otherwise <strong>will</strong> put this table into an
    // inconsistent state.
    //
    @SqlUpdate("insert into kv_last_applied_command (kv_command_index) values(:kv_command_index)")
    void addLastAppliedCommandIndex(@Bind("kv_command_index") long commandIndex);

    @SqlUpdate("update kv_last_applied_command set kv_command_index = :kv_command_index")
    void updateLastAppliedCommandIndex(@Bind("kv_command_index") long commandIndex);

    @SqlQuery("select kv_command_index from kv_last_applied_command limit 1")
    Long getLastAppliedCommandIndex();
}
