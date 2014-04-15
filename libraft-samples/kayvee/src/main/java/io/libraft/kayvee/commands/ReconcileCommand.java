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

package io.libraft.kayvee.commands;

import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import io.libraft.agent.snapshots.OnDiskSnapshotsStore;
import io.libraft.kayvee.configuration.KayVeeConfiguration;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.skife.jdbi.v2.DBI;

public final class ReconcileCommand extends ConfiguredCommand<KayVeeConfiguration> {

    public ReconcileCommand() {
        super("reconcile", "reconcile the snapshots database with their on-disk representations");
    }

    @Override
    protected void run(Bootstrap<KayVeeConfiguration> bootstrap, Namespace namespace, KayVeeConfiguration configuration) throws Exception {
        DataSource dataSource = CommandDataSource.newDataSource(configuration);
        DBI dbi = new DBI(dataSource);
        OnDiskSnapshotsStore snapshotsStore = new OnDiskSnapshotsStore(dbi, configuration.getSnapshotsConfiguration().getSnapshotsDirectory());

        try {
            snapshotsStore.initialize();
            snapshotsStore.reconcileSnapshots();
        } finally {
            snapshotsStore.teardown();
        }
    }
}