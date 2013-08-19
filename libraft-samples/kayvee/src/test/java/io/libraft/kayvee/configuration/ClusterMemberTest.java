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

package io.libraft.kayvee.configuration;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.MalformedURLException;

public class ClusterMemberTest {

    private static final String CLUSTER_MEMBER_ID = "SELF";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldThrowIfKayVeeURLWithoutASchemeIsUsed() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectCause(Matchers.<Throwable>instanceOf(MalformedURLException.class));
        new ClusterMember(CLUSTER_MEMBER_ID, "noProtocol:8000", "localhost:6080");
    }

    @Test
    public void shouldThrowIfKayVeeURLWithoutAHostIsUsed() {
        expectedException.expect(IllegalArgumentException.class);
        new ClusterMember(CLUSTER_MEMBER_ID, "http://", "localhost:6080");
    }

    @Test
    public void shouldThrowIfRaftEndpointWithoutAHostIsUsed() {
        expectedException.expect(IllegalStateException.class);
        new ClusterMember(CLUSTER_MEMBER_ID, "http://localhost:8000", "6080");
    }

    @Test
    public void shouldThrowIfRaftEndpointWithoutAPortIsUsed() {
        expectedException.expect(IllegalStateException.class);
        new ClusterMember(CLUSTER_MEMBER_ID, "http://localhost:8000", "localhost");
    }

    @Test
    public void shouldCreateValidClusterMember() {
        new ClusterMember(CLUSTER_MEMBER_ID, "http://localhost:8000", "localhost:6080");
    }
}
