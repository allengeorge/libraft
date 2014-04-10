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

package io.libraft.kayvee.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.net.HostAndPort;
import io.libraft.agent.configuration.Endpoints;
import org.hibernate.validator.constraints.NotEmpty;

import javax.annotation.Nullable;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Defines a KayVee server. Contains the following properties:
 * <ul>
 *     <li>id: unique id of the KayVee server (this id is used as this server's id in the Raft cluster).</li>
 *     <li>kayVeeUrl: HTTP endpoint to which KayVee REST API calls should be made.</li>
 *     <li>raftEndpoint: URL of the local server in host:port format to which Raft protocol messages can be sent.</li>
 * </ul>
 * See the KayVee README.md for more on the KayVee configuration.
 */
public final class ClusterMember {

    private static final String ID = "id";
    private static final String KAYVEE_URL = "kayVeeUrl";
    private static final String RAFT_ENDPOINT = "raftEndpoint";
    private static final String RAFT_HOST = "raftHost";
    private static final String RAFT_PORT = "raftPort";

    @NotEmpty
    private final String id;

    @NotEmpty
    private final URL kayVeeUrl;

    @NotEmpty
    private final String raftHost;

    @NotEmpty
    @Min(1)
    @Max(65535)
    private final int raftPort;

    /**
     * Constructor.
     *
     * @param id non-null, non-empty unique id of the local KayVee server and its underlying Raft agent
     * @param kayVeeUrl non-null, non-empty HTTP endpoint to which KayVee REST API calls should be made
     * @param raftEndpoint non-null, non-empty host:port URL of the local server to which Raft protocol messages can be sent
     */
    @JsonCreator
    public ClusterMember(@JsonProperty(ID) String id, @JsonProperty(KAYVEE_URL) String kayVeeUrl, @JsonProperty(RAFT_ENDPOINT) String raftEndpoint) {
        this.id = id;
        this.kayVeeUrl = stringToURL(kayVeeUrl);

        // throws an IllegalStateException on failure
        // I considered wrapping this with an IllegalArgumentException for consistency,
        // but decided that the extra level for indirection wasn't worth it
        HostAndPort raftHostAndPort = Endpoints.getValidHostAndPortFromString(raftEndpoint);

        this.raftHost = raftHostAndPort.getHostText();
        this.raftPort = raftHostAndPort.getPort();
    }

    private static URL stringToURL(String urlString) {
        try {
            URL convertedUrl = new URL(urlString);

            if (convertedUrl.getHost().isEmpty()) {
                throw new IllegalArgumentException("Url has empty host");
            }

            return convertedUrl;
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Get the unique id of the local KayVee server/Raft endpoint.
     *
     * @return non-null, non-empty unique id of the local KayVee server/Raft endpoint
     */
    public String getId() {
        return id;
    }

    /**
     * Get the HTTP URL of the local server to which
     * all KayVee REST API requests should be made.
     *
     * @return non-null, non-empty HTTP URL of the local
     * server to which all KayVee REST API requests should be made
     */
    public String getKayVeeUrl() {
        return kayVeeUrl.toString();
    }

    /**
     * Get the hostname of the endpoint of the local server
     * to which all Raft protocol messages should be sent.
     *
     * @return non-null, non-empty hostname of the endpoint
     * of the local server to which all Raft protocol messages should be sent
     */
    public String getRaftHost() {
        return raftHost;
    }

    /**
     * Get the port of the endpoint of the local server
     * to which all Raft protocol messages should be sent.
     *
     * @return non-zero port of the endpoint of the local
     * server to which all Raft protocol messages should be sent
     */
    public int getRaftPort() {
        return raftPort;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterMember other = (ClusterMember) o;

        return id.equals(other.id) && kayVeeUrl.equals(other.kayVeeUrl) && raftHost.equals(other.raftHost) && raftPort == other.raftPort;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, kayVeeUrl, raftHost, raftPort);
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add(ID, id)
                .add(KAYVEE_URL, kayVeeUrl)
                .add(RAFT_HOST, raftHost)
                .add(RAFT_PORT, raftPort)
                .toString();
    }
}
