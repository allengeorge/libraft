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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.net.HostAndPort;
import io.libraft.agent.configuration.Endpoints;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Defines a KayVee server. Contains the following properties:
 * <ul>
 *     <li>id</li>
 *     <li>kayVeeUrl</li>
 *     <li>raftEndpoint</li>
 * </ul>
 * See the KayVee README.md for more on the KayVee configuration.
 */
public final class ClusterMember {

    @NotEmpty
    private final String id;

    @NotNull
    private final URL kayVeeUrl;

    @NotEmpty
    private final String raftHost;

    @NotEmpty
    @Min(1)
    @Max(65535)
    private final int raftPort;

    @JsonCreator
    public ClusterMember(@JsonProperty("id") String id, @JsonProperty("kayVeeUrl") String kayVeeUrl, @JsonProperty("raftEndpoint") String raftEndpoint) {
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

    public String getId() {
        return id;
    }

    public String getKayVeeUrl() {
        return kayVeeUrl.toString();
    }

    public String getRaftHost() {
        return raftHost;
    }

    public int getRaftPort() {
        return raftPort;
    }

    @Override
    public boolean equals(Object o) {
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
                .add("id", id)
                .add("kayVeeUrl", kayVeeUrl)
                .add("raftHost", raftHost)
                .add("raftPort", raftPort)
                .toString();
    }
}
