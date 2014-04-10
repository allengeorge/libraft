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

package io.libraft.agent.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.hibernate.validator.constraints.NotEmpty;

import javax.annotation.Nullable;

/**
 * Represents the configuration for the database backend to
 * be used as the <strong>persistent</strong> store for
 * {@link io.libraft.algorithm.Log} and {@link io.libraft.algorithm.Store}.
 * Contains the following properties:
 * <ul>
 *     <li>driverClass</li>
 *     <li>url</li>
 *     <li>user</li>
 *     <li>password</li>
 * </ul>
 * See the project README.md for more on the configuration.
 */
public final class RaftDatabaseConfiguration {

    // NOTE: property names chosen to match those used in Dropwizard's database configuration
    private static final String DRIVER_CLASS = "driverClass";
    private static final String URL = "url";
    private static final String USER = "user";
    private static final String PASSWORD = "password";

    @NotEmpty
    @JsonProperty(DRIVER_CLASS)
    private final String driverClass;

    @NotEmpty
    @JsonProperty(URL)
    private final String url;

    @NotEmpty
    @JsonProperty(USER)
    private final String user;

    @JsonProperty(PASSWORD)
    @Nullable
    private final String password;

    /**
     * Constructor.
     *
     * @param driverClass JDBC database driver class
     * @param url URL of the form <em>"jdbc:..."</em> used to access the database
     * @param user user id of the account used to access the database
     * @param password password of the account used to access the database or {@code null} if no password is required
     */
    @JsonCreator
    public RaftDatabaseConfiguration(
            @JsonProperty(DRIVER_CLASS) String driverClass,
            @JsonProperty(URL) String url,
            @JsonProperty(USER) String user,
            @JsonProperty(PASSWORD) @Nullable String password) {
        this.driverClass = driverClass;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    /**
     * Get the fully-qualified class name of the JDBC database driver class.
     *
     * @return class name of the JDBC database driver class
     */
    public String getDriverClass() {
        return driverClass;
    }

    /**
     * Get the JDBC connection string used to access the database.
     * This connection string <strong>has not</strong> been validated.
     *
     * @return JDBC connection string used to access the database
     */
    public String getUrl() {
        return url;
    }

    /**
     * Get the user id of the account used to access the database.
     *
     * @return non-null, non-empty user id of the account used to access the database
     */
    public String getUser() {
        return user;
    }

    /**
     * Get the password of the account used to access the database.
     *
     * @return password of the account used to access the database or {@code null} if no password is required
     */
    public @Nullable String getPassword() {
        return password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RaftDatabaseConfiguration other = (RaftDatabaseConfiguration) o;
        return driverClass.equals(other.driverClass)
                && url.equals(other.url)
                && user.equals(other.user)
                && (password != null ? password.equals(other.password) : other.password == null);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(driverClass, url, user, password);
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add(DRIVER_CLASS, driverClass)
                .add(URL, url)
                .add(USER, user)
                .add(PASSWORD, password)
                .toString();
    }
}
