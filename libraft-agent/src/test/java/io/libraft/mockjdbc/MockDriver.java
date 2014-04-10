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

package io.libraft.mockjdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import static org.mockito.Mockito.mock;

/**
 * This class is intentionally open for extension so that Mockito can spy or mock it
 * <p/>
 * Looking at the DriverManager code, it appears that there are two entry points here:
 * <ol>
 *     <li>A caller calls {@link java.sql.DriverManager#getDriver(String)},
 *         in which case the {@code DriverManager} loops through its list of registered drivers
 *         and calls {@code acceptsUrl(String)} on each one, to find the matching driver
 *     </li>
 *     <li>A caller calls {@link java.sql.DriverManager#getConnection(String)},
 *         in which case the {@code DriverManager} loops through its list of registered drivers
 *         and calls {@code connect(String, Properties)} on each one, until it finds the
 *         first driver that returns a non-null {@code Connection}
 *      </li>
 * </ol>
 * This implies that {@code connect(String, Properties)} should defer to {@code acceptsUrl(String)} internally
 */
public class MockDriver implements Driver {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockDriver.class);

    private final String driverSpecificName;

    public MockDriver(String driverSpecificName) {
        LOGGER.trace("initialize mock driver name:{}", driverSpecificName);
        this.driverSpecificName = driverSpecificName;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("jul unsupported");
    }

    @Override
    public Connection connect(String url, Properties properties) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        return mock(Connection.class);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        LOGGER.info("check url:{} against driver:{}", url, driverSpecificName);

        boolean contains = url.contains(driverSpecificName);

        if (contains) {
            LOGGER.info("accept url:{}", url);
        }

        return contains;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }
}
