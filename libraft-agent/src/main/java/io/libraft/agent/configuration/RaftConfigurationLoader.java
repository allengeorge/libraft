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

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * Provides static utility methods for constructing and validating {@link RaftConfiguration} instances.
 */
public abstract class RaftConfigurationLoader {

    private RaftConfigurationLoader() { } // to prevent instantiation

    /**
     * Load and validate a {@code RaftAgent} configuration from the specified configuration file.
     * <p/>
     * See the project README.md for more on the configuration file options and format.
     *
     * @param configFile absolute path of the libraft-agent configuration file
     * @return fully-populated <strong>and</strong> validated {@link RaftConfiguration} instance.
     *         This instance can be used as an argument to
     *         {@link io.libraft.agent.RaftAgent#RaftAgent(RaftConfiguration, io.libraft.RaftListener)}.
     *
     * @throws IOException if {@code configFile} does not exist or cannot be found
     * @throws RaftConfigurationException if the configuration specified in {@code configFile} has errors
     */
    public static RaftConfiguration loadFromFile(String configFile) throws IOException {
        File config = new File(configFile);

        if (!config.exists() || !config.canRead()) {
            throw new IOException(String.format("%s cannot be opened", config));
        }

        ObjectMapper mapper = new ObjectMapper(); // this method is called rarely, so creating one inline is nbd
        RaftConfiguration configuration = mapper.readValue(config, RaftConfiguration.class);
        return validate(configuration);
    }

    /**
     * Validates a pre-existing {@link io.libraft.agent.configuration.RaftConfiguration}
     * instance. The following basic validation checks are performed:
     * <ul>
     *     <li>All required configuration fields are present.</li>
     *     <li>There are no unrecognized configuration elements.</li>
     *     <li>Endpoints are specified in valid <em>"host:port"</em> form.</li>
     *     <li>All ports are in the valid port range.</li>
     *     <li>All timeouts have values >= 0.</li>
     * </ul>
     * If an optional configuration block is present, it too is validated using the rules above.
     * More thorough validation is performed by components that use the configuration properties.
     *
     * @param configuration instance of {@code RaftConfiguration} to validate
     * @return {@code configuration} (useful if callers would like to method-chain)
     *
     * @throws RaftConfigurationException if the configuration specified in {@code configuration} has errors
     */
    public static RaftConfiguration validate(RaftConfiguration configuration) {
        // TODO (AG): understand why the validator instance cannot be static final
        Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

        Set<ConstraintViolation<RaftConfiguration>> violations = validator.validate(configuration);
        if (!violations.isEmpty()) {
            throw new RaftConfigurationException(violations);
        }

        return configuration;
    }
}
