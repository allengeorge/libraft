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

package io.libraft.kayvee.mappers;

import com.yammer.dropwizard.jetty.UnbrandedErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Random;

/**
 * Base exception mapper for all KayVee custom exception mappers.
 * <p/>
 * The implementation is based on {@link com.yammer.dropwizard.jersey.LoggingExceptionMapper}.
 * It includes small customizations to the response entity and allows subclasses to set the response code.
 * Unlike {@code LoggingExceptionMapper} it <strong>does not</strong> allow subclasses to format
 * log messages or response entities.
 *
 * @see javax.ws.rs.ext.ExceptionMapper
 * @see com.yammer.dropwizard.jersey.LoggingExceptionMapper
 */
@Provider
abstract class KayVeeLoggingExceptionMapper<ExceptionType extends Exception> implements ExceptionMapper<ExceptionType> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KayVeeLoggingExceptionMapper.class);
    private static final Random RANDOM = new Random();

    private final UnbrandedErrorHandler errorHandler = new UnbrandedErrorHandler();

    /**
     * Request being processed that generated the exception.
     */
    @Context
    protected HttpServletRequest request;

    /**
     * Constructor.
     */
    protected KayVeeLoggingExceptionMapper() { }

    /**
     * Generate an HTTP error response.
     * <p/>
     * This response includes:
     * <ul>
     *     <li>A unique id for this exception event.</li>
     *     <li>The response code (specified by subclasses).</li>
     *     <li>The exception stack trace.</li>
     * </ul>
     *
     * @param exception exception instance thrown while processing {@link KayVeeLoggingExceptionMapper#request}
     * @param status HTTP status code of the response (ex. 400, 404, etc.)
     * @return a valid {@code Response} instance with a fully-populated error message and HTTP response code set that can be sent to the client
     */
    protected final Response newResponse(ExceptionType exception, Response.Status status) {
        final StringWriter writer = new StringWriter(4096);
        try {
            final long id = randomId();
            logException(id, exception);
            errorHandler.writeErrorPage(request,
                                        writer,
                                        status.getStatusCode(),
                                        formatResponseEntity(id, exception),
                                        false);
        } catch (IOException e) {
            LOGGER.warn("Unable to generate error page", e);
        }

        return Response
                .status(status)
                .type(MediaType.TEXT_HTML_TYPE)
                .entity(writer.toString())
                .build();
    }

    private static long randomId() {
        synchronized (KayVeeLoggingExceptionMapper.class) {
            return RANDOM.nextLong();
        }
    }

    private void logException(long id, ExceptionType exception) {
        LOGGER.error(formatLogMessage(id), exception);
    }

    private String formatLogMessage(long id) {
        return String.format("Error handling a request: %016x", id);
    }

    private String formatResponseEntity(long id, Throwable cause) {
        return String.format("There was an error processing your request. The cause was \"%s\". It has been logged (ID %016x).\n", cause.getMessage(), id);
    }
}
