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
