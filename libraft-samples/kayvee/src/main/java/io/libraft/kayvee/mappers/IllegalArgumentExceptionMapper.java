package io.libraft.kayvee.mappers;

import javax.ws.rs.core.Response;

/**
 * Specialization of {@link KayVeeLoggingExceptionMapper} that
 * transforms an {@link IllegalArgumentException} into an HTTP
 * response with a {@code BAD REQUEST} (400) status code.
 */
public final class IllegalArgumentExceptionMapper extends KayVeeLoggingExceptionMapper<IllegalArgumentException> {

    @Override
    public Response toResponse(IllegalArgumentException exception) {
        return newResponse(exception, Response.Status.BAD_REQUEST);
    }
}
