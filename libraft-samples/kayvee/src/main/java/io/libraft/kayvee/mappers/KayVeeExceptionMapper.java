package io.libraft.kayvee.mappers;

import com.google.common.net.HttpHeaders;
import io.libraft.kayvee.configuration.ClusterMember;
import io.libraft.kayvee.store.CannotSubmitCommandException;
import io.libraft.kayvee.store.KayVeeException;
import io.libraft.kayvee.store.KeyAlreadyExistsException;
import io.libraft.kayvee.store.KeyNotFoundException;
import io.libraft.kayvee.store.ValueMismatchException;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.ext.Provider;
import java.net.URI;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Specialization of {@link KayVeeLoggingExceptionMapper} that
 * transforms a {@link KayVeeException} into an HTTP response
 * with the corresponding status code. Possible status codes are shown below:
 * <ul>
 *     <li>{@link CannotSubmitCommandException} with {@link CannotSubmitCommandException#getLeader()} as {@code !null}: {@code MOVED PERMANENTLY} (301).</li>
 *     <li>{@link CannotSubmitCommandException} with {@link CannotSubmitCommandException#getLeader()} as {@code null}: {@code SERVICE UNAVAILABLE} (503).</li>
 *     <li>{@link KeyNotFoundException}: {@code NOT FOUND} (404).</li>
 *     <li>{@link KeyAlreadyExistsException}: {@code CONFLICT} (409).</li>
 *     <li>{@link ValueMismatchException}: {@code CONFLICT} (409.)</li>
 * </ul>
 */
@Provider
public final class KayVeeExceptionMapper extends KayVeeLoggingExceptionMapper<KayVeeException> {

    @Override
    public Response toResponse(KayVeeException cause) {
        Response response;

        if (cause instanceof CannotSubmitCommandException) {
            response = newResponseForCannotSubmitCommandException((CannotSubmitCommandException) cause);
        } else if (cause instanceof KeyNotFoundException) {
            response = newResponse(cause, Response.Status.NOT_FOUND);
        }else if (cause instanceof KeyAlreadyExistsException) {
            response = newResponse(cause, Response.Status.CONFLICT);
        } else if (cause instanceof ValueMismatchException) {
            response = newResponse(cause, Response.Status.CONFLICT);
        } else {
            throw new IllegalArgumentException("unrecognized exception type:" + cause.getClass());
        }

        return response;
    }

    private Response newResponseForCannotSubmitCommandException(CannotSubmitCommandException cause) {
        String leader = cause.getLeader();

        if (leader == null) {
            return newResponse(cause, Response.Status.SERVICE_UNAVAILABLE);
        } else {
            URI leaderUri = getLeaderURI(request.getRequestURI(), leader, cause.getMembers());

            if (leaderUri != null) {
                Response response = newResponse(cause, Response.Status.MOVED_PERMANENTLY);
                response.getMetadata().add(HttpHeaders.LOCATION, leaderUri);
                return response;
            } else { // there _is_ a leader (the agent is communicating with it), but apparently there's no KayVee URL
                throw new IllegalStateException("no KayVee URL for leader:" + leader);
            }
        }
    }

    private static @Nullable URI getLeaderURI(String requestUri, String leader, Set<ClusterMember> members) {
        checkNotNull(leader);

        for (ClusterMember member : members) {
            if (member.getId().equals(leader)) {
                return UriBuilder.fromUri(member.getKayVeeUrl()).path(requestUri).build();
            }
        }

        return null;
    }
}
