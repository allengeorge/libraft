package io.libraft.kayvee.store;

import io.libraft.NotLeaderException;
import io.libraft.kayvee.configuration.ClusterMember;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * Thrown when a {@link KayVeeCommand} cannot
 * be completed because of a {@link NotLeaderException}.
 * <p/>
 * An instance of {@code CannotSubmitCommandException} <strong>must</strong>
 * be thrown whenever KayVee code catches an instance of {@code NotLeaderException}.
 * The original {@code NotLeaderException} <strong>must not</strong> be propagated.
 */
public final class CannotSubmitCommandException extends KayVeeException {

    private final String self;
    private final @Nullable String leader;
    private final Set<ClusterMember> members;

    public CannotSubmitCommandException(NotLeaderException cause, Set<ClusterMember> members) {
        super(cause.getMessage());

        this.self = cause.getSelf();
        this.leader = cause.getLeader();
        this.members = members;
    }

    public String getSelf() {
        return self;
    }

    public @Nullable String getLeader() {
        return leader;
    }

    public Set<ClusterMember> getMembers() {
        return members;
    }
}
