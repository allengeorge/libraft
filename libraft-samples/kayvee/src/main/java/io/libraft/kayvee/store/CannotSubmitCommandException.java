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

    private static final long serialVersionUID = -1841537123003976950L;

    private final String self;
    private final @Nullable String leader;
    private final Set<ClusterMember> members;

    /**
     * Constructor.
     *
     * @param cause instance of {@code NotLeaderException} that was thrown
     *              when a command was submitted to the Raft cluster
     * @param members unique ids of all the servers (including the local server) that comprise the Raft cluster
     */
    public CannotSubmitCommandException(NotLeaderException cause, Set<ClusterMember> members) {
        super(cause.getMessage());

        this.self = cause.getSelf();
        this.leader = cause.getLeader();
        this.members = members;
    }

    /**
     * Get the unique id of the local KayVee server.
     *
     * @return unique id of the local KayVee server
     */
    public String getSelf() {
        return self;
    }

    /**
     * Get the unique id of the server that is the leader of the Raft cluster.
     *
     * @return unique id of the server that is the leader of the Raft cluster, or null if the leader is not known
     */
    public @Nullable String getLeader() {
        return leader;
    }

    /**
     * Get the unique ids of all the servers that comprise the Raft cluster.
     *
     * @return unique ids of all the servers (including the local server) that comprise the Raft cluster
     */
    public Set<ClusterMember> getMembers() {
        return members;
    }
}
