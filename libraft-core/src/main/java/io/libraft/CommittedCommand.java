package io.libraft;

import com.google.common.base.Objects;

// TODO (AG): I strongly suspect I'll have to generalize this class to communicate configuration changes and snapshots as well
/**
 * Holder class that pairs a committed {@link Command} and its Raft log index.
 */
public final class CommittedCommand {

    private final long index;
    private final Command command;

    /**
     * Constructor.
     *
     * @param index index > 0 in the Raft log of the committed {@code Command}
     * @param command the committed {@code Command} instance
     *
     * @see Command
     */
    public CommittedCommand(long index, Command command) {
        this.index = index;
        this.command = command;
    }

    /**
     * Get the log index of the committed {@code Command}.
     *
     * @return index > 0 in the Raft log of the committed {@code Command}
     */
    public long getIndex() {
        return index;
    }

    /**
     * Get the committed {@code Command}.
     *
     * @return the committed {@code Command} instance
     */
    public Command getCommand() {
        return command;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CommittedCommand other = (CommittedCommand) o;

        return index == other.index && command.equals(other.command);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(index, command);
    }

    @Override
    public String toString() {
        return Objects
                .toStringHelper(this)
                .add("index", index)
                .add("command", command)
                .toString();
    }
}