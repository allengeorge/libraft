package io.libraft;

/**
 * Implemented by classes that represent a committed {@link Command}.
 */
public interface CommittedCommand extends Committed {

    /**
     * Get the committed {@code Command}.
     *
     * @return the committed {@code Command} instance
     */
    Command getCommand();
}