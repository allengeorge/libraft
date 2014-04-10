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

package io.libraft.agent.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.libraft.Command;
import io.libraft.agent.CommandDeserializer;
import io.libraft.agent.CommandSerializer;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

/**
 * Base class for all Raft RPC request and response messages.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = RequestVote.class, name = "REQUEST_VOTE"),
        @JsonSubTypes.Type(value = RequestVoteReply.class, name = "REQUEST_VOTE_REPLY"),
        @JsonSubTypes.Type(value = AppendEntries.class, name = "APPEND_ENTRIES"),
        @JsonSubTypes.Type(value = AppendEntriesReply.class, name = "APPEND_ENTRIES_REPLY")
})
@JsonPropertyOrder({RaftRPC.TYPE, RaftRPC.SOURCE, RaftRPC.DESTINATION, RaftRPC.TERM})
public abstract class RaftRPC {

    static final String TYPE = "type";
    static final String SOURCE = "source";
    static final String DESTINATION = "destination";
    static final String TERM = "term";

    // FIXME (AG): find a way to not require this
    // I understand why the first half is required (i.e. I need to say 'all instances of Command.class should be deserialized as Class<T>)
    // but I don't understand why Jackson then doesn't do the 'standard' deserialization process for Class<T> and instead
    // requires me to create a custom deserializer that simply does that
    // I'm sure it's my misunderstanding of Jackson, but, having fought this for a number of days
    // I'm no longer in the mood to deal with it
    /**
     * Setup serialization and deserialization for Jackson-annotated {@link Command} subclasses.
     * All Jackson-annotated {@code Command} subclasses <strong>must</strong> derive from a single base class.
     * <p/>
     * The following <strong>is</strong> supported:
     * <pre>
     *     Object
     *     +-- CMD_BASE
     *         +-- CMD_0
     *         +-- CMD_1
     * </pre>
     * And the following is <strong>not</strong> supported:
     * <pre>
     *     Object
     *     +-- CMD_0
     *     +-- CMD_1
     * </pre>
     * See {@code RaftAgent} for more on which {@code Command} types are supported.
     *
     * @param mapper instance of {@code ObjectMapper} with which the serialization/deserialization mapping is registered
     * @param commandSubclassKlass the base class of all the Jackson-annotated {@code Command} classes
     *
     * @see io.libraft.agent.RaftAgent
     */
    public static <T extends Command> void setupJacksonAnnotatedCommandSerializationAndDeserialization(ObjectMapper mapper, Class<T> commandSubclassKlass) {
        SimpleModule module = new SimpleModule("raftrpc-jackson-command-module", new Version(0, 0, 0, "inline", "io.libraft", "raftrpc-jackson-command-module"));

        module.addAbstractTypeMapping(Command.class, commandSubclassKlass);
        module.addDeserializer(commandSubclassKlass, new RaftRPCCommand.PassThroughDeserializer<T>(commandSubclassKlass));

        mapper.registerModule(module);
    }

    /**
     * Setup custom serialization and deserialization for POJO {@link Command} subclasses.
     * <p/>
     * See {@code RaftAgent} for more on which {@code Command} types are supported.
     *
     * @param mapper instance of {@code ObjectMapper} with which the serialization/deserialization mapping is registered
     * @param commandSerializer instance of {@code CommandSerializer} that can serialize a POJO {@code Command} instance into binary
     * @param commandDeserializer instance of {@code CommandDeserializer} that can deserialize binary into a POJO {@code Command} instance
     *
     * @see io.libraft.agent.RaftAgent
     */
    public static void setupCustomCommandSerializationAndDeserialization(ObjectMapper mapper, CommandSerializer commandSerializer, CommandDeserializer commandDeserializer) {
        SimpleModule module = new SimpleModule("raftrpc-custom-command-module", new Version(0, 0, 0, "inline", "io.libraft", "raftrpc-command-module"));

        module.addSerializer(Command.class, new RaftRPCCommand.Serializer(commandSerializer));
        module.addDeserializer(Command.class, new RaftRPCCommand.Deserializer(commandDeserializer));

        mapper.registerModule(module);
    }

    @NotEmpty
    @JsonProperty(SOURCE)
    private final String source;

    @NotEmpty
    @JsonProperty(DESTINATION)
    private final String destination;

    @Min(value = 0)
    @JsonProperty(TERM)
    private final long term;

    // NOTE: package-private so that nobody outside this package can create additional RPCs
    // NOTE: unfortunately I cannot annotate this with @JsonProperty and have it be picked up by subclasses because:
    //   1. Jackson doesn't support this
    //   2. Every class with a @JsonCreator annotation must annotate all its parameters with a @JsonProperty annotation
    //      see: http://stackoverflow.com/questions/17528581/inheriting-jsoncreator-annotations-from-superclass
    // As a result, I have to annotate source/destination in all the subclasses with the _same_ @JsonProperty("source")
    // or @JsonProperty("destination") name

    /**
     * Constructor.
     *
     * @param source unique id of the Raft server that generated the message
     * @param destination unique id of the Raft server that is the intended recipient
     * @param term election term in which the message was generated
     */
    RaftRPC(String source, String destination, long term) {
        this.source = source;
        this.destination = destination;
        this.term = term;
    }

    /**
     * Get the unique id of the server that sent the message.
     *
     * @return unique, non-null, non-empty id of the source server
     */
    public final String getSource() {
        return source;
    }

    /**
     * Get the unique id of the server meant to receive the message.
     *
     * @return unique, non-null, non-empty id of the destination server
     */
    public final String getDestination() {
        return destination;
    }

    /**
     * Get the election term in which the message was generated.
     *
     * @return election term > 0 in which the message was generated
     */
    public final long getTerm() {
        return term;
    }
}
