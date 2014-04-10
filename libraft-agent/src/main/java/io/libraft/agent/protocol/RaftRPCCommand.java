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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.io.Closeables;
import io.libraft.Command;
import io.libraft.agent.CommandDeserializer;
import io.libraft.agent.CommandSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Defines Jackson {@link JsonSerializer} and {@link JsonDeserializer}
 * implementations that transform both Jackson-annotated {@link Command} classes
 * and POJOs to/from the {@link io.libraft.agent.RaftAgent} JSON wire protocol.
 */
public abstract class RaftRPCCommand {

    private RaftRPCCommand() { } // to prevent instantiation

    /**
     * {@code JsonSerializer} that uses a user-supplied
     * {@link CommandSerializer} to convert a POJO {@link Command}
     * instance into a base-64-encoded string. This class must <strong>only</strong>
     * be used in conjunction with {@link RaftRPCCommand.Deserializer}. Both the
     * {@code CommandSerializer} and {@code CommandDeserializer} <strong>must</strong>
     * be set using {@link io.libraft.agent.protocol.RaftRPC#setupCustomCommandSerializationAndDeserialization(ObjectMapper, CommandSerializer, CommandDeserializer)}.
     */
    public static final class Serializer extends JsonSerializer<Command> {

        private final CommandSerializer commandSerializer;

        /**
         * Constructor.
         *
         * @param commandSerializer implementation of {@code CommandSerializer} that converts a
         *                          POJO {@link Command} instance into binary
         */
        public Serializer(CommandSerializer commandSerializer) {
            this.commandSerializer = commandSerializer;
        }

        // suddenly, the jackson errors make sense now
        // with my default implementation of the Serializer I was getting the following error:
        //     com.fasterxml.jackson.databind.JsonMappingException: Type id handling not implemented for type io.libraft.kayvee.command.KayVeeCommand$SetCommand (through reference chain: io.libraft.agent.protocol.AppendEntries["entries"]->java.util.ArrayList[0])
        //         at com.fasterxml.jackson.databind.JsonMappingException.wrapWithPath(JsonMappingException.java:232)
        //         at com.fasterxml.jackson.databind.JsonMappingException.wrapWithPath(JsonMappingException.java:211)
        //         at com.fasterxml.jackson.databind.ser.std.StdSerializer.wrapAndThrow(StdSerializer.java:210)
        //         at com.fasterxml.jackson.databind.ser.impl.IndexedListSerializer.serializeContentsUsing(IndexedListSerializer.java:130)
        //         at com.fasterxml.jackson.databind.ser.impl.IndexedListSerializer.serializeContents(IndexedListSerializer.java:69)
        //         at com.fasterxml.jackson.databind.ser.impl.IndexedListSerializer.serializeContents(IndexedListSerializer.java:21)
        //         at com.fasterxml.jackson.databind.ser.std.AsArraySerializerBase.serialize(AsArraySerializerBase.java:186)
        //         at com.fasterxml.jackson.databind.ser.BeanPropertyWriter.serializeAsField(BeanPropertyWriter.java:569)
        //         at com.fasterxml.jackson.databind.ser.std.BeanSerializerBase.serializeFields(BeanSerializerBase.java:597)
        //         at com.fasterxml.jackson.databind.ser.std.BeanSerializerBase.serializeWithType(BeanSerializerBase.java:492)
        //         at com.fasterxml.jackson.databind.ser.impl.TypeWrappedSerializer.serialize(TypeWrappedSerializer.java:35)
        //         at com.fasterxml.jackson.databind.ser.DefaultSerializerProvider.serializeValue(DefaultSerializerProvider.java:118)
        //         at com.fasterxml.jackson.databind.ObjectMapper._configAndWriteValue(ObjectMapper.java:2718)
        //         at com.fasterxml.jackson.databind.ObjectMapper.writeValue(ObjectMapper.java:2177)
        //         at io.libraft.kayvee.command.KayVeeCommandTest.shouldSerializeAndDeserializeKayVeeCommand(KayVeeCommandTest.java:75)
        //         ...
        //         at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)
        //         at com.intellij.rt.execution.application.AppMain.main(AppMain.java:120)
        //         Caused by: java.lang.UnsupportedOperationException: Type id handling not implemented for type io.libraft.kayvee.command.KayVeeCommand$SetCommand
        //         at com.fasterxml.jackson.databind.JsonSerializer.serializeWithType(JsonSerializer.java:142)
        //         at com.fasterxml.jackson.databind.ser.impl.TypeWrappedSerializer.serialize(TypeWrappedSerializer.java:35)
        //         at com.fasterxml.jackson.databind.SerializerProvider.defaultSerializeField(SerializerProvider.java:689)
        //         at io.libraft.agent.protocol.RaftRPCLogEntry$Serializer.serialize(RaftRPCLogEntry.java:73)
        //         at io.libraft.agent.protocol.RaftRPCLogEntry$Serializer.serialize(RaftRPCLogEntry.java:57)
        //         at com.fasterxml.jackson.databind.ser.impl.IndexedListSerializer.serializeContentsUsing(IndexedListSerializer.java:124)
        //         ... 39 more
        // if my command had the following annotations:
        //     @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
        //     @JsonSubTypes({@JsonSubTypes.Type(value = KayVeeCommand.SetCommand.class, name = "SET")})
        //
        // commenting out the annotations resolved the problem
        // These annotations override the default, which is to add the fully-qualified class name as the type id
        // during serialization. As far as I understand, if I do this, then my custom serializer _has to implement_
        // serializeWithType in order to encode this type information into the resulting JSON object. The examples
        // on which I based my serializer never overrode this method because they were never dealing with objects
        // with type annotations.
        //
        // Anyways, what I'm doing is dangerous because it completely throws away the type information encoded in the
        // jackson annotations for the object and simply serializes the command using the SerializableCommand.serialize()
        // call
        @SuppressWarnings("DuplicateThrows")
        @Override
        public void serializeWithType(Command command, JsonGenerator generator, SerializerProvider provider, TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
            throw new IllegalArgumentException("attempting to serialize jackson-type-annotated object with custom serializer");
        }

        @SuppressWarnings("DuplicateThrows")
        @Override
        public void serialize(Command command, JsonGenerator generator, SerializerProvider provider) throws IOException, JsonProcessingException {
            ByteArrayOutputStream bos = null;
            try {
                bos = new ByteArrayOutputStream();
                commandSerializer.serialize(command, bos);
                generator.writeBinary(bos.toByteArray());
            } finally {
                Closeables.close(bos, true);
            }
        }
    }

    /**
     * {@code JsonDeserializer} that uses a user-supplied
     * {@link CommandDeserializer} to convert a base-64-encoded string into a
     * POJO {@link Command} instance. This class must <strong>only</strong> be used in
     * conjunction with {@link RaftRPCCommand.Serializer}. Both the
     * {@code CommandSerializer} and {@code CommandDeserializer} must be set using
     * {@link io.libraft.agent.protocol.RaftRPC#setupCustomCommandSerializationAndDeserialization(ObjectMapper, CommandSerializer, CommandDeserializer)}.
     */
    public static final class Deserializer extends JsonDeserializer<Command> {

        private final CommandDeserializer commandDeserializer;

        /**
         * Constructor.
         *
         * @param commandDeserializer implementation of {@code CommandDeserializer} that converts binary into a POJO {@link Command} instance
         */
        public Deserializer(CommandDeserializer commandDeserializer) {
            this.commandDeserializer = commandDeserializer;
        }

        @SuppressWarnings("DuplicateThrows")
        @Override
        public Command deserialize(JsonParser parser, DeserializationContext context) throws IOException, JsonProcessingException {
            ByteArrayOutputStream bos = null;
            ByteArrayInputStream bosToDeserializerIn = null;
            try {
                bos = new ByteArrayOutputStream();
                parser.readBinaryValue(bos);

                bosToDeserializerIn = new ByteArrayInputStream(bos.toByteArray());
                return commandDeserializer.deserialize(bosToDeserializerIn);
            } finally {
                Closeables.close(bos, true);
                Closeables.close(bosToDeserializerIn, true);
            }
        }
    }

    /**
     * Implementation of a {@link com.fasterxml.jackson.databind.JsonDeserializer}
     * that deserializes json into a Jackson-annotated class.
     * <p/>
     * This class must <strong>only</strong> used in conjunction
     * with Jackson-based serialization. It must <strong>not</strong>
     * be used for classes for which a user explicitly sets a
     * {@link CommandSerializer} via
     * {@link RaftRPC#setupCustomCommandSerializationAndDeserialization(ObjectMapper, CommandSerializer, CommandDeserializer)}.
     *
     * @param <CommandSubclass> the base-class type of the Jackson-annotated {@code Command} classes
     *
     * @see io.libraft.Command
     */
    static class PassThroughDeserializer<CommandSubclass extends Command> extends JsonDeserializer<CommandSubclass> {

        private final Class<CommandSubclass> commandSubclassKlass;

        /**
         * Constructor.
         *
         * @param commandSubclassKlass the base-class of the Jackson-annotated {@code Command} classes
         */
        PassThroughDeserializer(Class<CommandSubclass> commandSubclassKlass) {
            this.commandSubclassKlass = commandSubclassKlass;
        }

        @SuppressWarnings("DuplicateThrows")
        @Override
        public CommandSubclass deserialize(JsonParser parser, DeserializationContext context) throws IOException, JsonProcessingException {
            ObjectCodec codec = parser.getCodec();
            return codec.treeToValue(parser.readValueAsTree(), commandSubclassKlass);
        }
    }
}