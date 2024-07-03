package main.inquirehandler.v1.models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

// As.EXISTING_PROPERTY avoids duplication of "type" field when serializing models, since AbstractQueryMessage also has it as a class field
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", include = As.EXISTING_PROPERTY)

// Duplication of the names for each message type is done on purpose, since for the time being deserialization of these messages must be compatible
// with old and new values. Look at newMessageTypes configuration property on the server.
@JsonSubTypes({
        @Type(value = FailureMessage.class, name = "failure"),
        @Type(value = FailureMessage.class, name = "FailureMessage"),
        @Type(value = FinalResultMessage.class, name = "final"),
        @Type(value = FinalResultMessage.class, name = "FinalResultMessage"),
        @Type(value = PartialUpdateMessage.class, name = "partial"),
        @Type(value = PartialUpdateMessage.class, name = "PartialUpdateMessage"),
        @Type(value = StartExecutionMessage.class, name = "start"),
        @Type(value = StartExecutionMessage.class, name = "StartExecutionMessage")
})
public interface Message {
}

