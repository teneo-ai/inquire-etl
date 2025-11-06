package inquireetl.inquirehandler.v2.models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

// As.EXISTING_PROPERTY avoids duplication of "type" field when serializing models, since AbstractQueryMessage also has it as a class field
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", include = As.EXISTING_PROPERTY)

@JsonSubTypes({
        @Type(value = FailureMessage.class, name = "FailureMessage"),
        @Type(value = FinalResultMessage.class, name = "FinalResultMessage"),
        @Type(value = PartialUpdateMessage.class, name = "PartialUpdateMessage"),
        @Type(value = StartExecutionMessage.class, name = "StartExecutionMessage")
})
public interface Message {
}

