package inquireetl.inquirehandler.v2.models;

import inquireetl.inquirehandler.AbstractQueryResultMessage;

public class PartialUpdateMessage extends AbstractQueryResultMessage {

    private ExecutionProgress progress;
    private AggregationMethod aggregationMethod;

    public PartialUpdateMessage() {
        super();
    }

    public void setProgress(ExecutionProgress progress) {
        this.progress = progress;
    }

    public void setAggregationMethod(AggregationMethod aggregationMethod) {
        this.aggregationMethod = aggregationMethod;
    }
}
