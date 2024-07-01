package main.InquireHandler.v1.models;

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
