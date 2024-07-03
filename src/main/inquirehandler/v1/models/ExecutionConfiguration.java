package main.inquirehandler.v1.models;

public class ExecutionConfiguration {

    private boolean commandExecutionOnly = false;
    private boolean executeCommandIteratively = true;
    private TimeEstimate timeEstimate;
    private boolean limitDuringExecution = true;

    public ExecutionConfiguration() {
    }

    public void setCommandExecutionOnly(boolean commandExecutionOnly) {
        this.commandExecutionOnly = commandExecutionOnly;
    }

    public void setExecuteCommandIteratively(boolean executeCommandIteratively) {
        this.executeCommandIteratively = executeCommandIteratively;
    }

    public void setTimeEstimate(TimeEstimate timeEstimate) {
        this.timeEstimate = timeEstimate;
    }

    public TimeEstimate getTimeEstimate() {
        return timeEstimate;
    }

    public void setLimitDuringExecution(boolean limitDuringExecution) {
        this.limitDuringExecution = limitDuringExecution;
    }

    public enum TimeEstimate {
        immediate,
        fast,
        slow
    }
}
