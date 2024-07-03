package main.inquirehandler.v1.models;

public class StartExecutionMessage extends AbstractQueryResultMessage {

    private String phases;
    private String mainCommand;
    private ExecutionConfiguration executionConfiguration;

    public StartExecutionMessage() {
        super();
    }

    public ExecutionConfiguration getExecutionConfiguration() {
        return executionConfiguration;
    }


    public void setExecutionConfiguration(ExecutionConfiguration executionConfiguration) {
        this.executionConfiguration = executionConfiguration;
    }

    public void setPhases(String phases) {
        this.phases = phases;
    }

    public void setMainCommand(String mainCommand) {
        this.mainCommand = mainCommand;
    }
}
