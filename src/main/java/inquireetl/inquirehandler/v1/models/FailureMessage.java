package inquireetl.inquirehandler.v1.models;

public class FailureMessage extends AbstractQueryMessage {

    private String errorMessage;
    private Exception exception;
    private String stacktrace;

    public FailureMessage() {
        super();
        this.error = true;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public void setStacktrace(String stacktrace) {
        this.stacktrace = stacktrace;
    }
}
