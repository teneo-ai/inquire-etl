package inquireetl.inquirehandler.v1.models;

public class ExecutionProgress {

    private long seenResults;
    private long seenSessions;
    private long totalSessions;
    private int pagesProcessed;
    private int estimatedPages;
    private long lastExecutionTime;
    private double meanExecutionTime;
    private double executionTimeVariance;

    public ExecutionProgress() {
    }

    public void setSeenResults(long seenResults) {
        this.seenResults = seenResults;
    }

    public void setSeenSessions(long seenSessions) {
        this.seenSessions = seenSessions;
    }

    public void setTotalSessions(long totalSessions) {
        this.totalSessions = totalSessions;
    }

    public void setPagesProcessed(int pagesProcessed) {
        this.pagesProcessed = pagesProcessed;
    }

    public void setEstimatedPages(int estimatedPages) {
        this.estimatedPages = estimatedPages;
    }

    public void setLastExecutionTime(long lastExecutionTime) {
        this.lastExecutionTime = lastExecutionTime;
    }

    public void setMeanExecutionTime(double meanExecutionTime) {
        this.meanExecutionTime = meanExecutionTime;
    }

    public void setExecutionTimeVariance(double executionTimeVariance) {
        this.executionTimeVariance = executionTimeVariance;
    }
}
