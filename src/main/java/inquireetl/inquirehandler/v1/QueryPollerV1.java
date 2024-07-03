package inquireetl.inquirehandler.v1;

import inquireetl.inquirehandler.AbstractPoller;
import inquireetl.inquirehandler.AuthorizationFilter;
import inquireetl.inquirehandler.v1.models.AbstractQueryResultMessage;
import inquireetl.inquirehandler.v1.models.ExecutionConfiguration;
import inquireetl.inquirehandler.v1.models.FinalResultMessage;
import inquireetl.inquirehandler.v1.models.StartExecutionMessage;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.util.Map;

public class QueryPollerV1 extends AbstractPoller {

    private AbstractQueryResultMessage lastMessage;

    public QueryPollerV1(WebTarget webTarget, AuthorizationFilter authorizationFilter, AbstractQueryResultMessage lastMessage, Object timeout) {
        super(webTarget, authorizationFilter);
        this.lastMessage = lastMessage;
        this.timeout = timeout;
    }

    @Override
    public Iterable<Map<String, Object>> getResults() {
        return lastMessage.getResult();
    }

    @Override
    public AbstractQueryResultMessage parseResponse(Response pollResponse) throws Exception {
        return InquireHandlerV1.parseResponse(pollResponse);
    }

    @Override
    public boolean poll() throws Exception {

        if (isFinished()) {
            return true;
        }

        WebTarget wt = webTarget.path("tql/poll").queryParam("id", lastMessage.getId());
        if (timeout != null) {
            wt = wt.queryParam("timeout", timeout);
        }
        Response pollResponse = doGet(wt);
        lastMessage = parseResponse(pollResponse);

        return isFinished();
    }

    public boolean isFinished() {

        if (lastMessage instanceof FinalResultMessage) {
            return true;
        }

        if (!(lastMessage instanceof StartExecutionMessage)) {
            return false;
        }

        StartExecutionMessage se = (StartExecutionMessage) lastMessage;
        return se.getExecutionConfiguration().getTimeEstimate().equals(ExecutionConfiguration.TimeEstimate.immediate);
    }

}
