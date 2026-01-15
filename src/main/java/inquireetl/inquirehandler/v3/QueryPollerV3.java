package inquireetl.inquirehandler.v3;

import inquireetl.inquirehandler.AbstractPoller;
import inquireetl.inquirehandler.AbstractQueryResultMessage;
import inquireetl.inquirehandler.AuthorizationFilter;
import inquireetl.inquirehandler.v3.models.ExecutionConfiguration;
import inquireetl.inquirehandler.v3.models.FinalResultMessage;
import inquireetl.inquirehandler.v3.models.StartExecutionMessage;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.util.Map;

public class QueryPollerV3 extends AbstractPoller {

    private AbstractQueryResultMessage lastMessage;

    public QueryPollerV3(WebTarget webTarget, AuthorizationFilter authorizationFilter, AbstractQueryResultMessage lastMessage, Object timeout) {
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
        return InquireHandlerV3.parseResponse(pollResponse);
    }

    @Override
    public boolean poll() throws Exception {

        if (isFinished()) {
            return true;
        }

        WebTarget wt = webTarget.path("/v3/tql/poll/" + lastMessage.getId());
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
