/*
 * ARTIFICIAL SOLUTIONS HAS PROVIDED THIS CODE 'AS IS' AND MAKES NO REPRESENTATION, WARRANTY, ASSURANCE, GUARANTEE OR INDUCEMENT, WHETHER EXPRESS,
 * IMPLIED OR OTHERWISE, REGARDING ITS ACCURACY, COMPLETENESS OR PERFORMANCE.
 *
 */
package inquireetl;

import inquireetl.inquirehandler.AbstractInquireHandler;
import inquireetl.inquirehandler.AbstractPoller;
import inquireetl.inquirehandler.v1.InquireHandlerV1;
import inquireetl.inquirehandler.v2.InquireHandlerV2;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;


public class InquireData {
    
    private static final Pattern excludedQueryNamePattern = Pattern.compile("(?i)(usage_([–\\-])?_*)(transactions|interactions|sessions|standard_usage)");

    /**
     * All values have already been assigned to default in Main if no user-provided value is found.
     * @param queryName - Will default to "all" if no command line argument is passed. If it is, it will only get the results of that one named query.
     * @param dateFrom - Further filter the query results for the date range. NOTE -> This will not cause a query to change the time limitations it already has, but will filter on the results of the query. Formats yyyy-MM-ddTHH:mm:ssZ or yyyy-MM-dd.
     * @param dateTo - Same as dateFrom
     * @param backend_URL - The address of the Inquire Endpoint
     * @param username - The username that is allowed to query shared queries in Inquire
     * @param password - The password for the user above
     * @param apiToken - The apiToken that is used if username and password is not being used.
     * @param lds_name - The name of the Log Data Source in Inquire
     * @param timeout - Communications timeout, defaults to 30
     * @param esPageSize - The esPageSize value as per
     * https://developers.teneo.ai/documentation/7.4.0/swagger/teneo-inquire/swagger/index.html#/tql/submitQuery
     * @param apiVersion - The Inquire API version to use.
     * @return resultMap - A map containing the queried Data
     */
    public static HashMap<String, Iterable<Map<String, Object>>> get(String queryName, String dateFrom, String dateTo, String backend_URL, String username, String password, String apiToken, String lds_name, String timeout, String esPageSize, Integer apiVersion) {

        try {
            System.out.println("Starting processing data from " + backend_URL + " at " + new Date());
            LinkedHashMap<String, Iterable<Map<String, Object>>> resultsMap = new LinkedHashMap<>();
            AbstractInquireHandler inquireHandler = null;
            if (apiVersion == null || apiVersion.equals(1)) {
                inquireHandler = new InquireHandlerV1(new URL(backend_URL), apiToken);
                if (apiToken == null) {
                    inquireHandler.login(username, password);
                }
                List<inquireetl.inquirehandler.v1.models.SharedQuery> sharedQueries = ((InquireHandlerV1) inquireHandler).getSharedQueries(lds_name);
                for (final inquireetl.inquirehandler.v1.models.SharedQuery publishedQuery : sharedQueries) {
                    // Does not run queries that do not match the provided query name (unless "all").
                    // It will also filter out the usage queries used by billing to monitor usage.
                    generateResults(queryName, dateFrom, dateTo, lds_name, timeout, esPageSize, resultsMap, inquireHandler, publishedQuery.getPublishedName());
                }
            }
            else if (apiVersion.equals(2)) {
                inquireHandler = new InquireHandlerV2(new URL(backend_URL), apiToken);
                if (apiToken == null) {
                    inquireHandler.login(username, password);
                }
                List<inquireetl.inquirehandler.v2.models.SharedQuery> sharedQueries = ((InquireHandlerV2) inquireHandler).getSharedQueries(lds_name);
                for (final inquireetl.inquirehandler.v2.models.SharedQuery publishedQuery : sharedQueries) {
                    // Does not run queries that do not match the provided query name (unless "all").
                    // It will also filter out the usage queries used by billing to monitor usage.
                    generateResults(queryName, dateFrom, dateTo, lds_name, timeout, esPageSize, resultsMap, inquireHandler, publishedQuery.getPublishedName());
                }
            }
            if (apiToken != null) {
                inquireHandler.logout();
            }
            return resultsMap;

        } catch (Exception e) {
            System.out.println("ERROR: An error has occurred while processing LDS " + lds_name + ". Will continue processing the others, please run this one again.");
            System.out.println(e.getMessage());
        }
        return null;
    }

    private static void generateResults(String queryName, String dateFrom, String dateTo, String lds_name, String timeout, String esPageSize, LinkedHashMap<String, Iterable<Map<String, Object>>> resultsMap, AbstractInquireHandler inquireHandler, String publishedQueryName) {
        if ((queryName.equalsIgnoreCase(publishedQueryName) || queryName.equalsIgnoreCase("all")) && !excludedQueryNamePattern.matcher(publishedQueryName).matches()) {
            try {
                Iterable<Map<String, Object>> results = runTqlQuery(inquireHandler, lds_name, dateFrom, dateTo, timeout, esPageSize, publishedQueryName);
                resultsMap.put(publishedQueryName, results);
            } catch (Exception ex) {
                System.out.println("ERROR: An error has occurred while processing query " + queryName + ". Will continue processing the other queries, please run this one again.");
                System.out.println(ex.getMessage());
            }
        }
    }

    /**
     *
     * @param lds_name - Log Data Soruce Name
     * @param from - Time limit
     * @param to - Time limit
     * @param timeout - Seconds before giving up on connection
     * @param esPageSize - The esPageSize value as per
     * https://developers.teneo.ai/documentation/7.4.0/swagger/teneo-inquire/swagger/index.html#/tql/submitQuery
     * @param qryName - Name of the shared query
     * @return map with query result values
     * @throws Exception Something went wrong
     */
    private static Iterable<Map<String, Object>> runTqlQuery(AbstractInquireHandler handler, String lds_name, String from, String to, String timeout, String esPageSize, String qryName) throws Exception {
        LinkedHashMap<String, Object> params = new LinkedHashMap<>();
        // Setup dates if used
        DateFormat format = null;
        if (from != null) {
            format = new SimpleDateFormat((from.length() == 10 ? "yyyy-MM-dd" : "yyyy-MM-dd'T'HH:mm:ss'Z'"), Locale.ENGLISH);
            final String dateFromUtc = Long.toString(format.parse(from).getTime());
            params.put("from", dateFromUtc);
        }
        if (to != null) {
            final String dateToUtc = Long.toString(Objects.requireNonNull(format).parse(to).getTime());
            params.put("to", dateToUtc);
        }

        // SDCS-54
        // https://artificialsolutions.atlassian.net/browse/SDCS-54?focusedCommentId=73948
        // adding esPageSize to params as per
        // https://developers.teneo.ai/documentation/7.4.0/swagger/teneo-inquire/swagger/index.html#/tql/submitQuery
        if (esPageSize != null && esPageSize.length() > 0) params.put("esPageSize", esPageSize);

        params.put("timeout", timeout);

        System.out.println("Running query: " + qryName + "\nWith params: " + params);

        AbstractPoller poller = handler.submitSharedQuery(lds_name, qryName, params);
        while (!poller.poll()) {
        }
        final Iterable<Map<String, Object>> results = poller.getResults();
        System.out.println("Query " + qryName + " finished\n");
        return results;
    }
}
