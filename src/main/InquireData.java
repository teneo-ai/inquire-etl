/*
 * ARTIFICIAL SOLUTIONS HAS PROVIDED THIS CODE 'AS IS' AND MAKES NO REPRESENTATION, WARRANTY, ASSURANCE, GUARANTEE OR INDUCEMENT, WHETHER EXPRESS,
 * IMPLIED OR OTHERWISE, REGARDING ITS ACCURACY, COMPLETENESS OR PERFORMANCE.
 *
 */
package main;

import com.artisol.teneo.inquire.api.models.SharedQuery;
import com.artisol.teneo.inquire.client.TeneoInquireClient;
import com.artisol.teneo.inquire.client.resources.TqlResourceImpl;
import com.artisol.teneo.inquire.client.Client;

import java.io.OutputStream;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.net.URL;


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
     * @param lds_name - The name of the Log Data Source in Inquire
     * @param timeout - Communications timeout, defaults to 30
     * @param esPageSize - The esPageSize value as per
     * https://developers.teneo.ai/documentation/7.2.0/swagger/teneo-inquire/swagger/index.html#/tql/submitQuery
     * @return resultMap - A map containing the queried Data
     */
    public static HashMap<String, Iterable<Map<String, Object>>> get(String queryName, String dateFrom, String dateTo, String backend_URL, String username, String password, String lds_name, String timeout, String esPageSize) {

        try {
            System.out.println("Starting processing data from " + backend_URL + " at " + new Date());

            // Login before running reports:
            //QueryClient clientES = QueryClient.create(backend_URL);
            //TeneoInquireClient clientES = TeneoInquireClient.create(backend_URL);
            //Client clientES = Client.create(backend_URL);

            LinkedHashMap<String, Iterable<Map<String, Object>>> resultsMap = new LinkedHashMap<>();
            
            TeneoInquireClient clientES = new TeneoInquireClient(new URL(backend_URL));
            clientES.getAuth().login(username, password);
            List<SharedQuery> sharedQueries = clientES.getTql().getSharedQueries(lds_name);
            for (final SharedQuery publishedQuery : sharedQueries) {
                final String publishedQueryName = publishedQuery.getPublishedName();
                // Does not run queries that do not match the provided query name (unless "all").
                // It will also filter out the usage queries used by billing to monitor usage.
                if ((queryName.equalsIgnoreCase(publishedQueryName) || queryName.equalsIgnoreCase("all")) && !excludedQueryNamePattern.matcher(publishedQueryName).matches()) {
                    //Sleep between requests to avoid overwhelming Inquire
                    Thread.sleep(1000);
                    Iterable<Map<String, Object>> results = runTqlQuery(clientES, lds_name, publishedQuery.getQuery(), dateFrom, dateTo, timeout, esPageSize, publishedQueryName);
                    resultsMap.put(publishedQuery.getPublishedName(), results);
                }
            }
            /*

            Iterable<Map<String, Object>> results;

            // Get all shared/published queries
            Collection<SharedQuery> sharedQueries = clientES.getSharedQueries(lds_name);

            //Iterate over every shared query and make request
            for (SharedQuery publishedQuery : sharedQueries) {
                String publishedQueryName = publishedQuery.getPublishedName();
                //Does not run queries that do not match the provided query name (unless "all"). It will also filter out the usage queries used by billing to monitor usage.
                if ((queryName.equalsIgnoreCase(publishedQueryName) ||
                        queryName.equalsIgnoreCase("all")) &&
                        !publishedQueryName.matches("(?i)(usage_([–\\-])?_*)(transactions|interactions|sessions|standard_usage)")
                ) {
                    //Sleep between requests to avoid overwhelming Inquire
                    Thread.sleep(1000);
                    results = runTqlQuery(clientES, lds_name, publishedQuery.getQuery(), dateFrom, dateTo, timeout, publishedQueryName);
                    resultsMap.put(publishedQuery.getPublishedName(), results);
                }
            }

        //Close the Inquire Client
            clientES.close();
            System.out.println("Finished fetching query data at " + new Date());
            */
            return resultsMap;

        } catch (Exception e) {
            System.out.println("ERROR: An error has occurred while processing LDS " + lds_name + ". Will continue processing the others, please run this one again.");
            System.out.println(e.getMessage());
        }
        return null;
    }

    /**
     *
     * @param clientES - The Inquire Client instance
     * @param lds_name - Log Data Soruce Name
     * @param qry - The query extracted from the shared query
     * @param from - Time limit
     * @param to - Time limit
     * @param timeout - Seconds before giving up on connection
     * @param esPageSize - The esPageSize value as per
     * https://developers.teneo.ai/documentation/7.2.0/swagger/teneo-inquire/swagger/index.html#/tql/submitQuery
     * @param qryName - Name of the shared query
     * @return map with query result values
     * @throws Exception Something went wrong
     */
    private static Iterable<Map<String, Object>> runTqlQuery(TeneoInquireClient clientES, String lds_name, String qry, String from, String to, String timeout, String esPageSize, String qryName) throws Exception {
        /*
        System.setErr(new PrintStream(new OutputStream() {
            public void write(int b) {
            }
        }));
        */
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
        // https://developers.teneo.ai/documentation/7.2.0/swagger/teneo-inquire/swagger/index.html#/tql/submitQuery
        if (esPageSize != null && esPageSize.length() > 0) params.put("esPageSize", esPageSize);

        params.put("timeout", timeout);

        System.out.println("Running query: " + qryName + "\nWith params: " + params);
        
        // results = clientES.executeQuery(lds_name, qry, params);
        
        TqlResourceImpl.QueryPoller queryPoller = clientES.getTql().submitSharedQuery(lds_name, qryName, params);
        while (!queryPoller.poll()) {
        }
        final Iterable<Map<String, Object>> results = queryPoller.getResults();
        System.out.println("Query " + qryName + " finished\n");
        return results;
    }
}
