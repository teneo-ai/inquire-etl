/*
 * ARTIFICIAL SOLUTIONS HAS PROVIDED THIS CODE 'AS IS' AND MAKES NO REPRESENTATION, WARRANTY, ASSURANCE, GUARANTEE OR INDUCEMENT, WHETHER EXPRESS,
 * IMPLIED OR OTHERWISE, REGARDING ITS ACCURACY, COMPLETENESS OR PERFORMANCE.
 *
 */
package inquirereport;

import com.artisol.teneo.inquire.api.models.SharedQuery;
import com.artisol.teneo.inquire.client.QueryClient;

import java.io.OutputStream;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


public class InquireData {


    public static HashMap<String, Iterable<Map<String, Object>>> get(String queryName, String dateFrom, String dateTo, String backend_URL, String username, String password, String lds_name, String timeout) {

        try {
            System.out.println("Starting processing data from " + backend_URL + " at " + new Date());

            // Login before running reports
            QueryClient clientES = QueryClient.create(backend_URL);
            clientES.login(username, password);


            Iterable<Map<String, Object>> results;

            // Export all shared/published queries
            Collection<SharedQuery> sharedQueries = clientES.getSharedQueries(lds_name);


            HashMap<String, Iterable<Map<String, Object>>> resultsMap = new HashMap<>();

            for (SharedQuery publishedQuery : sharedQueries) {
                String publishedQueryName = publishedQuery.getPublishedName();
                Thread.sleep(1000);
                if ((queryName.equalsIgnoreCase(publishedQueryName) || queryName.equalsIgnoreCase("all")) && !publishedQueryName.matches("(?i)(usage_([–\\-])?_*)(transactions|interactions|sessions|standard_usage)")) {
                    results = runTqlQuery(clientES, lds_name, publishedQuery.getQuery(), dateFrom, dateTo, timeout);
                    System.out.println(results);
                    resultsMap.put(publishedQuery.getPublishedName(), results);
                }
            }


            clientES.close();
            System.out.println("Finished fetching query data at " + new Date());
            return resultsMap;

        } catch (Exception e) {
            System.out.println("ERROR: An error has occurred while processing LDS " + lds_name + ". Will continue processing the others, please run this one again.");
            System.out.println(e.getMessage());
        }
        return null;
    }

    public static Iterable<Map<String, Object>> runTqlQuery(QueryClient clientES, String lds_name, String qry, String from, String to, String timeout) throws Exception {

        Iterable<Map<String, Object>> results;

        System.setErr(new PrintStream(new OutputStream() {
            public void write(int b) {
            }
        }));

        // Setup dates if used
        LinkedHashMap<String, Object> params = new LinkedHashMap<>();
        String dateFromUtc;
        String dateToUtc;

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
        if (from != null) {
            dateFromUtc = Long.toString(format.parse(from).getTime());
            params.put("from", dateFromUtc);
        }

        if (to != null) {
            dateToUtc = Long.toString(format.parse(to).getTime());
            params.put("to", dateToUtc);
        }

        params.put("timeout",timeout);

        results = clientES.executeQuery(lds_name, qry, params);

        return results;

    }

}
