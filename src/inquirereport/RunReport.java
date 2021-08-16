/*
 * ARTIFICIAL SOLUTIONS HAS PROVIDED THIS CODE 'AS IS' AND MAKES NO REPRESENTATION, WARRANTY, ASSURANCE, GUARANTEE OR INDUCEMENT, WHETHER EXPRESS,
 * IMPLIED OR OTHERWISE, REGARDING ITS ACCURACY, COMPLETENESS OR PERFORMANCE.
 *
 */
package inquirereport;

import com.artisol.teneo.inquire.api.models.SharedQuery;
import com.artisol.teneo.inquire.client.QueryClient;

import java.util.*;


public class RunReport {


    public static HashMap<String, Iterable<Map<String, Object>>> start(String queryName, String dateFrom, String dateTo, String backend_URL, String username, String password, String lds_name, String timeout) {

        try {
            System.out.println("Starting processing data from " + backend_URL + " at " + new Date());

            // Login before running reports
            QueryClient clientES = QueryClient.create(backend_URL);
            clientES.login(username, password);


            Iterable<Map<String, Object>> results;

            // Export all shared/published queries
            Collection<SharedQuery> sharedQueries = clientES.getSharedQueries(lds_name);
            ArrayList<String> usageQueries = new ArrayList<>();
            usageQueries.add("Usage_–_Transactions");
            usageQueries.add("Usage_–_Sessions");
            usageQueries.add("Usage_–_Standard_Usage");

            HashMap<String, Iterable<Map<String, Object>>> resultsMap = new HashMap<>();

            for (SharedQuery publishedQuery : sharedQueries) {
                String publishedQueryName = publishedQuery.getPublishedName();
                Thread.sleep(1000);
                if ((queryName.equalsIgnoreCase(publishedQueryName) || queryName.equalsIgnoreCase("all")) && !usageQueries.contains(publishedQueryName)) {
                    results = PublishedQuery.getOutput(clientES, lds_name, publishedQuery.getQuery(), dateFrom, dateTo, timeout);
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

}
