/*
 * ARTIFICIAL SOLUTIONS HAS PROVIDED THIS CODE 'AS IS' AND MAKES NO REPRESENTATION, WARRANTY, ASSURANCE, GUARANTEE OR INDUCEMENT, WHETHER EXPRESS, 
 * IMPLIED OR OTHERWISE, REGARDING ITS ACCURACY, COMPLETENESS OR PERFORMANCE.
 * 
 */
package inquirereport;

import com.artisol.teneo.inquire.client.QueryClient;

import java.util.Map;

/**
 * This class runs a TQL query and returns the results: 
 * 
 * @author Mark Jones
 */
public class PublishedQuery {

    public static Iterable<Map<String, Object>> getOutput(QueryClient clientES, String lds_name, String qry, String from, String to, String timeout) throws Exception {

        Iterable<Map<String, Object>> results = RunTqlQuery.getOutput(clientES, lds_name, qry, from, to, timeout);
        return results;

    }
}
