/*
 * ARTIFICIAL SOLUTIONS HAS PROVIDED THIS CODE 'AS IS' AND MAKES NO REPRESENTATION, WARRANTY, ASSURANCE, GUARANTEE OR INDUCEMENT, WHETHER EXPRESS, 
 * IMPLIED OR OTHERWISE, REGARDING ITS ACCURACY, COMPLETENESS OR PERFORMANCE.
 * 
 */
package inquirereport;

import com.artisol.teneo.inquire.client.*;
import java.io.OutputStream;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * This is a generic class that takes an Inquire client instance, a query string and submits the request to the server. 
 * 
 * @author Mark Jones
 */
public class RunTqlQuery {

    public static Iterable<Map<String, Object>> getOutput(QueryClient clientES, String lds_name, String qry, String from, String to, String timeout) throws Exception {

        Iterable<Map<String, Object>> results = null;

        try {

            System.setErr(new PrintStream(new OutputStream() {
                public void write(int b) {
                }
            }));

            // Setup dates if used
            LinkedHashMap params = new LinkedHashMap();
            String dateFromUtc;
            String dateToUtc;

            DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
            if (from != null) {
                dateFromUtc = Long.toString(((Date) format.parse(from)).getTime());
                params.put("from", dateFromUtc);
            }

            if (to != null) {
                dateToUtc = Long.toString(((Date) format.parse(to)).getTime());
                params.put("to", dateToUtc);
            }
            
            params.put("timeout",timeout);

            results = clientES.executeQuery(lds_name, qry, params);
        } catch (Exception e) {
            throw e;
        }

        return results;

    }

}
