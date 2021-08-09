/*
 * ARTIFICIAL SOLUTIONS HAS PROVIDED THIS CODE 'AS IS' AND MAKES NO REPRESENTATION, WARRANTY, ASSURANCE, GUARANTEE OR INDUCEMENT, WHETHER EXPRESS,
 * IMPLIED OR OTHERWISE, REGARDING ITS ACCURACY, COMPLETENESS OR PERFORMANCE.
 *
 */
package inquirereport;

import com.artisol.teneo.inquire.api.models.SharedQuery;
import com.artisol.teneo.inquire.client.*;

import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.util.Collection;


public class RunReport {


    public static void start(String queryName, String outputFolder, String dateFrom, String dateTo, String backend_URL, String username, String password, String lds_name, String separator, String timeout) {

        try {
            System.out.println("Starting Writing files to " + outputFolder + " at " + new Date());

            // Login before running reports
            QueryClient clientES = QueryClient.create(backend_URL);
            clientES.login(username, password);

            // Have all information, now run appropriate reports
            String reportSuffix = "_";
            if (!(dateFrom == null || dateTo == null)) {
                reportSuffix += dateFrom + "_" + dateTo;
            } else {
                reportSuffix += "all";
            }

            Iterable<Map<String, Object>> results;

            // Export all shared/published queries
            Collection<SharedQuery> sharedQueries = clientES.getSharedQueries(lds_name);
            ArrayList<String> usageQueries = new ArrayList<String>();
            usageQueries.add("Usage_–_Transactions");
            usageQueries.add("Usage_–_Sessions");
            usageQueries.add("Usage_–_Standard_Usage");

            for (SharedQuery publishedQuery : sharedQueries) {
                String publishedQueryName = publishedQuery.getPublishedName();
                Thread.sleep(1000);
                if ((queryName.equalsIgnoreCase(publishedQueryName) || queryName.equalsIgnoreCase("all")) && !usageQueries.contains(publishedQueryName)) {
                    results = PublishedQuery.getOutput(clientES, lds_name, publishedQuery.getQuery(), dateFrom, dateTo, timeout);
                    System.out.println(results);
                    RunReport.fileOutput(separator, results, publishedQuery.getPublishedName() + reportSuffix, outputFolder);
                }
            }


            clientES.close();
            System.out.println("Finished Writing files to " + outputFolder + " at " + new Date());

        } catch (Exception e) {
            System.out.println("ERROR: An error has occurred while processing LDS " + lds_name + ". Will continue processing the others, please run this one again.");
            System.out.println(e.getMessage());
            System.out.println(e);
        }
    }

    private static String fileOutput(String fileFormat, Iterable<Map<String, Object>> results, String fileName, String path) {

        //replace bad chars in filename
        fileName = sanitizeFilename(fileName);

        // create the output folder if it does not exist
        File directory = new File(String.valueOf(path));
        if (!directory.exists()) {
            System.out.println("Making directory " + directory.getPath());
            directory.mkdirs();
        }

        String outputFileName = fileName;
        if (fileFormat.equalsIgnoreCase("json")) {
            outputFileName = path.concat(outputFileName).concat(".json");

        } else {
            outputFileName = path.concat(outputFileName).concat(".txt");
        }

        System.out.println("Writing to " + outputFileName + " ...");

        if (fileFormat.equalsIgnoreCase("json")) {
            // Write out to JSON
            writeResultstoJson(results, outputFileName);
        } else {
            // Write out to text files
            writeResultstoText(results, outputFileName, fileFormat);
        }
        return outputFileName;
    }

    private static void writeResultstoJson(Iterable<Map<String, Object>> results, String fullFileName) {

        ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.writeValue(new File(fullFileName), results);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void writeResultstoText(Iterable<Map<String, Object>> results,
                                           String fullFileName,
                                           String separator) {
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(fullFileName), "utf-8"))) {

            boolean headerDisplayed = false;
            for (Map<String, Object> map : results) {
                List header = new ArrayList();
                List row = new ArrayList();
                map.entrySet().stream().forEach((entry) -> {
                    header.add("\"" + entry.getKey() + "\"");
                    row.add("\"" + entry.getValue().toString().replace("\"", "\"\"") + "\"");
                });
                if (!headerDisplayed) {
                    writer.write(String.join(separator, header) + System.getProperty("line.separator"));
                    headerDisplayed = true;
                }
                writer.write(String.join(separator, row) + System.getProperty("line.separator"));
            }

            System.out.println("Writing to " + fullFileName + " complete.");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static String sanitizeFilename(String name) {
        return name.replaceAll("[:\\\\/*?|<>]", "_");
    }


}
