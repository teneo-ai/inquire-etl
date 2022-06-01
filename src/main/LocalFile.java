package main;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class LocalFile {
    
    private static final Pattern whitespaces = Pattern.compile("\\s+", Pattern.MULTILINE); 

    /**
     *
     * @param separator The character that will be used to separate the data points.
     *                  If "json" is received it will generate a JSON file.
     *                  If "," is received, a CSV file is created.
     *                  Else a *.txt file is created.
     * @param results The contents of the query response from Inquire.
     * @param path The folder where the files will be created.
     *             The method recursively create directories within the output directory with the name of the configuration file.
     *             For example, a file called carwash_config.properties will result in /outputDir/carwash/queryname.extension
     */

    static void output(String separator, Map.Entry<String, Iterable<Map<String, Object>>> results, String path) {

        //replace bad chars in filename
        String fileName = sanitizeFilename(results.getKey());

        // create the output folder if it does not exist.
        File directory = new File(String.valueOf(path));
        if (!directory.exists()) {
            boolean mkdirsSuccess = directory.mkdirs();
            System.out.println((mkdirsSuccess ? "Success making " : "Failed to make ") + " directory: " + directory.getPath());
        }

        //This will assign the extension to the file according to the extension
        String outputFileName = fileName;
        if (separator.equalsIgnoreCase("json")) {
            outputFileName = path.concat(outputFileName).concat(".json");

        } else if (separator.equalsIgnoreCase(",")) {
            outputFileName = path.concat(outputFileName).concat(".csv");
        } else {
            outputFileName = path.concat(outputFileName).concat(".txt");
        }

        System.out.println("Writing to " + outputFileName + " ...");

        if (separator.equalsIgnoreCase("json")) {
            // Write out to JSON
            writeResultsToJson(results.getValue(), outputFileName);
        } else {
            // Write out to text files
            writeResultstoText(results.getValue(), outputFileName, separator);
        }
    }

    private static void writeResultsToJson(Iterable<Map<String, Object>> results, String fullFileName) {

        ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.writeValue(new File(fullFileName), results);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void writeResultstoText(Iterable<Map<String, Object>> results, String fullFileName, String separator) {
        //try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fullFileName), StandardCharsets.UTF_8))) {
        try (Writer writer = new BufferedWriter(new FileWriter(fullFileName, StandardCharsets.UTF_8))) {
            final String lineSeparator = System.getProperty("line.separator");
            List<String> header = new ArrayList<>();
            final List<String> row = new ArrayList<>();
            for (final Map<String, Object> map : results) {
                for (final Map.Entry<String, Object> entry : map.entrySet()) {
                    if (header != null) header.add("\"" + entry.getKey().trim().replace("\"", "'") + '"');
                    final String cell = whitespaces.matcher(entry.getValue().toString()).replaceAll(" ").trim();
                    row.add(cell.isEmpty() ? "\"\"" : ("\"" + cell.replace("\"", "'") + '"'));
                }
                if (header != null) {
                    writer.write(String.join(separator, header));
                    header = null;
                    writer.write(lineSeparator);
                }
                writer.write(String.join(separator, row));
                row.clear();
                writer.write(lineSeparator);
            }
            /*
            boolean headerDisplayed = false;
            for (Map<String, Object> map : results) {
                List<String> header = new ArrayList<>();
                List<String> row = new ArrayList<>();
                map.entrySet().stream().forEach((entry) -> {
                    header.add("\"" + entry.getKey() + "\"");
                    final String cell = whitespaces.matcher(entry.getValue().toString()).replaceAll(" ").trim();
                    //row.add("\"" + entry.getValue().toString().replace("\"", "'") + "\"");
                    //TODO
                    row.add(cell.isEmpty() ? "\"\"" : ("\"" + cell.replace("\"", "'") + '"'));
                });
                if (!headerDisplayed) {
                    writer.write(String.join(separator, header) + lineSeparator);
                    headerDisplayed = true;
                }
                writer.write(String.join(separator, row) + lineSeparator);
            }
            */
            System.out.println("Writing to " + fullFileName + " complete.");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static String sanitizeFilename(String name) {
        return name.replaceAll("(?i)[^a-z0-9_]+", "_").substring(0, Math.min(name.length(), 59));
    }
}
