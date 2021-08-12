package inquirereport;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This is the main class of this project. It allows users to create generate
 * CSV or JSON reports out of Inquire data and load them to an SQL server
 *
 * @author Reuven Farchi, Mark Jones, Elizabeth Stovall
 */
public class Main {
    /**
     * @param args command line arguments:
     *             --config <config.properties> : Mandatory. configuration file or directory e.g. etc/ or test_config.properties. If a directory is selected the application will iterate over all *_config.properties files found in the directory.\n"
     *             --query <published query>: Optional. name of published query required. This is not case-sensitive. Defaults to the value 'all' which produces all published queries in the LDS.\n"
     *             --output <output_folder>: Optional. The output location for the reports, e.g. ./output/. System Temp folder will be used as default\n"
     *             --from <from_date>: Optional. Date for the query search to start from. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z. Must be have a to_date if used.\n"
     *             --to <to_date>: Optional. Date for the query search to end. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z. Must have a from_date if used.\n"
     */
    public static void main(String[] args) throws GeneralSecurityException, IOException {

        HashMap<String, String> argsMap = new HashMap<>();
        for (String arg : args) {
            String[] splitArg = arg.split("--|=");
            argsMap.put(splitArg[1], splitArg[2]);
        }

        // Check for the command line arguments
        if (!argsMap.containsKey("config") || argsMap.containsKey("help")) {
            System.out.println(
                    "Parameter usage: \n"
                            + "Usage: java -jar \"Inquire_Extract.jar\" --config=<config.properties> [--query=<published query> --output=<output_folder> --from=<from_date> --to=<to_date>]\n"
                            + "\t- config.properties: configuration file or directory e.g. etc/ or test_config.properties. If a directory is selected the application will iterate over all *_config.properties files found in the directory.\n"
                            + "\t- published query: optional. name of published query required. This is not case-sensitive. Defaults to the value 'all' which produces all published queries in the LDS.\n"
                            + "\t- output_folder: optional. The output location for the reports, e.g. ./output/. System Temp folder will be used as default\n"
                            + "\t- from_date: optional. Date for the query search to start from. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z. Must be have a to_date if used.\n"
                            + "\t- to_date: optional. Date for the query search to end. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z. Must have a from_date if used.\n"
            );
            System.exit(0);
        }

        StringBuilder configFilesPath = new StringBuilder(argsMap.get("config"));
        boolean isConfigPathDir = new File(configFilesPath.toString()).isDirectory();

        String queryName = argsMap.getOrDefault("query", "all");

        String outputFolderPath = argsMap.getOrDefault("output", System.getProperty("java.io.tmpdir") + "/inquire_exporter/");
        outputFolderPath += (outputFolderPath.charAt(outputFolderPath.length() - 1) == '/' ? "" : "/");

        String dateFrom = argsMap.getOrDefault("from", null);
        String dateTo = argsMap.getOrDefault("to", null);


        File[] files;
        if (isConfigPathDir) {
            configFilesPath.append(configFilesPath.charAt(configFilesPath.length() - 1) == '/' ? "" : "/");
            files = new File(configFilesPath.toString()).listFiles((dir, name) -> name.endsWith(".properties"));
        } else {
            files = new File[1];
            files[0] = new File(configFilesPath.toString());
        }

        if (files == null) {
            throw new IOException("No properties files found in supplied directory.");
        }
        for (File file : files) {
            if (file.isFile()) {
                String fileName = file.getName();
                String filePath = configFilesPath + (isConfigPathDir ? fileName : "");
                System.out.println("Currently processing " + filePath);

                String currentOutputFolderPath = outputFolderPath + (fileName.split("_config")[0] + "/");
                System.out.println("Temp file path: " + currentOutputFolderPath);

                try {

                    InputStream input = new FileInputStream(filePath); // Access to properties file containing Inquire backend, user and password


                    // Read properties file
                    Properties prop = new Properties();
                    prop.load(input);


                    // Check all required properties are found
                    String backend_URL = prop.getProperty("backend");
                    String lds_name = prop.getProperty("lds");
                    String username = prop.getProperty("user");
                    String password = prop.getProperty("password");
                    String separator = prop.getProperty("separator");
                    String timeout = prop.getProperty("timeout");


                    //Check for required
                    boolean mandatoryMissing = false;
                    String missingProperties = "";

                    if (backend_URL == null) {
                        missingProperties += "\t- backend: mandatory. The URL to the Teneo Inquire backend\n";
                    }
                    if (lds_name == null) {
                        missingProperties += "\t- lds: mandatory. The name of the LDS.\n";
                    }
                    if (username == null) {
                        missingProperties += "\t- username: mandatory. user name to access the LDS\n";
                    }
                    if (password == null) {
                        missingProperties += "\t- password: mandatory. user password to access the LDS\n";
                    }


                    //Check if any mandatory ones have been missed
                    if (!missingProperties.equals("")) {
                        mandatoryMissing = true;
                    }

                    //Check optionals
                    if (separator == null) {
                        missingProperties += "\t- separator: optional. Separator used between fields in the output files. Defaults to 'json'.\n";
                    }
                    if (timeout == null) {
                        missingProperties += "\t- timeout: optional. Timeout for queries. Defaults to 30 seconds.\n";
                    }

                    if (!missingProperties.equals("")) {
                        String errorString = (mandatoryMissing ? "ERROR:" : "WARNING:") + " Missing configuration properties. \n"
                                + "The following could not be found: \n"
                                + missingProperties;
                        System.out.println(errorString);
                        if (mandatoryMissing) {
                            throw new RuntimeException("Configuration File missing mandatory fields.");
                        }
                    }

                    //Apply defaults
                    separator = separator != null ? separator : "json";
                    timeout = timeout != null ? timeout : "30";
                    Thread.sleep(1000);
                    RunReport.start(queryName, currentOutputFolderPath, dateFrom, dateTo, backend_URL, username, password, lds_name, separator, timeout);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    System.out.println(
                            Arrays.stream(e.getStackTrace())
                                    .map(StackTraceElement::toString)
                                    .collect(Collectors.joining("\n"))
                    );

                }

            }
        }
    }
}
