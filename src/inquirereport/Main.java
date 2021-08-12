package inquirereport;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * This is the main class of this project. It allows users to create generate
 * Calls on the Inquire client to get shared query data and can both generate CSV or JSON reports, and load the data to Google Sheets or to an Azure SQL Database.
 *
 * @author Reuven Farchi, Elizabeth Stovall, Mark Jones
 */
public class Main {
    public static void main(String[] args) throws GeneralSecurityException, IOException {


        // Check for the command line arguments
        if (args.length == 0 || args.length == 4 || args.length > 5) {
            System.out.println(
                    "ERROR: Wrong number of parameters. \n"
                            + "Usage: java -jar \"InquireETL.jar\" <config.properties> [<published query> <output_folder> <from_date> <to_date>]\n"
                            + "\t- config.properties: configuration files directory. e.g. src/tqlquery/test_config.properties\n"
                            + "\t- published query: optional. name of published query required. This is not case-sensitive. Defaults to the value 'all' which produces all published queries in the LDS.\n"
                            + "\t- output_folder: optional. The output location for the reports, e.g. ./output/. System Temp folder will be used as default\n"
                            + "\t- from_date: optional. Date for the query search to start from. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z. Must be have a to_date if used.\n"
                            + "\t- to_date: optional. Date for the query search to end. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z. Must have a from_date if used.\n"
            );
            System.exit(0);
        }

        StringBuilder configFilesPath = new StringBuilder(args[0]);
        String queryName = "all";
        String outputFolderPath = "";
        String dateFrom = null;
        String dateTo = null;

        configFilesPath.append(configFilesPath.charAt(configFilesPath.length() - 1) == '/' ? "" : "/");

        if (args.length >= 2) {
            queryName = args[1];
        }
        if (args.length >= 3) {
            outputFolderPath = args[2];
            outputFolderPath += (outputFolderPath.charAt(outputFolderPath.length() - 1) == '/' ? "" : "/");
        } else {
            outputFolderPath = System.getProperty("java.io.tmpdir") + "/inquire_exporter/";
        }


        if (args.length == 5) {
            // Dates must be format yyyy-MM-dd e.g. 2017-08-31T23:55:01Z
            dateFrom = args[3];
            dateTo = args[4];
        }

        File[] files = new File(configFilesPath.toString()).listFiles((dir, name) -> name.endsWith(".properties"));

        if (files == null) {
            throw new IOException("No properties files found in supplied directory.");
        }
        for (File file : files) {
            if (file.isFile()) {
                String fileName = file.getName();
                String filePath = configFilesPath + fileName;
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
                    String credentials = prop.getProperty("credentials");
                    String appName = prop.getProperty("appName");
                    String sheetId = prop.getProperty("sheetId");


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
                    if (credentials == null) {
                        missingProperties += "\t- credentials: mandatory. The path to the Google API credentials file.\n";
                    }
                    if (appName == null) {
                        missingProperties += "\t- appName: mandatory. The name of the Google App to which the service account is linked, e.g. Inquire Exporter.";
                    }
                    if (sheetId == null) {
                        missingProperties += "\t- sheetId: mandatory. The Id of the Google Sheet document. It is a long hash that can be found in the browser's url bar.";
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
                    GoogleSheetData.load(credentials, appName, sheetId, currentOutputFolderPath);
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
