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
 * @author Reuven Farchi, Elizabeth Stovall, Mark Jones
 */
public class Main {
    /**
     * @param args command line arguments:
     *             --help: Optional : Displays command line and property file help.
     *             --export_only: Optional : Do not create new reports, export the data in the output folder only.
     *             --google_sheets: Optional: Loads the data into Google Sheets.
     *             --azure_sql: Optional: Loads the data into Azure SQL.
     *             --config  : Optional. configuration file or directory e.g. etc/ or test_config.properties. If a directory is selected the application will iterate over all *_config.properties files found in the directory.Defaults to application root."
     *             --query : Optional. name of published query required. This is not case-sensitive. Defaults to the value 'all' which produces all published queries in the LDS.\n"
     *             --from : Optional. Date for the query search to start from. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z. Must be have a to_date if used.\n"
     *             --to : Optional. Date for the query search to end. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z. Must have a from_date if used.\n"
     */
    public static void main(String[] args) throws IOException {

        HashMap<String, String> argsMap = new HashMap<>();
        for (String arg : args) {
            String[] splitArg = arg.split("-{1,2}|=");
            argsMap.put(splitArg[1], (splitArg.length > 2 ? splitArg[2] : ""));
        }

        if (argsMap.containsKey("help")) {
            printHelp();
            System.exit(0);
        }


        StringBuilder configFilesPath = new StringBuilder(argsMap.getOrDefault("config", new File(".").getCanonicalPath()));
        boolean isConfigPathDir = new File(configFilesPath.toString()).isDirectory();

        String queryName = argsMap.getOrDefault("query", "all");
        String dateFrom = argsMap.getOrDefault("from", null);
        String dateTo = argsMap.getOrDefault("to", null);
        boolean exportToSheets = argsMap.containsKey("google_sheets");
        boolean exportToSql = argsMap.containsKey("azure_sql");
        boolean exportOnly = argsMap.containsKey("export_only");


        File[] configFiles;
        if (isConfigPathDir) {
            configFilesPath.append(configFilesPath.charAt(configFilesPath.length() - 1) == '/' ? "" : "/");
            configFiles = new File(configFilesPath.toString()).listFiles((dir, name) -> name.endsWith(".properties"));
        } else {
            configFiles = new File[1];
            configFiles[0] = new File(configFilesPath.toString());
        }

        if (configFiles == null || configFiles.length == 0) {
            printHelp();
            throw new IOException("No properties files found in supplied directory.");
        }
        for (File configFile : configFiles) {
            if (configFile.isFile()) {
                String fileName = configFile.getName();
                String filePath = configFilesPath + (isConfigPathDir ? fileName : "");
                System.out.println("Currently processing " + filePath);

                try {

                    InputStream input = new FileInputStream(filePath); // Access to properties file containing Inquire backend, user and password


                    // Read properties file
                    Properties prop = new Properties();
                    prop.load(input);


                    // Check all required properties are found
                    String backend_URL = prop.getProperty("inquireBackend");
                    String lds_name = prop.getProperty("lds");
                    String username = prop.getProperty("inquireUser");
                    String password = prop.getProperty("inquirePassword");

                    String separator = prop.getProperty("separator");
                    String timeout = prop.getProperty("timeout");


                    String outputFolderPath = (String) prop.getOrDefault("outputDir", System.getProperty("java.io.tmpdir") + "/inquire_exporter/");
                    outputFolderPath += (outputFolderPath.charAt(outputFolderPath.length() - 1) == '/' ? "" : "/") + (fileName.split("_config")[0] + "/");


                    System.out.println("Output file path: " + outputFolderPath);

                    String googleCredentialsPath = prop.getProperty("googleCredentialsPath");
                    String googleCloudAppName = prop.getProperty("googleCloudAppName");
                    String googleSheetId = prop.getProperty("googleSheetId");

                    String azureServerName = prop.getProperty("azureServerName");
                    String azureDatabaseName = prop.getProperty("azureDatabaseName");
                    String azureUser = prop.getProperty("azureUser");
                    String azurePassword = prop.getProperty("azurePassword");


                    //Check for required
                    boolean mandatoryMissing = false;
                    String missingProperties = "";
                    //General properties
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

                    //Specific properties
                    //Google Sheets
                    if (googleCredentialsPath == null && exportToSheets) {
                        missingProperties += "\t- googleCredentialsPath: mandatory. The path to the Google API credentials file.\n";
                    }
                    if (googleCloudAppName == null && exportToSheets) {
                        missingProperties += "\t- googleCloudAppName: mandatory. The name of the Google App to which the service account is linked, e.g. Inquire Exporter.\n";
                    }
                    if (googleSheetId == null && exportToSheets) {
                        missingProperties += "\t- googleSheetId: mandatory. The Id of the Google Sheet document. It is a long hash that can be found in the browser's url bar.\n";
                    }
                    //Azure SQL
                    if (azureServerName == null && exportToSql) {
                        missingProperties += "\t- azureServerName: mandatory. The name of the server as it appears in the Azure AQL Database overview.\n";
                    }
                    if (azureDatabaseName == null && exportToSql) {
                        missingProperties += "\t- azureDatabaseName: mandatory. The name of the database as it appears in the Azure AQL Database overview.\n";
                    }
                    if (azureUser == null && exportToSql) {
                        missingProperties += "\t- azureUser: mandatory. Database username with permissions to create tables and add data.\n";
                    }
                    if (azurePassword == null && exportToSql) {
                        missingProperties += "\t- azurePassword: mandatory. Password for the Azure user.\n";
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


                    //Apply defaults for export type
                    separator = separator != null ? separator : "json";
                    timeout = timeout != null ? timeout : "30";
                    Thread.sleep(1000);
                  if(!exportOnly) {
                      HashMap<String, Iterable<Map<String, Object>>> reportMap = InquireData.get(queryName, dateFrom, dateTo, backend_URL, username, password, lds_name, timeout);
                      System.out.println(Objects.requireNonNull(reportMap).entrySet());

                      for (Map.Entry<String, Iterable<Map<String, Object>>> report : reportMap.entrySet()) {
                          if (exportToSheets && !separator.equals("json")) {
                              LocalFile.output("json", report, outputFolderPath);
                          }
                          if (exportToSql && !separator.equals(",")) {
                              LocalFile.output(",", report, outputFolderPath);
                          }
                          LocalFile.output(separator, report, outputFolderPath);
                      }
                  }

                    if (exportToSheets) {
                        GoogleSheetExport.load(googleCredentialsPath, googleCloudAppName, googleSheetId, outputFolderPath);
                    }
                    if (exportToSql) {
                        AzureSqlExport.load(azureServerName, azureDatabaseName, azureUser, azurePassword, outputFolderPath);
                    }


                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    System.out.println(
                            Arrays.stream(e.getStackTrace())
                                    .map(StackTraceElement::toString)
                                    .collect(Collectors.joining("\n"))
                    );
                    printHelp();
                }


            }
        }
    }

    private static void printHelp() {
        System.out.println(
                "Parameters:\n"
                        + "Usage: java -jar \"Inquire_Extract.jar\" [--config=<config.properties> --query=<published query> --from=<from_date> --to=<to_date>]\n"
                        + "- config.properties: Optional. \n" +
                        "\tConfiguration file or directory e.g. etc/ or test_config.properties.\n" +
                        "\tIf a directory is selected the application will iterate over all *_config.properties files found in the directory.\n" +
                        "\tDefaults to application root.\n"
                        + "- published query: Optional.\n" +
                        "\tName of published query to fetch. This is not case-sensitive.\n" +
                        "\tDefaults to the value 'all' which produces all published queries in the LDS.\n"
                        + "- from_date: Optional.\n" +
                        "\tDate for the query search to start from. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z.\n" +
                        "\tMust be have a to_date if used.\n"
                        + "- to_date: Optional.\n" +
                        "\tDate for the query search to end. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z.\n" +
                        "\tMust have a from_date if used.\n\n" +
                        "Configurations:\n" +
                        "Usage: create a \n" +
                        "- backend: mandatory. The URL to the Teneo Inquire backend\n" +
                        "\t- lds: mandatory. The name of the LDS.\n" +
                        "\t- username: mandatory. user name to access the LDS\n" +
                        "\t- password: mandatory. user password to access the LDS\n" +
                        "\t- googleCredentialsPath: mandatory if --google_sheets is used. The path to the Google API credentials file.\n" +
                        "\t- googleCloudAppName: mandatory if --google_sheets is used. The name of the Google App to which the service account is linked, e.g. Inquire Exporter.\n" +
                        "\t- googleSheetId: mandatory if --google_sheets is used. The Id of the Google Sheet document. It is a long hash that can be found in the browser's url bar.\n" +
                        "\t- azureServerName: mandatory if --azure_sql is used. The name of the server as it appears in the Azure SQL Database overview.\n" +
                        "\t- azureDatabaseName: mandatory if --azure_sql is used.. The name of the database as it appears in the Azure SQL Database overview.\n" +
                        "\t- azureUser: mandatory if --azure_sql is used.. Database username with permissions to create tables and add data.\n" +
                        "\t- azurePassword: mandatory if --azure_sql is used. Password for the Azure user.\n" +
                        "\t- separator: optional. Separator used between fields in the output files. Defaults to 'json'.\n" +
                        "\t- timeout: optional. Timeout for queries. Defaults to 30 seconds."

        );
    }
}
