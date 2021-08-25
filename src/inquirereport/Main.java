package inquirereport;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.*;

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
    public static void main(String[] args) throws IOException, IllegalArgumentException, SQLException, GeneralSecurityException, InterruptedException {

        try {
            HashMap<String, String> argsMap = new HashMap<>();
            List<String> legalParams = Arrays.asList("config", "google_sheets", "azure_sql", "export_only", "query", "from", "to", "help");
            for (String arg : args) {
                String[] splitArg = arg.split("-{2}|=");

                if (splitArg.length < 2 || !legalParams.contains(splitArg[1])) {
                    printHelp();
                    throw new IllegalArgumentException("illegal argument received: " + String.join("", splitArg));
                }
                argsMap.put(splitArg[1], (splitArg.length > 2 ? splitArg[2] : ""));
            }

            if (argsMap.containsKey("help")) {
                printHelp();
                System.exit(0);
            }


            StringBuilder configFilesPath = new StringBuilder(argsMap.getOrDefault("config", new File(".").getCanonicalPath()));
            boolean isConfigPathDir = new File(configFilesPath.toString()).isDirectory();

            String queryName = argsMap.getOrDefault("query", "all");
            //TODO => Accept YYYY-MM-dd for to and from params
            String dateFrom = argsMap.getOrDefault("from", null);
            String dateTo = argsMap.getOrDefault("to", null);
            boolean exportToSheets = argsMap.containsKey("google_sheets");
            boolean exportToSql = argsMap.containsKey("azure_sql");
            boolean exportOnly = argsMap.containsKey("export_only");

            if (exportOnly && !exportToSheets && !exportToSql) {
                throw new IllegalArgumentException("--export_only command received but neither --azure_sql nor --google_sheets were specified");
            }

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

                    int numberOfInquireQueries = 0;
                    int numberOfFilesWritten = 0;
                    HashSet<String> formatCount = new HashSet<>();
                    Map<String, List<String>> googleSheetsResults = null;
                    Map<String, List<String>> azureTablesResults = null;
                    StringBuilder finalReport = new StringBuilder("\n\n\tfileName: " + fileName + "\n\n");

                    if (!exportOnly) {
                        HashMap<String, Iterable<Map<String, Object>>> reportMap = InquireData.get(queryName, dateFrom, dateTo, backend_URL, username, password, lds_name, timeout);
                        numberOfInquireQueries = Objects.requireNonNull(reportMap).size();

                        for (Map.Entry<String, Iterable<Map<String, Object>>> report : reportMap.entrySet()) {
                            if (exportToSheets && !separator.equals("json")) {
                                formatCount.add("json");
                                numberOfFilesWritten++;
                                LocalFile.output("json", report, outputFolderPath);
                            }
                            if (exportToSql && !separator.equals(",")) {
                                formatCount.add("csv");
                                numberOfFilesWritten++;
                                LocalFile.output(",", report, outputFolderPath);
                            }
                            formatCount.add(separator.equals(",") ? "csv" : separator);
                            numberOfFilesWritten++;
                            LocalFile.output(separator, report, outputFolderPath);
                        }
                        finalReport
                                .append("Number Of Inquire Queries: \t")
                                .append(numberOfInquireQueries)
                                .append("\n")
                                .append("Number Of Files Written: \t")
                                .append(numberOfFilesWritten)
                                .append("\n")
                                .append("Format count: \t")
                                .append(formatCount.size())
                                .append("\n")
                                .append("Format list: \t")
                                .append(formatCount)
                                .append("\n\n");
                    }

                    if (exportToSheets) {
                        googleSheetsResults = GoogleSheetExport.load(googleCredentialsPath, googleCloudAppName, googleSheetId, outputFolderPath);

                        List<String> updatedList = googleSheetsResults.get("updated");
                        List<String> skippedList = googleSheetsResults.get("skipped");
                        List<String> failedList = googleSheetsResults.get("failed");

                        finalReport.append("Updated Google Sheets: \t")
                                .append(updatedList.size())
                                .append("\t")
                                .append(updatedList)
                                .append("\n")
                                .append("Skipped Google Sheets: \t")
                                .append(skippedList.size())
                                .append("\t")
                                .append(skippedList)
                                .append("\n")
                                .append("Failed Google Sheets: \t")
                                .append(failedList.size())
                                .append("\t")
                                .append(failedList)
                                .append("\n");
                    }
                    if (exportToSql) {
                        azureTablesResults = AzureSqlExport.load(azureServerName, azureDatabaseName, azureUser, azurePassword, outputFolderPath);
                        List<String> updatedList = azureTablesResults.get("updated");
                        List<String> skippedList = azureTablesResults.get("skipped");
                        List<String> failedList = azureTablesResults.get("failed");

                        finalReport.append("Updated Azure SQL Tables: \t")
                                .append(updatedList.size())
                                .append("\t")
                                .append(updatedList)
                                .append("\n")
                                .append("Skipped Azure SQL Tables: \t")
                                .append(skippedList.size())
                                .append("\t")
                                .append(skippedList)
                                .append("\n")
                                .append("Failed Azure SQL Tables: \t")
                                .append(failedList.size())
                                .append("\t")
                                .append(failedList)
                                .append("\n");
                    }

                    System.out.println(finalReport


                    );
                }
            }
        } catch (Exception e) {
            printHelp();
            System.out.println("General Exception: " + e.getMessage());
        }
    }

    private static void printHelp() {
        System.out.println(
                "Usage: java -jar \"Inquire_Extract.jar\" [--config=<config> --google_sheets --azure_sql --export_only --query=<query> --from=<from_date> --to=<to_date> --help]\n" +
                        "Parameters:\n"
                        + "- config: Optional. \n" +
                        "\tConfiguration file or directory e.g. etc/ or test_config.properties.\n" +
                        "\tIf a directory is selected the application will iterate over all *_config.properties files found in the directory.\n" +
                        "\tDefaults to application root.\n"
                        + "- google_sheets: Optional.\n" +
                        "\tExport the report data to Google sheets. Requires Google Config in config file. Will generate .json reports in a addition to any specified be the separator.\n"
                        + "- azure_sql: Optional.\n" +
                        "\tExport the report data to Azure SQL Database Requires Azure Config in config file. Will generate .csv reports in a addition to any specified be the separator.\n"
                        + "- export_only: Optional.\n" +
                        "\tExport the report data to either Google Sheets or SQL (or both) depending on the options above, without generating new reports from Inquire, but using the files already there. Can be used to re-run exports without polling again.\n"
                        + "- query: Optional.\n" +
                        "\tName of published query to fetch. This is not case-sensitive.\n" +
                        "\tDefaults to the value 'all' which produces all published queries in the LDS.\n"
                        + "- from_date: Optional.\n" +
                        "\tDate for the query search to start from. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z.\n" +
                        "\tMust be have a to_date if used.\n"
                        + "- to_date: Optional.\n" +
                        "\tDate for the query search to end. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z.\n" +
                        "\tMust have a from_date if used.\n"
                        + "- help: Optional.\n" +
                        "\tShow this message.\n\n" +
                        "Configurations:\n" +
                        "Create a *_config.properties file with the following entries: \n" +
                        "- inquireBackend: Mandatory.\n" +
                        "\tThe URL to the Teneo Inquire backend\n" +
                        "- inquireUser: Mandatory.\n" +
                        "\tUser name to access the LDS.\n" +
                        "- inquirePassword: Mandatory.\n" +
                        "\tUser password to access the LDS.\n" +
                        "- lds: Mandatory.\n" +
                        "\tThe name of the LDS in Inquire.\n" +
                        "- googleCredentialsPath: Mandatory if --google_sheets is used.\n" +
                        "\tThe path to the Google API credentials file.\n" +
                        "- googleCloudAppName: Mandatory if --google_sheets is used.\n" +
                        "\tThe name of the Google App to which the service account is linked, e.g. Inquire Exporter.\n" +
                        "- googleSheetId: Mandatory if --google_sheets is used.\n" +
                        "\tThe Id of the Google Sheet document. It is a long hash that can be found in the browser's url bar.\n" +
                        "- azureServerName: Mandatory if --azure_sql is used.\n" +
                        "\tThe name of the server as it appears in the Azure SQL Database overview.\n" +
                        "- azureDatabaseName: Mandatory if --azure_sql is used.\n" +
                        "\tThe name of the database as it appears in the Azure SQL Database overview.\n" +
                        "- azureUser: Mandatory if --azure_sql is used.\n" +
                        "\tDatabase username with permissions to create tables and add data.\n" +
                        "- azurePassword: Mandatory if --azure_sql is used.\n" +
                        "\tPassword for the Azure user.\n" +
                        "- separator: Optional.\n" +
                        "\tSeparator used between fields in the output files. Defaults to 'json'.\n" +
                        "- timeout: Optional.\n" +
                        "\tTimeout for queries. Defaults to 30 seconds."

        );
    }
}
