package inquirereport;

import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;

import java.io.*;
import java.sql.*;
import java.util.*;

public class AzureSqlExport {

    /**
     *
     * @param serverName The name of the server as it appears in the Azure SQL Database overview.
     * @param dbName The name of the database as it appears in the Azure SQL Database overview.
     * @param userName Database username with permissions to create tables and add data.
     * @param password Password for the Azure user.
     * @param dataFolder Path where the reports to be exported are stored in csv format
     * @return a map with three lists, for updated, skipped and failed operations respectively.
     * @throws SQLException Data file format incorrect or corrupted
     * @throws IOException No files found in report folder
     * @throws InterruptedException Internal Azure error
     */
    public static Map<String, List<String>> load(String serverName, String dbName, String userName, String password, String dataFolder) throws SQLException, IOException, InterruptedException {

        //Lists to return for final report
        List<String> updatedTables = new ArrayList<>();
        List<String> skippedTables = new ArrayList<>();
        List<String> failedTables = new ArrayList<>();

//Prepare data
        java.io.File dir = new java.io.File(dataFolder);
        java.io.File[] directoryListing = dir.listFiles((_dir, name) -> name.toLowerCase().endsWith(".csv"));
        if (directoryListing != null) {
            //Iterate over each *.json file in the directory
            for (java.io.File child : directoryListing) {

                if (child.isFile()) {
                    //Sleep to slow down requests to SQL server
                    Thread.sleep(1000);

                    String[] nameElements = child.getName().split("\\.");
                    //Get name of sheet from file name, without extensions. Then cut to max allowed table name length
                    String tableName = nameElements[0].substring(0, Math.min(nameElements[0].length(), 127));

                    List<String[]> csvData = new ArrayList<>();
                    //Parse CSV
                    try (BufferedReader br = new BufferedReader(new FileReader(child.getAbsolutePath()))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] splitLine = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                            for (int c = 0; c < splitLine.length; c++) {
                                splitLine[c] = splitLine[c].replace("\"", "");
                            }
                            csvData.add(splitLine);
                        }
                    } catch (FileNotFoundException e) {
                        failedTables.add(tableName);
                        System.out.println(e.getMessage());
                        continue;
                    }


                    //Get column names and data types
                    StringBuilder columnTypes = new StringBuilder();
                    String[] columnNames = csvData.size() > 0 ? csvData.get(0) : new String[0];


                    //Create JDBC connection and statement
                    String connectionUrl =
                            "jdbc:sqlserver://" + serverName + ":1433;" +
                                    "database=" + dbName + ";" +
                                    "user=" + userName + ";" +
                                    "password=" + password + ";" +
                                    "encrypt=true;" +
                                    "trustServerCertificate=false;" +
                                    "hostNameInCertificate=*.database.windows.net;" +
                                    "loginTimeout=30;";
                    Connection connection = DriverManager.getConnection(connectionUrl);
                    Statement statement = connection.createStatement();

                    // Assign data types
                    for (int i = 0; i < columnNames.length; i++) {
                        String type;
                        switch (columnNames[i]) {
                            case "date":
                                type = "DATE";
                                break;
                            case "count":
                                type = "INT";
                                break;
                            default:
                                type = "TEXT";
                                break;
                        }

                        if (i > 0) {
                            columnTypes.append(", ");
                        }

                        //Enquote and cut column names to max length allowed
                        columnNames[i] = statement.enquoteIdentifier(columnNames[i].substring(0, Math.min(columnNames[i].length(), 25)), true);
                        columnTypes.append(columnNames[i]).append(" ").append(type);
                    }

//If file is not empty write data
                    if (csvData.size() > 0) {
                        //Create table if it doesn't exist
                        //TODO => TRY TO CONVERT THIS QUERY TO A PREPARED STATEMENT (MIGHT NOT BE POSSIBLE). IF NOT, SANITIZE TABLE NAME TO AVOID SQL INJECTIONS
                        String makeTableQuery =
                                "IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "'))\n" +
                                        "BEGIN CREATE TABLE " + tableName + " (" + columnTypes + ") END";

                        int result = statement.executeUpdate(makeTableQuery);
                        //TODO => MAKE SURE TABLE BEING CREATED OR NOT IS REPORTED CORRECTLY
                        System.out.println(result > -1 ? "Table " + tableName + " already exists." : "Making table " + tableName + ".");

                        //This will create a chain of interrogation marks as placeholders for the prepared statement, e.g., (?, ?, ?)
                        String valuesPlaceholderChain = "(" + ("?,".repeat(columnNames.length)).substring(0, (columnNames.length * 2) - 1) + ")";

                        //Append data query
                        String insertDataQuery = "INSERT INTO " + tableName + "(" + String.join(",", columnNames) + ") VALUES " + valuesPlaceholderChain;
                        //Get Row data
                        SQLServerPreparedStatement preparedStatement = (SQLServerPreparedStatement) connection.prepareStatement(insertDataQuery);
                        //Insert data to prepared statement
                        for (int j = 1; j < csvData.size(); j++) {
                            String[] rowData = csvData.get(j);
                            for (int k = 0; k < rowData.length; k++) {
                                String cellData = rowData[k];
                                if (Objects.equals(columnNames[k], "\"date\"")) {
                                    preparedStatement.setDate(k + 1, java.sql.Date.valueOf(cellData));
                                } else if (Objects.equals(columnNames[k], "\"count\"")) {
                                    preparedStatement.setInt(k + 1, Integer.parseInt(cellData));
                                } else {
                                    preparedStatement.setString(k + 1, cellData);
                                }
                            }
                            preparedStatement.addBatch();
                        }


                        //Execute statement
                        int[] rowResults = preparedStatement.executeBatch();
                        System.out.println("Done updating table. " + tableName + ". Modified rows: " + rowResults.length);
                        updatedTables.add(tableName);
                        preparedStatement.close();
                        connection.close();

                    }

                    else {
                        //If file empty, skip.
                        skippedTables.add(tableName);
                        System.out.println("Table: " + tableName + "was not created or updates because the source file has no data. Please check the report data and the queries on Inquire.");
                    }
                }

            }
        }


        return Map.of(
                "updated", updatedTables,
                "skipped", skippedTables,
                "failed", failedTables
        );
    }

}