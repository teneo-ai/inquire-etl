package inquirereport;

import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;

import java.io.*;
import java.sql.*;
import java.util.*;

public class AzureSqlExport {
    // Connect to your database.
    // Replace server name, username, and password with your credentials
    public static Map<String, List<String>> load(String serverName, String dbName, String userName, String password, String dataFolder) throws SQLException, IOException, InterruptedException {

        List<String> updatedTables = new ArrayList<>();
        List<String> skippedTables = new ArrayList<>();
        List<String> failedTables = new ArrayList<>();

//Prepare data
        java.io.File dir = new java.io.File(dataFolder);
        java.io.File[] directoryListing = dir.listFiles((_dir, name) -> name.toLowerCase().endsWith(".csv"));
        if (directoryListing != null) {
            for (java.io.File child : directoryListing) {
                Thread.sleep(1000);
                if (child.isFile()) {
                    String[] nameElements = child.getName().split("\\.");
                    String tableName = nameElements[0].substring(0, Math.min(nameElements[0].length(), 127));

                    List<String[]> csvData = new ArrayList<>();
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


                        columnNames[i] = statement.enquoteIdentifier(columnNames[i].substring(0, Math.min(columnNames[i].length(), 25)), true);
                        columnTypes.append(columnNames[i]).append(" ").append(type);
                    }


                    if (csvData.size() > 0) {
                        //Create table if it doesn't exist
                        String makeTableQuery =
                                "IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "'))\n" +
                                        "BEGIN CREATE TABLE " + tableName + " (" + columnTypes + ") END";

                        int result = statement.executeUpdate(makeTableQuery);
                        System.out.println(result > -1 ? "Table " + tableName + " already exists." : "Making table " + tableName + ".");

                        String valuesPlaceholderChain = "(" + ("?,".repeat(columnNames.length)).substring(0, (columnNames.length * 2) - 1) + ")";

                        String insertDataQuery = "INSERT INTO " + tableName + "(" + String.join(",", columnNames) + ") VALUES " + valuesPlaceholderChain;
                        //Get Row data
                        SQLServerPreparedStatement preparedStatement = (SQLServerPreparedStatement) connection.prepareStatement(insertDataQuery);

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


                        int[] rowResults = preparedStatement.executeBatch();

                        System.out.println("Done updating table. " + tableName + ". Modified rows: " + rowResults.length);
                        updatedTables.add(tableName);
                        preparedStatement.close();
                        connection.close();

                    } else {
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