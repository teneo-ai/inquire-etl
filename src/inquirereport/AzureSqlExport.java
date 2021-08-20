package inquirereport;

import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class AzureSqlExport {
    // Connect to your database.
    // Replace server name, username, and password with your credentials
    public static void load(String serverName, String dbName, String userName, String password, String dataFolder) throws SQLException, IOException, InterruptedException {


//Prepare data
        java.io.File dir = new java.io.File(dataFolder);
        java.io.File[] directoryListing = dir.listFiles((_dir, name) -> name.toLowerCase().endsWith(".csv"));
        if (directoryListing != null) {
            for (java.io.File child : directoryListing) {
                Thread.sleep(1000);
                if (child.isFile()) {
                    String[] nameElements = child.getName().split("\\.");
                    String tableName = nameElements[0].substring(0, Math.min(nameElements[0].length(), 127));
                    List<String[]> csvData = readData(child.getAbsolutePath());

                    System.out.println("table name: " + tableName);

                    //Get column names and data types
                    StringBuilder columnTypes = new StringBuilder();
                    String[] columnNames = csvData.size() > 0 ? csvData.get(0) : new String[0];
                    System.out.println("column names: " + Arrays.toString(columnNames));


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


                        columnNames[i] = (columnNames[i].substring(0, Math.min(columnNames[i].length(), 25)));
                        columnTypes.append(columnNames[i]).append(" ").append(type);
                    }
                    System.out.println("column types: " + columnTypes);

                    if (csvData.size() > 0) {
                        //Create table if it doesn't exist
                        String makeTableQuery =
                                "IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "'))\n" +
                                        "BEGIN CREATE TABLE " + tableName + " (" + columnTypes + ") END";

                        SQLServerPreparedStatement preparedStatement = (SQLServerPreparedStatement) connection.prepareStatement(makeTableQuery);
                        int result = preparedStatement.executeUpdate();
                        System.out.println(result > -1 ? "Table " + tableName + " already exists." : "Making table " + tableName +".");

                        String valuesPlaceholderChain = "(" + ("?,".repeat(columnNames.length)).substring(0, (columnNames.length * 2) - 1) + ")";

                        String insertDataQuery = "INSERT INTO " + tableName + "(" + String.join(",", columnNames) + ") VALUES " + valuesPlaceholderChain;
                        //Get Row data
                        preparedStatement = (SQLServerPreparedStatement) connection.prepareStatement(insertDataQuery);

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
                        System.out.println("Done. Modified rows: " + Arrays.toString(rowResults));

                        preparedStatement.close();
                        connection.close();

                    } else {
                        System.out.println("Table: " + tableName + "was not created or updates because the source file has no data. Please check the report data and the queries on Inquire.");
                    }
                }

            }
        }
    }

    private static List<String[]> readData(String file) throws IOException {
        List<String[]> content = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] splitLine = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                for (int c = 0; c < splitLine.length; c++) {
                    splitLine[c] = splitLine[c].replace("\"", "");
                }
                content.add(splitLine);
            }
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
        return content;
    }
}