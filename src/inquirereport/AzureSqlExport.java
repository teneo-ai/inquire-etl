package inquirereport;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class AzureSqlExport {
    // Connect to your database.
    // Replace server name, username, and password with your credentials
    public static void load(String serverName, String dbName, String userName, String password, String dataFolder) {
        String connectionUrl =
                "jdbc:sqlserver://" + serverName + ":1433;" +
                        "database=" + dbName + ";" +
                        "user=" + userName + ";" +
                        "password=" + password + ";" +
                        "encrypt=true;" +
                        "trustServerCertificate=false;" +
                        "hostNameInCertificate=*.database.windows.net;" +
                        "loginTimeout=30;";


        try {
            Connection connection = DriverManager.getConnection(connectionUrl);

            java.io.File dir = new java.io.File(dataFolder);
            java.io.File[] directoryListing = dir.listFiles((_dir, name) -> name.toLowerCase().endsWith(".csv"));
            if (directoryListing != null) {
                for (java.io.File child : directoryListing) {
                    Thread.sleep(1000);
                    if (child.isFile()) {
                        String[] nameElements = child.getName().split("\\.");
                        String tableName = nameElements[0].substring(0, Math.min(nameElements[0].length(), 127));
                        List<String[]> csvData = readData(child.getAbsolutePath());

                        System.out.println(tableName);
                        System.out.println(Arrays.toString(csvData.get(0)));


                        Statement statement = connection.createStatement();

                        //Get column names and data types
                        StringBuilder columnTypes = new StringBuilder();
                        String[] columnNames = csvData.get(0);
                        for (int i = 0; i < columnNames.length; i++) {
                            String type = "";
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
                            columnNames[i] = statement.enquoteIdentifier((columnNames[i].substring(0, Math.min(columnNames[i].length(), 25))), true);
                            columnTypes.append(columnNames[i]).append(" ").append(type);
                        }
                        System.out.println(columnTypes);

                        //Get Row data
                        StringBuilder valuesBuilder = new StringBuilder();
                        for (int j = 1; j < csvData.size(); j++) {
                            String[] rowData = csvData.get(j);
                            if(j != 1){
                                valuesBuilder.append(", ");
                            }
                            valuesBuilder.append("(");
                            for (int h = 0; h < rowData.length; h++) {
                                String cellData = rowData[h];
                                if (Objects.equals(columnNames[h], "\"date\"")) {
                                    cellData = "CONVERT(DATE, '" + cellData + "',23)";
                                }
                                else if(!Objects.equals(columnNames[h], "\"count\"")){
                                    cellData = statement.enquoteLiteral(cellData);
                                }
                                if(h != 0){
                                    valuesBuilder.append(", ");
                                }
                                valuesBuilder.append(cellData);
                            }

                            valuesBuilder.append(")");
                        }

                        System.out.println(valuesBuilder.toString());

                        //Create table if it doesn't exist
                        String sqlQuery =
                                "IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "'))\n" +
                                        "BEGIN CREATE TABLE " + tableName + " (" + columnTypes + ") END\n" +
                                        "INSERT INTO " + tableName + "(" + String.join(",", columnNames) + ") VALUES " + valuesBuilder;
                        int result = statement.executeUpdate(sqlQuery);
                        System.out.println("Done. " + result);


                    }

                }
            }


        }
        // Handle any errors that may have occurred.
        catch (SQLException | InterruptedException | IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private static List<String[]> readData(String file) throws IOException {
        List<String[]> content = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = "";
            while ((line = br.readLine()) != null) {
                line = line.replace("\"", "");
                content.add(line.split(","));
            }
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
        return content;
    }
}