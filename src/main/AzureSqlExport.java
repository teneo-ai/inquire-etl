package main;

import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.owasp.esapi.ESAPI;
import org.owasp.esapi.Encoder;
import org.owasp.esapi.codecs.OracleCodec;


public class AzureSqlExport {


    Pattern regexInt = Pattern.compile("^\\d+$");
    Pattern regexFloat = Pattern.compile("^([+-]?\\d*\\.?\\d*)$");
    Pattern regexBool = Pattern.compile("(?i)^true|false");
    Pattern regexDate = Pattern.compile(
            "^((2000|2400|2800|(19|2[0-9](0[48]|[2468][048]|[13579][26])))-02-29)$"
                    + "|^(((19|2[0-9])[0-9]{2})-02-(0[1-9]|1[0-9]|2[0-8]))$"
                    + "|^(((19|2[0-9])[0-9]{2})-(0[13578]|10|12)-(0[1-9]|[12][0-9]|3[01]))$"
                    + "|^(((19|2[0-9])[0-9]{2})-(0[469]|11)-(0[1-9]|[12][0-9]|30))$");
    Pattern regexAdorners = Pattern.compile(".*(.{2}[:.])(\\D{1,3}):");

    /**
     * @param serverName The name of the server as it appears in the Azure SQL Database overview.
     * @param dbName     The name of the database as it appears in the Azure SQL Database overview.
     * @param userName   Database username with permissions to create tables and add data.
     * @param password   Password for the Azure user.
     * @param dataFolder Path where the reports to be exported are stored in csv format
     * @return a map with three lists, for updated, skipped and failed operations respectively.
     * @throws SQLException         Data file format incorrect or corrupted
     * @throws IOException          No files found in report folder
     * @throws InterruptedException Internal Azure error
     */

    public Map<String, List<String>> load(String serverName, String dbName, String userName, String password, String dataFolder) throws SQLException, IOException, InterruptedException {

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
                    Encoder encoder = ESAPI.encoder();
                    OracleCodec oracleCodec = new OracleCodec();
                    String tableName =
                            encoder.encodeForSQL(
                                    oracleCodec,
                                    nameElements[0].substring(0, Math.min(nameElements[0].length(), 127))
                            );


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
                    Map<String, String> columnMap = new LinkedHashMap<>();

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
                        String dataType;
                        String randomSample = csvData.get(ThreadLocalRandom.current().nextInt(1, csvData.size()))[i];
                        Matcher annotationsMatcher = this.regexAdorners.matcher(randomSample);
                        String annotationType = "";
                        if (annotationsMatcher.matches()) {
                            annotationType = annotationsMatcher.group(1).equals(":a:") ? "s" : annotationsMatcher.group(2);
                        }
                        if (annotationType.equals("d") || columnNames[i].equals("date") || this.regexDate.matcher(randomSample).matches()) {
                            dataType = "DATE";
                        } else if (annotationType.equals("n") || columnNames[i].equals("count") || this.regexInt.matcher(randomSample).matches()) {
                            dataType = "INT";
                        } else if (annotationType.equals("f") || this.regexFloat.matcher(randomSample).matches()) {
                            dataType = "FLOAT";
                        } else if (annotationType.equals("b") || this.regexBool.matcher(randomSample).matches()) {
                            dataType = "BIT";
                        } else {
                            dataType = "TEXT";
                        }

                        //Enquote and cut column names to max length allowed
                        columnNames[i] = encoder.encodeForSQL(
                                oracleCodec,
                                statement.enquoteIdentifier(columnNames[i].substring(0, Math.min(columnNames[i].length(), 25)), true)
                        );
                        columnMap.put(columnNames[i], dataType);
                    }

//If file is not empty write data
                    if (csvData.size() > 0) {
                        //Create table if it doesn't exist
                        String columnTypes = columnMap.entrySet()
                                .stream()
                                .map(e -> e.getKey() + " " + e.getValue())
                                .collect(Collectors.joining(", ")).trim();
                        String makeTableQuery =
                                "IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "'))\n" +
                                        "BEGIN CREATE TABLE " + tableName + " (" + columnTypes + ") END";

                        int result = statement.executeUpdate(makeTableQuery);
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
                                String dataType = columnMap.get(columnNames[k]);
                                switch (dataType) {
                                    case "DATE":
                                        preparedStatement.setDate(k + 1, java.sql.Date.valueOf(cellData));
                                        break;
                                    case "INT":
                                        preparedStatement.setInt(k + 1, Integer.parseInt(cellData));
                                        break;
                                    case "FLOAT":
                                        preparedStatement.setFloat(k + 1, Float.parseFloat(cellData));
                                        break;
                                    case "BIT":
                                        preparedStatement.setBoolean(k + 1, Boolean.parseBoolean(cellData));
                                        break;
                                    case "TEXT":
                                        preparedStatement.setString(k + 1, cellData);
                                        break;
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

                    } else {
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