package inquirereport;

import com.github.cliftonlabs.json_simple.JsonArray;
import com.github.cliftonlabs.json_simple.JsonException;
import com.github.cliftonlabs.json_simple.JsonObject;
import com.github.cliftonlabs.json_simple.Jsoner;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.*;
import org.jsoup.Jsoup;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.stream.Stream;


public class GoogleSheetExport {

    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    /**
     *
     * @param credentialsPath Path to the credentials.json file for Google Authentication.
     * @param appName Name of the Google Cloud Application to which the Service Account belongs.
     * @param spreadsheetId The Id of the Google Sheet document. It is a long hash that can be found in the browser's url bar.
     * @param dataFolder Path where the reports to be exported are stored in json format
     * @return a map with three lists, for updated, skipped and failed operations respectively.
     * @throws IOException Credentials file not found
     * @throws GeneralSecurityException Credentials file not accepted
     * @throws InterruptedException Issues connecting with Google Servers
     */
    public static Map<String, List<String>> load(String credentialsPath, String appName, String spreadsheetId, String dataFolder) throws IOException, GeneralSecurityException, InterruptedException {

        // These three maps are returned with the results of the export operation
        List<String> updatedSheets = new ArrayList<>();
        List<String> skippedSheets = new ArrayList<>();
        List<String> failedSheets = new ArrayList<>();

        // Spreadsheet range is the name of the sheet (corresponds to query) within the workbook (corresponds to config file) where the data is appended.
        // For example, workbook at https://docs.google.com/spreadsheets/d/{spreadsheetID} is defined in file example_config.properties.
        // The application will look in {dataFolder/example/} and read each *.json file there, for example, total_users_by_date.json.
        // This will then create a sheet called total_users_by_date in the workbook above.
        String spreadsheetRange;
        Boolean[] append = {false};

        // Build a new authorized API client service.
        NetHttpTransport HTTP_TRANSPORT;

        HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();

        //Create credentials object
        GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream(credentialsPath))
                .createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS));

        //Create Service Client for Google Sheets API
        Sheets sheetsService = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                .setApplicationName(appName)
                .build();

//Iterate over each *.json file in the directory
        java.io.File dir = new java.io.File(dataFolder);
        java.io.File[] directoryListing = dir.listFiles((_dir, name) -> name.toLowerCase().endsWith(".json"));
        if (directoryListing != null) {
            for (java.io.File child : directoryListing) {


                if (child.isFile()) {
                    //Sleep to avoid going over the allowed 60 calls per minute in free tier.
                    Thread.sleep(1000);
                    String[] nameElements = child.getName().split("\\.");
                    //Get name of sheet from file name, without extensions. Then cut to max allowed sheet name length
                    spreadsheetRange = nameElements[0].substring(0, Math.min(nameElements[0].length(), 99));

                    //Try catch block gets or creates a sheet
                    try {
                        //Attempt to get sheet.

                        ValueRange checkSheet = sheetsService.spreadsheets().values()
                                .get(spreadsheetId, spreadsheetRange)
                                .execute();
                        if (checkSheet != null) {
                            System.out.println("Sheet: " + spreadsheetRange + " exists");
                            append[0] = true;
                        }
                    }
                    catch (GoogleJsonResponseException e) {

                        //Create sheet
                        System.out.println("Sheet: " + spreadsheetRange + " didn't exist Spreadsheet so creating it.");
                        append[0] = false;

                        SheetProperties props = new SheetProperties();
                        props.set("title", spreadsheetRange);
                        AddSheetRequest addSheet = new AddSheetRequest();
                        addSheet.setProperties(props);
                        BatchUpdateSpreadsheetRequest update = new BatchUpdateSpreadsheetRequest();
                        Request request = new Request();
                        request.setAddSheet(addSheet);
                        List<Request> requests = new ArrayList<>();
                        requests.add(request);
                        update.setRequests(requests);

                        BatchUpdateSpreadsheetResponse batchUpdateSpreadsheetResponse = sheetsService.spreadsheets().batchUpdate(spreadsheetId, update).execute();
                    }

                    //Initialize values object
                    List<List<Object>> valuesToAdd = new ArrayList<>();

                    //Get json file data.
                    Stream<String> stream = Files.lines(Paths.get(child.getAbsolutePath()), StandardCharsets.UTF_8);

                    StringBuilder stringBuilder = new StringBuilder();
                    stream.forEach(stringBuilder::append);
                    String data = stringBuilder.toString();

                    try {
                        //Parse JSON
                        JsonArray deserialize = (JsonArray) Jsoner.deserialize(data);
                        //If file has no data, skip and add to skipped sheets
                        if (deserialize.size() == 0) {
                            skippedSheets.add(spreadsheetRange);
                            continue;
                        }
                        boolean[] header = {false};

                        //Build data object
                        deserialize.forEach(
                                (item) -> {

                                    List<Object> row = new ArrayList<>();
                                    JsonObject record = (JsonObject) item;

                                    if (!header[0]) {
                                        if (!append[0]) {
                                            valuesToAdd.add(Arrays.asList(record.keySet().toArray()));
                                        }
                                        header[0] = true;
                                    }
                                    record.forEach(
                                            (key, value) -> {

                                                String cellValue;

                                                if (value == null) {
                                                    cellValue = "";
                                                } else {
                                                    cellValue = Jsoup.parse(value.toString()).text();
                                                }
                                                row.add(cellValue);

                                            });
                                    valuesToAdd.add(row);


                                }

                        );

                        //Create request body
                        ValueRange body = new ValueRange()
                                .setValues(valuesToAdd);

                        //Append data to sheet
                        AppendValuesResponse executionResult = sheetsService.spreadsheets().values().append(spreadsheetId, spreadsheetRange, body)
                                .setValueInputOption("USER_ENTERED")
                                .execute();
                        System.out.println("Finished updating sheet: " + spreadsheetRange + ".");
                        updatedSheets.add(spreadsheetRange);
                    } catch (JsonException e) {
                        failedSheets.add(spreadsheetRange);
                        System.out.println("Error in sheet: " + spreadsheetRange + " =>" + e.getMessage());
                    }
                }
            }
        } else {
            //handle if dir isn't really a dir
            throw new IOException("Directory does not contain files.");
        }
        return Map.of(
                "updated", updatedSheets,
                "skipped", skippedSheets,
                "failed", failedSheets
        );
    }
}