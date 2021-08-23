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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;


public class GoogleSheetExport {

    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    public static void load(String credentialsPath, String appName, String spreadsheetId, String dataFolder) throws IOException, GeneralSecurityException, InterruptedException {



        String spreadsheetRange;
        Boolean[] append = {false};

        // Build a new authorized API client service.
        NetHttpTransport HTTP_TRANSPORT;

        HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();


        GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream(credentialsPath))
                .createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS));


        Sheets sheetsService = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                .setApplicationName(appName)
                .build();


        java.io.File dir = new java.io.File(dataFolder);
        java.io.File[] directoryListing = dir.listFiles((_dir, name) -> name.toLowerCase().endsWith(".json"));
        if (directoryListing != null) {
            for (java.io.File child : directoryListing) {
                Thread.sleep(1000);
                if (child.isFile()) {
                    String[] nameElements = child.getName().split("\\.");
                    spreadsheetRange = nameElements[0].substring(0, Math.min(nameElements[0].length(), 99));

                    try {
                        ValueRange checkSheet = sheetsService.spreadsheets().values()
                                .get(spreadsheetId, spreadsheetRange)
                                .execute();
                        if (checkSheet != null) {
                            System.out.println("Sheet: " + spreadsheetRange + " exists");
                            append[0] = true;
                        }
                    } catch (GoogleJsonResponseException e) {
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

                        sheetsService.spreadsheets().batchUpdate(spreadsheetId, update).execute();
                    }


                    List<List<Object>> valuesToAdd = new ArrayList<>();


                    Stream<String> stream = Files.lines(Paths.get(child.getAbsolutePath()), StandardCharsets.UTF_8);

                    StringBuilder stringBuilder = new StringBuilder();
                    stream.forEach(stringBuilder::append);
                    String data = stringBuilder.toString();

                    try {
                        JsonArray deserialize = (JsonArray) Jsoner.deserialize(data);

                        boolean[] header = {false};
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
                    } catch (JsonException e) {
                        System.out.println(e.getMessage());
                    }

                    ValueRange body = new ValueRange()
                            .setValues(valuesToAdd);

                    sheetsService.spreadsheets().values().append(spreadsheetId, spreadsheetRange, body)
                            .setValueInputOption("USER_ENTERED")
                            .execute();


                }
            }
        } else {
            //handle if dir isn't really a dir
            throw new IOException("Directory does not contain files.");
        }
    }
}