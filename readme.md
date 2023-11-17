# Purpose
This tool will run Shared Queries from and Inquire Log Data Source and create files locally with the data. These files can be in json or csv format or have arbitrary separator in a text file. The tool can also export the results of the Queries to Google Sheets or Azure SQL.
This is an automation tool, it is not meant for extended manual use. The intended functionality is for the compiled .jar file to be run periodically by a Cron job in order to update a data source.

# Build
Use Maven command to first validate:
`mvn validate`

Now build
`mvn clean compile assembly:single`
# Usage
java -jar "Inquire_Extract.jar" [--config=<config> --google_sheets --azure_sql --export_only --query=<query> --from=<from_date> --to=<to_date> --help]

# Parameters
- **config**: Optional.
  Configuration file or directory e.g. etc/ or test_config.properties.
  If a directory is selected the application will iterate over all *_config.properties files found in the directory.
  Defaults to application root.
- **google_sheets**: Optional.
  Export the report data to Google sheets. Requires Google Config in config file. Will generate .json reports in a addition to any specified be the separator.
- **azure_sql**: Optional.
  Export the report data to Azure SQL Database Requires Azure Config in config file. Will generate .csv reports in a addition to any specified be the separator.
- **export_only**: Optional.
  Export the report data to either Google Sheets or SQL (or both) depending on the options above, without generating new reports from Inquire, but using the files already there. Can be used to re-run exports without polling again.
- **query**: Optional.
  Name of published query to fetch. This is not case-sensitive.
  Defaults to the value 'all' which produces all published queries in the LDS.
- **from_date**: Optional.
  Date for the query search to start from. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z.
  Must be have a to_date if used.
- **to_date**: Optional.
  Date for the query search to end. Valid format is yyyy-MM-ddTHH:mm:ssZ e.g. 2017-08-31T23:55:01Z.
  Must have a from_date if used.
- **esPageSize**: Optional.
  The esPageSize parameter as per [the documentation](https://developers.teneo.ai/documentation/7.3.0/swagger/teneo-inquire/swagger/index.html#/tql/submitSharedQuery)
- **help**: Optional.
  Show this message.

# Configurations

Create a *_config.properties file with the following entries =>

- **inquireBackend**: Mandatory.
  The URL to the Teneo Inquire backend
- **inquireUser**: Mandatory.
  User name to access the LDS.
- **inquirePassword**: Mandatory.
  User password to access the LDS.
- **lds**: Mandatory.
  The name of the LDS in Inquire.
- **googleCredentialsPath**: Mandatory if --google_sheets is used.
  The path to the Google API credentials file.
- **googleCloudAppName**: Mandatory if --google_sheets is used.
  The name of the Google App to which the service account is linked, e.g. Inquire Exporter.
- **googleSheetId**: Mandatory if --google_sheets is used.
  The Id of the Google Sheet document. It is a long hash that can be found in the browser's url bar.
- **azureServerName**: Mandatory if --azure_sql is used.
  The name of the server as it appears in the Azure SQL Database overview.
- **azureDatabaseName**: Mandatory if --azure_sql is used.
  The name of the database as it appears in the Azure SQL Database overview.
- **azureUser**: Mandatory if --azure_sql is used.
  Database username with permissions to create tables and add data.
- **azurePassword**: Mandatory if --azure_sql is used.
  Password for the Azure user.
- **separator**: Optional.
  Separator used between fields in the output files. Defaults to 'json'.
- **timeout**: Optional.
  Timeout for queries. Defaults to 30 seconds.
