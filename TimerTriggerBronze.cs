using System;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using Microsoft.Data.SqlClient;
using SendGrid;
using SendGrid.Helpers.Mail;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;

namespace Company.Function
{
    public class TimerBlobProcessor
    {
        private readonly ILogger<TimerBlobProcessor> _logger;

        public TimerBlobProcessor(ILogger<TimerBlobProcessor> logger)
        {
            _logger = logger;
        }

        [Function("TimerBlobProcessor")]
        public async Task<OutputType> Run([TimerTrigger("0 0 1 * * 1")] TimerInfo myTimer)
        {
            string functionName = nameof(TimerBlobProcessor);
            string status = "Success";
            string triggeredBy = "TimerTrigger";
            int recordCount = 0;
            string errorMessage = null;
            DateTime timestamp = DateTime.UtcNow;
            var functionLogs = new List<FunctionLog>();

            _logger.LogInformation("Started TimerBlobProcessor at {Timestamp}", timestamp);

            string connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            if (string.IsNullOrEmpty(connectionString))
            {
                _logger.LogError("AzureWebJobsStorage is not configured.");
                status = "Failed";
                errorMessage = "AzureWebJobsStorage is not configured.";
                return CreateErrorOutput(functionName, triggeredBy, recordCount, timestamp, errorMessage, functionLogs);
            }

            string containerName = "bidssfinal";
            var blobServiceClient = new BlobServiceClient(connectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient(containerName);

            var recordsByBlob = new Dictionary<string, List<AccidentItem>>();

            try
            {
                await foreach (var blobItem in containerClient.GetBlobsAsync())
                {
                    try
                    {
                        var blobClient = containerClient.GetBlobClient(blobItem.Name);
                        var response = await blobClient.GetPropertiesAsync();
                        var properties = response.Value;

                        if (properties.Metadata.ContainsKey("Processed"))
                        {
                            _logger.LogInformation("Skipping already processed blob: {BlobName}", blobItem.Name);
                            continue;
                        }

                        using var stream = await blobClient.OpenReadAsync();
                        using var reader = new StreamReader(stream);
                        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                        {
                            MissingFieldFound = null,
                            HeaderValidated = null,
                            Delimiter = ",",
                            BadDataFound = context =>
                            {
                                string msg = $"Bad data at row {context.Context.Parser.Row} in {blobItem.Name}: {context.RawRecord}";
                                _logger.LogWarning(msg);
                                Task.Run(() => SendEmailAsync("Bad Data in Blob", msg));
                            }
                        };

                        using var csvReader = new CsvReader(reader, config);
                        csvReader.Context.RegisterClassMap<AccidentItemMap>();
                        await csvReader.ReadAsync();
                        csvReader.ReadHeader();
                        _logger.LogInformation("CSV Headers in {BlobName}: {Headers}", blobItem.Name, string.Join(", ", csvReader.HeaderRecord));

                        var records = new List<AccidentItem>();
                        while (await csvReader.ReadAsync())
                        {
                            try
                            {
                                var record = csvReader.GetRecord<AccidentItem>();
                                if (record != null) records.Add(record);
                            }
                            catch (Exception ex)
                            {
                                string msg = $"Error parsing row {csvReader.Context.Parser.Row} in {blobItem.Name}: {ex.Message}";
                                _logger.LogError(msg);
                                await SendEmailAsync("CSV Parsing Error", msg);
                            }
                        }

                        recordCount += records.Count;
                        _logger.LogInformation("Parsed {RecordCount} records from {BlobName}", records.Count, blobItem.Name);
                        recordsByBlob[blobItem.Name] = records;

                        await blobClient.SetMetadataAsync(new Dictionary<string, string> { { "Processed", DateTime.UtcNow.ToString() } });
                        _logger.LogInformation("Marked {BlobName} as processed", blobItem.Name);
                    }
                    catch (Exception ex)
                    {
                        string msg = $"Error processing blob {blobItem.Name}: {ex.Message}\nStackTrace: {ex.StackTrace}";
                        _logger.LogError(msg);
                        functionLogs.Add(new FunctionLog
                        {
                            FunctionName = functionName,
                            Status = "Failed",
                            TriggeredBy = triggeredBy,
                            RecordCount = 0,
                            Timestamp = DateTime.UtcNow,
                            Message = msg
                        });
                        await SendEmailAsync("Blob Processing Error", msg);
                    }
                }

                if (recordsByBlob.Any())
                {
                    _logger.LogInformation("Inserting data from {BlobCount} blobs into SQL Database", recordsByBlob.Count);
                    await InsertToDatabase(recordsByBlob);
                    await SendEmailAsync("Blob Processing Report", $"Processed and inserted data from {recordsByBlob.Count} blobs into database.");
                }
                else
                {
                    _logger.LogWarning("No new blobs found to process.");
                }

                var logEntry = new FunctionLog
                {
                    FunctionName = functionName,
                    Status = status,
                    TriggeredBy = triggeredBy,
                    RecordCount = recordCount,
                    Timestamp = timestamp,
                    Message = $"Processed {recordsByBlob.Count} blobs successfully"
                };
                functionLogs.Add(logEntry);

                string jsonLog = JsonSerializer.Serialize(logEntry);
                _logger.LogInformation("Log entry JSON: {JsonLog}", jsonLog);

                return new OutputType
                {
                    FunctionLogs = functionLogs
                };
            }
            catch (Exception ex)
            {
                status = "Failed";
                errorMessage = ex.Message + (ex.InnerException != null ? $" InnerException: {ex.InnerException.Message}" : "");
                return CreateErrorOutput(functionName, triggeredBy, recordCount, timestamp, errorMessage, functionLogs);
            }
        }

        private async Task DropAndCreateTable(string tableName, SqlConnection conn)
        {
            try
            {
                string dropTableSQL = $"IF OBJECT_ID('bronze.{tableName}', 'U') IS NOT NULL DROP TABLE bronze.{tableName};";
                string createTableSQL = @"
                    CREATE TABLE bronze." + tableName + @" (
                        Accident_Index NVARCHAR(200),
                        Location_Easting_OSGR FLOAT,
                        Location_Northing_OSGR FLOAT,
                        Longitude FLOAT,
                        Latitude FLOAT,
                        Police_Force INT,
                        Accident_Severity INT,
                        Number_of_Vehicles INT,
                        Number_of_Casualties INT,
                        Date NVARCHAR(30),
                        Day_of_Week INT,
                        Time NVARCHAR(30),
                        Local_Authority_District INT,
                        Local_Authority_Highway NVARCHAR(200),
                        First_Road_Class INT,
                        First_Road_Number INT,
                        Road_Type INT,
                        Speed_Limit INT,
                        Junction_Detail INT,
                        Junction_Control INT,
                        Second_Road_Class INT,
                        Second_Road_Number INT,
                        Pedestrian_Crossing_Human_Control INT,
                        Pedestrian_Crossing_Physical_Facilities INT,
                        Light_Conditions INT,
                        Weather_Conditions INT,
                        Road_Surface_Conditions INT,
                        Special_Conditions_at_Site INT,
                        Carriageway_Hazards INT,
                        Urban_or_Rural_Area INT,
                        Did_Police_Officer_Attend_Scene_of_Accident INT,
                        LSOA_of_Accident_Location NVARCHAR(200)
                    );";

                using (SqlCommand cmd = new SqlCommand(dropTableSQL + createTableSQL, conn))
                {
                    await cmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Dropped (if exists) and created table: bronze.{TableName}", tableName);
                }
            }
            catch (Exception ex)
            {
                string errorMsg = $"Error creating table bronze.{tableName}: {ex.Message}\nStackTrace: {ex.StackTrace}";
                _logger.LogError(errorMsg);
                await SendEmailAsync("SQL Table Creation Error", errorMsg);
                throw;
            }
        }

        private async Task InsertToDatabase(Dictionary<string, List<AccidentItem>> recordsByBlob)
        {
            string sqlConnectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
            if (string.IsNullOrEmpty(sqlConnectionString))
            {
                throw new ArgumentNullException("SqlConnectionString", "SQL Connection String is not configured.");
            }

            using (SqlConnection conn = new SqlConnection(sqlConnectionString))
            {
                await conn.OpenAsync();
                _logger.LogInformation("SQL Connection opened successfully.");

                foreach (var blobEntry in recordsByBlob)
                {
                    string blobName = blobEntry.Key;
                    var records = blobEntry.Value;
                    string tableName = Path.GetFileNameWithoutExtension(blobName).Replace("-", "_").Replace(" ", "_").ToLower();

                    try
                    {
                        await DropAndCreateTable(tableName, conn);

                        _logger.LogInformation("Inserting {RecordCount} records into bronze.{TableName} using SqlBulkCopy", records.Count, tableName);
                        DataTable dataTable = ConvertToDataTable(records);

                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                        {
                            bulkCopy.DestinationTableName = $"bronze.{tableName}";
                            await bulkCopy.WriteToServerAsync(dataTable);
                        }

                        _logger.LogInformation("Successfully inserted {RecordCount} records into bronze.{TableName}", records.Count, tableName);
                    }
                    catch (Exception ex)
                    {
                        string errorMsg = $"Error inserting data into bronze.{tableName}: {ex.Message}\nStackTrace: {ex.StackTrace}";
                        _logger.LogError(errorMsg);
                        await SendEmailAsync("SqlBulkCopy Error", errorMsg);
                        throw;
                    }
                }
            }
        }

        private DataTable ConvertToDataTable(List<AccidentItem> records)
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("Accident_Index", typeof(string));
            dt.Columns.Add("Location_Easting_OSGR", typeof(double)).AllowDBNull = true;
            dt.Columns.Add("Location_Northing_OSGR", typeof(double)).AllowDBNull = true;
            dt.Columns.Add("Longitude", typeof(double)).AllowDBNull = true;
            dt.Columns.Add("Latitude", typeof(double)).AllowDBNull = true;
            dt.Columns.Add("Police_Force", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Accident_Severity", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Number_of_Vehicles", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Number_of_Casualties", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Date", typeof(string));
            dt.Columns.Add("Day_of_Week", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Time", typeof(string));
            dt.Columns.Add("Local_Authority_District", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Local_Authority_Highway", typeof(string));
            dt.Columns.Add("First_Road_Class", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("First_Road_Number", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Road_Type", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Speed_Limit", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Junction_Detail", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Junction_Control", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Second_Road_Class", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Second_Road_Number", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Pedestrian_Crossing_Human_Control", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Pedestrian_Crossing_Physical_Facilities", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Light_Conditions", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Weather_Conditions", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Road_Surface_Conditions", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Special_Conditions_at_Site", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Carriageway_Hazards", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Urban_or_Rural_Area", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("Did_Police_Officer_Attend_Scene_of_Accident", typeof(int)).AllowDBNull = true;
            dt.Columns.Add("LSOA_of_Accident_Location", typeof(string));

            foreach (var record in records)
            {
                dt.Rows.Add(
                    record.Accident_Index,
                    record.Location_Easting_OSGR,
                    record.Location_Northing_OSGR,
                    record.Longitude,
                    record.Latitude,
                    record.Police_Force,
                    record.Accident_Severity,
                    record.Number_of_Vehicles,
                    record.Number_of_Casualties,
                    record.Date,
                    record.Day_of_Week,
                    record.Time,
                    record.Local_Authority_District,
                    record.Local_Authority_Highway,
                    record.First_Road_Class,
                    record.First_Road_Number,
                    record.Road_Type,
                    record.Speed_Limit,
                    record.Junction_Detail,
                    record.Junction_Control,
                    record.Second_Road_Class,
                    record.Second_Road_Number,
                    record.Pedestrian_Crossing_Human_Control,
                    record.Pedestrian_Crossing_Physical_Facilities,
                    record.Light_Conditions,
                    record.Weather_Conditions,
                    record.Road_Surface_Conditions,
                    record.Special_Conditions_at_Site,
                    record.Carriageway_Hazards,
                    record.Urban_or_Rural_Area,
                    record.Did_Police_Officer_Attend_Scene_of_Accident,
                    record.LSOA_of_Accident_Location
                );
            }

            return dt;
        }

        private async Task SendEmailAsync(string subject, string body)
        {
            string apiKey = Environment.GetEnvironmentVariable("SendGridApiKey");
            if (string.IsNullOrEmpty(apiKey))
            {
                _logger.LogError("SendGrid API Key is not configured.");
                return;
            }

            var client = new SendGridClient(apiKey);
            var from = new EmailAddress("phuongvynguyenngoc97@gmail.com", "Azure Function");
            var to = new EmailAddress("vynnp22416c@st.uel.edu.vn", "Admin");
            var msg = MailHelper.CreateSingleEmail(from, to, subject, body, body);

            try
            {
                var response = await client.SendEmailAsync(msg);
                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogError("Failed to send email. Status Code: {StatusCode}", response.StatusCode);
                }
                else
                {
                    _logger.LogInformation("Email sent successfully.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error sending email: {Message}", ex.Message);
            }
        }

        private OutputType CreateErrorOutput(string functionName, string triggeredBy, int recordCount, DateTime timestamp, string errorMessage, List<FunctionLog> functionLogs)
        {
            var logEntry = new FunctionLog
            {
                FunctionName = functionName,
                Status = "Failed",
                TriggeredBy = triggeredBy,
                RecordCount = recordCount,
                Timestamp = timestamp,
                Message = $"Error in TimerBlobProcessor: {errorMessage}"
            };

            string jsonLog = JsonSerializer.Serialize(logEntry);
            _logger.LogInformation("Log entry JSON: {JsonLog}", jsonLog);

            functionLogs.Add(logEntry);

            _logger.LogError("FunctionName: {FunctionName}, Status: {Status}, TriggeredBy: {TriggeredBy}, RecordCount: {RecordCount}, Timestamp: {Timestamp}, Message: {Message}, ErrorMessage: {ErrorMessage}",
                functionName, "Failed", triggeredBy, recordCount, timestamp, logEntry.Message, errorMessage);

            return new OutputType
            {
                FunctionLogs = functionLogs
            };
        }
    }

    public class AccidentItem
    {
        [Name("Accident_Index")] public string Accident_Index { get; set; }
        [Name("Location_Easting_OSGR")] public double? Location_Easting_OSGR { get; set; }
        [Name("Location_Northing_OSGR")] public double? Location_Northing_OSGR { get; set; }
        [Name("Longitude")] public double? Longitude { get; set; }
        [Name("Latitude")] public double? Latitude { get; set; }
        [Name("Police_Force")] public int? Police_Force { get; set; }
        [Name("Accident_Severity")] public int? Accident_Severity { get; set; }
        [Name("Number_of_Vehicles")] public int? Number_of_Vehicles { get; set; }
        [Name("Number_of_Casualties")] public int? Number_of_Casualties { get; set; }
        [Name("Date")] public string Date { get; set; }
        [Name("Day_of_Week")] public int? Day_of_Week { get; set; }
        [Name("Time")] public string Time { get; set; }
        [Name("Local_Authority_(District)")] public int? Local_Authority_District { get; set; }
        [Name("Local_Authority_(Highway)")] public string Local_Authority_Highway { get; set; }
        [Name("1st_Road_Class")] public int? First_Road_Class { get; set; }
        [Name("1st_Road_Number")] public int? First_Road_Number { get; set; }
        [Name("Road_Type")] public int? Road_Type { get; set; }
        [Name("Speed_Limit")] public int? Speed_Limit { get; set; }
        [Name("Junction_Detail")] public int? Junction_Detail { get; set; }
        [Name("Junction_Control")] public int? Junction_Control { get; set; }
        [Name("2nd_Road_Class")] public int? Second_Road_Class { get; set; }
        [Name("2nd_Road_Number")] public int? Second_Road_Number { get; set; }
        [Name("Pedestrian_Crossing-Human_Control")] public int? Pedestrian_Crossing_Human_Control { get; set; }
        [Name("Pedestrian_Crossing-Physical_Facilities")] public int? Pedestrian_Crossing_Physical_Facilities { get; set; }
        [Name("Light_Conditions")] public int? Light_Conditions { get; set; }
        [Name("Weather_Conditions")] public int? Weather_Conditions { get; set; }
        [Name("Road_Surface_Conditions")] public int? Road_Surface_Conditions { get; set; }
        [Name("Special_Conditions_at_Site")] public int? Special_Conditions_at_Site { get; set; }
        [Name("Carriageway_Hazards")] public int? Carriageway_Hazards { get; set; }
        [Name("Urban_or_Rural_Area")] public int? Urban_or_Rural_Area { get; set; }
        [Name("Did_Police_Officer_Attend_Scene_of_Accident")] public int? Did_Police_Officer_Attend_Scene_of_Accident { get; set; }
        [Name("LSOA_of_Accident_Location")] public string LSOA_of_Accident_Location { get; set; }
    }

    public sealed class AccidentItemMap : ClassMap<AccidentItem>
    {
        public AccidentItemMap()
        {
            Map(m => m.Accident_Index).Name("Accident_Index");
            Map(m => m.Location_Easting_OSGR).Name("Location_Easting_OSGR").TypeConverterOption.NumberStyles(NumberStyles.Any);
            Map(m => m.Location_Northing_OSGR).Name("Location_Northing_OSGR").TypeConverterOption.NumberStyles(NumberStyles.Any);
            Map(m => m.Longitude).Name("Longitude").TypeConverterOption.NumberStyles(NumberStyles.Any);
            Map(m => m.Latitude).Name("Latitude").TypeConverterOption.NumberStyles(NumberStyles.Any);
            Map(m => m.Police_Force).Name("Police_Force");
            Map(m => m.Accident_Severity).Name("Accident_Severity");
            Map(m => m.Number_of_Vehicles).Name("Number_of_Vehicles");
            Map(m => m.Number_of_Casualties).Name("Number_of_Casualties");
            Map(m => m.Date).Name("Date");
            Map(m => m.Day_of_Week).Name("Day_of_Week");
            Map(m => m.Time).Name("Time");
            Map(m => m.Local_Authority_District).Name("Local_Authority_(District)");
            Map(m => m.Local_Authority_Highway).Name("Local_Authority_(Highway)");
            Map(m => m.First_Road_Class).Name("1st_Road_Class");
            Map(m => m.First_Road_Number).Name("1st_Road_Number");
            Map(m => m.Road_Type).Name("Road_Type");
            Map(m => m.Speed_Limit).Name("Speed_Limit");
            Map(m => m.Junction_Detail).Name("Junction_Detail");
            Map(m => m.Junction_Control).Name("Junction_Control");
            Map(m => m.Second_Road_Class).Name("2nd_Road_Class");
            Map(m => m.Second_Road_Number).Name("2nd_Road_Number");
            Map(m => m.Pedestrian_Crossing_Human_Control).Name("Pedestrian_Crossing-Human_Control");
            Map(m => m.Pedestrian_Crossing_Physical_Facilities).Name("Pedestrian_Crossing-Physical_Facilities");
            Map(m => m.Light_Conditions).Name("Light_Conditions");
            Map(m => m.Weather_Conditions).Name("Weather_Conditions");
            Map(m => m.Road_Surface_Conditions).Name("Road_Surface_Conditions");
            Map(m => m.Special_Conditions_at_Site).Name("Special_Conditions_at_Site");
            Map(m => m.Carriageway_Hazards).Name("Carriageway_Hazards");
            Map(m => m.Urban_or_Rural_Area).Name("Urban_or_Rural_Area");
            Map(m => m.Did_Police_Officer_Attend_Scene_of_Accident).Name("Did_Police_Officer_Attend_Scene_of_Accident");
            Map(m => m.LSOA_of_Accident_Location).Name("LSOA_of_Accident_Location");
        }
    }

    public class CustomDateTimeConverter : JsonConverter<DateTime>
    {
        public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            string dateString = reader.GetString();
            return DateTime.ParseExact(dateString, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        }

        public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value.ToString("yyyy-MM-dd HH:mm:ss"));
        }
    }

    public class FunctionLog
    {
        public int Id { get; set; }
        public string FunctionName { get; set; }
        public string Status { get; set; }
        public string TriggeredBy { get; set; }
        public int RecordCount { get; set; }

        [JsonConverter(typeof(CustomDateTimeConverter))]
        public DateTime Timestamp { get; set; }
        public string Message { get; set; }
    }

    public class OutputType
    {
        [SqlOutput("function_logs", connectionStringSetting: "SqlConnectionString")]
        public List<FunctionLog> FunctionLogs { get; set; }
    }
}