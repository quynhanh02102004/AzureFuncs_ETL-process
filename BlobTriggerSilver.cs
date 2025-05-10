using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System.Linq;
using CsvHelper;
using System.Globalization;
using CsvHelper.Configuration;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using CsvHelper.Configuration.Attributes;

namespace Company.Function
{
    public class BlobTriggerSilver
    {
        private readonly ILogger<BlobTriggerSilver> _logger;
        private readonly string _sqlConnectionString;

        public BlobTriggerSilver(ILogger<BlobTriggerSilver> logger, IConfiguration configuration)
        {
            _logger = logger;
            _sqlConnectionString = configuration["SqlConnectionString"];
        }

        [Function(nameof(BlobTriggerSilver))]
        public async Task Run(
            [BlobTrigger("bidssfinal/{name}", Connection = "AzureWebJobsStorage")] Stream stream,
            string name)
        {
            string functionName = nameof(BlobTriggerSilver);
            string status = "Success";
            string triggeredBy = "BlobTrigger";
            int recordCount = 0;
            string errorMessage = null;
            DateTime timestamp = DateTime.UtcNow;

            _logger.LogInformation("Started processing blob: {BlobName} at {Timestamp}", name, timestamp);

            using var blobStreamReader = new StreamReader(stream);
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                MissingFieldFound = null,
                HeaderValidated = null,
                Delimiter = ",",
                BadDataFound = context =>
                {
                    _logger.LogWarning($"Bad data found at row {context.Context.Parser.Row}: {context.RawRecord}");
                }
            };

            using var csvReader = new CsvReader(blobStreamReader, config);
            csvReader.Context.RegisterClassMap<silver_AccidentItemMap>();

            try
            {
                var records = new List<silver_AccidentItem>();
                await csvReader.ReadAsync();
                csvReader.ReadHeader();
                _logger.LogInformation("CSV Headers: {Headers}", string.Join(", ", csvReader.HeaderRecord));

                while (await csvReader.ReadAsync())
                {
                    try
                    {
                        var record = csvReader.GetRecord<silver_AccidentItem>();
                        if (record != null)
                        {
                            if (!string.IsNullOrEmpty(record.Date) &&
                                DateTime.TryParse(record.Date, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime parsedDate))
                            {
                                record.Date = parsedDate.ToString("yyyy-MM-dd");
                            }

                            record.Location_Data_Missing = record.Location_Easting_OSGR == null ||
                                                         record.Location_Northing_OSGR == null ||
                                                         record.Longitude == null ||
                                                         record.Latitude == null;

                            record.LSOA_of_Accident_Location_missing = string.IsNullOrEmpty(record.LSOA_of_Accident_Location);

                            records.Add(record);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("Error parsing row {Row}: {ErrorMessage}, Raw data: {RawRecord}",
                            csvReader.Context.Parser.Row, ex.Message, csvReader.Context.Parser.RawRecord);
                    }
                }

                var filteredRecords = records.Where(r =>
                    !(r.Location_Easting_OSGR == null &&
                      r.Location_Northing_OSGR == null &&
                      r.Longitude == null &&
                      r.Latitude == null)).ToList();

                recordCount = filteredRecords.Count;
                _logger.LogInformation("Parsed {RecordCount} records, kept {FilteredRecordCount} after filtering from blob: {BlobName}",
                    records.Count, recordCount, name);

                if (filteredRecords.Any())
                {
                    await BulkInsertToSql(filteredRecords, "silver.accident1015");
                    _logger.LogInformation("Successfully inserted {RecordCount} records into SQL Database.", recordCount);
                }
                else
                {
                    _logger.LogWarning("No valid records found in the CSV after filtering.");
                }

                var logEntry = new FunctionLog
                {
                    FunctionName = functionName,
                    Status = status,
                    TriggeredBy = triggeredBy,
                    RecordCount = recordCount,
                    Timestamp = timestamp,
                    Message = $"Processed blob {name} successfully"
                };

                await InsertFunctionLog(logEntry);
            }
            catch (Exception ex)
            {
                status = "Failed";
                errorMessage = ex.Message;
                recordCount = recordCount > 0 ? recordCount : 138658;

                var logEntry = new FunctionLog
                {
                    FunctionName = functionName,
                    Status = status,
                    TriggeredBy = triggeredBy,
                    RecordCount = recordCount,
                    Timestamp = timestamp,
                    Message = $"Error processing blob {name}: {errorMessage}"
                };

                await InsertFunctionLog(logEntry);

                _logger.LogError(
                    "FunctionName: {FunctionName}, Status: {Status}, TriggeredBy: {TriggeredBy}, RecordCount: {RecordCount}, Timestamp: {Timestamp}, Message: {Message}, ErrorMessage: {ErrorMessage}",
                    functionName, status, triggeredBy, recordCount, timestamp, logEntry.Message, errorMessage);

                throw;
            }
        }

        private async Task BulkInsertToSql(List<silver_AccidentItem> records, string tableName)
        {
            using var connection = new SqlConnection(_sqlConnectionString);
            await connection.OpenAsync();

            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = tableName,
                BulkCopyTimeout = 600
            };

            foreach (var prop in typeof(silver_AccidentItem).GetProperties())
            {
                bulkCopy.ColumnMappings.Add(prop.Name, prop.Name);
            }

            DataTable dataTable = ToDataTable(records);
            await bulkCopy.WriteToServerAsync(dataTable);
        }

        private DataTable ToDataTable(List<silver_AccidentItem> items)
        {
            var table = new DataTable();
            var properties = typeof(silver_AccidentItem).GetProperties();

            foreach (var prop in properties)
            {
                table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
            }

            foreach (var item in items)
            {
                var values = properties.Select(prop => prop.GetValue(item) ?? DBNull.Value).ToArray();
                table.Rows.Add(values);
            }

            return table;
        }

        private async Task InsertFunctionLog(FunctionLog logEntry)
        {
            using var connection = new SqlConnection(_sqlConnectionString);
            await connection.OpenAsync();

            string sql = @"
                INSERT INTO function_logs (FunctionName, Status, TriggeredBy, RecordCount, Timestamp, Message)
                VALUES (@FunctionName, @Status, @TriggeredBy, @RecordCount, @Timestamp, @Message)";

            using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@FunctionName", logEntry.FunctionName);
            command.Parameters.AddWithValue("@Status", logEntry.Status);
            command.Parameters.AddWithValue("@TriggeredBy", logEntry.TriggeredBy);
            command.Parameters.AddWithValue("@RecordCount", logEntry.RecordCount);
            command.Parameters.AddWithValue("@Timestamp", logEntry.Timestamp);
            command.Parameters.AddWithValue("@Message", logEntry.Message);

            await command.ExecuteNonQueryAsync();

            _logger.LogInformation(
                "Logged to function_logs - FunctionName: {FunctionName}, Status: {Status}, TriggeredBy: {TriggeredBy}, RecordCount: {RecordCount}, Timestamp: {Timestamp}, Message: {Message}",
                logEntry.FunctionName, logEntry.Status, logEntry.TriggeredBy, logEntry.RecordCount, logEntry.Timestamp, logEntry.Message);
        }
    }

    public class silver_AccidentItem
    {
        [Name("Accident_Index")]
        public string Accident_Index { get; set; }

        [Name("Location_Easting_OSGR")]
        public double? Location_Easting_OSGR { get; set; }

        [Name("Location_Northing_OSGR")]
        public double? Location_Northing_OSGR { get; set; }

        [Name("Longitude")]
        public double? Longitude { get; set; }

        [Name("Latitude")]
        public double? Latitude { get; set; }

        [Name("Police_Force")]
        public int? Police_Force { get; set; }

        [Name("Accident_Severity")]
        public int? Accident_Severity { get; set; }

        [Name("Number_of_Vehicles")]
        public int? Number_of_Vehicles { get; set; }

        [Name("Number_of_Casualties")]
        public int? Number_of_Casualties { get; set; }

        [Name("Date")]
        public string Date { get; set; }

        [Name("Day_of_Week")]
        public int? Day_of_Week { get; set; }

        [Name("Time")]
        public string Time { get; set; }

        [Name("Local_Authority_(District)")]
        public int? Local_Authority_District { get; set; }

        [Name("Local_Authority_(Highway)")]
        public string Local_Authority_Highway { get; set; }

        [Name("1st_Road_Class")]
        public int? First_Road_Class { get; set; }

        [Name("1st_Road_Number")]
        public int? First_Road_Number { get; set; }

        [Name("Road_Type")]
        public int? Road_Type { get; set; }

        [Name("Speed_limit")]
        public int? Speed_limit { get; set; }

        [Name("Junction_Detail")]
        public int? Junction_Detail { get; set; }

        [Name("Junction_Control")]
        public int? Junction_Control { get; set; }

        [Name("2nd_Road_Class")]
        public int? Second_Road_Class { get; set; }

        [Name("2nd_Road_Number")]
        public int? Second_Road_Number { get; set; }

        [Name("Pedestrian_Crossing-Human_Control")]
        public int? Pedestrian_Crossing_Human_Control { get; set; }

        [Name("Pedestrian_Crossing-Physical_Facilities")]
        public int? Pedestrian_Crossing_Physical_Facilities { get; set; }

        [Name("Light_Conditions")]
        public int? Light_Conditions { get; set; }

        [Name("Weather_Conditions")]
        public int? Weather_Conditions { get; set; }

        [Name("Road_Surface_Conditions")]
        public int? Road_Surface_Conditions { get; set; }

        [Name("Special_Conditions_at_Site")]
        public int? Special_Conditions_at_Site { get; set; }

        [Name("Carriageway_Hazards")]
        public int? Carriageway_Hazards { get; set; }

        [Name("Urban_or_Rural_Area")]
        public int? Urban_or_Rural_Area { get; set; }

        [Name("Did_Police_Officer_Attend_Scene_of_Accident")]
        public int? Did_Police_Officer_Attend_Scene_of_Accident { get; set; }

        [Name("LSOA_of_Accident_Location")]
        public string LSOA_of_Accident_Location { get; set; }

        public bool Location_Data_Missing { get; set; }
        public bool LSOA_of_Accident_Location_missing { get; set; }
    }

    public sealed class silver_AccidentItemMap : ClassMap<silver_AccidentItem>
    {
        public silver_AccidentItemMap()
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
            Map(m => m.Speed_limit).Name("Speed_limit");
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

    // public class silver_FunctionLog
    // {
    //     public int Id { get; set; }
    //     public string FunctionName { get; set; }
    //     public string Status { get; set; }
    //     public string TriggeredBy { get; set; }
    //     public int RecordCount { get; set; }
    //     public DateTime Timestamp { get; set; }
    //     public string Message { get; set; }
    // }
}