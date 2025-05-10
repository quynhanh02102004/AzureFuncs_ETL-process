using System;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;

namespace Company.Function
{
    public class TimeTriggertoGold
    {
        private readonly ILogger<TimeTriggertoGold> _logger;
        private readonly IConfiguration _configuration;

        public TimeTriggertoGold(ILoggerFactory loggerFactory, IConfiguration configuration)
        {
            _logger = loggerFactory.CreateLogger<TimeTriggertoGold>();
            _configuration = configuration;
        }

        [Function("TimeTriggertoGold")]
        public gold_OutputType Run([TimerTrigger("0 */1000 * * * *")] TimerInfo myTimer)
        {
            string functionName = nameof(TimeTriggertoGold);
            string status = "Success";
            string triggeredBy = "TimerTrigger";
            int recordCount = 0;
            string errorMessage = null;
            DateTime timestamp = DateTime.UtcNow;

            _logger.LogInformation("Started TimeTriggertoGold function at: {Timestamp}", timestamp);

            string? connectionString = _configuration["SqlConnectionString"];
            if (string.IsNullOrEmpty(connectionString))
            {
                status = "Failed";
                errorMessage = "Connection string is null or empty.";
                _logger.LogError("Connection string is null or empty.");

                var logEntry = new FunctionLog
                {
                    FunctionName = functionName,
                    Status = status,
                    TriggeredBy = triggeredBy,
                    RecordCount = recordCount,
                    Timestamp = timestamp,
                    Message = errorMessage
                };

                return new gold_OutputType
                {
                    FunctionLogs = new List<FunctionLog> { logEntry }
                };
            }

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    _logger.LogInformation("Database connection opened successfully.");

                    new SqlCommand("SET XACT_ABORT ON;", connection).ExecuteNonQuery();

                    // Process Dimension Tables
                    recordCount += ProcessDimensionTable(connection, "Light_Conditions", "[silver].[accident1015]", 
                        "[gold].[Dim_LightConditions]", GetLightConditionsDescription, ref errorMessage);

                    recordCount += ProcessDimensionTable(connection, "Weather_Conditions", "[silver].[accident1015]", 
                        "[gold].[Dim_WeatherConditions]", GetWeatherConditionsDescription, ref errorMessage);

                    recordCount += ProcessDimensionTable(connection, "Urban_or_Rural_Area", "[silver].[accident1015]", 
                        "[gold].[Dim_UrbanorRuralArea]", GetUrbanOrRural, ref errorMessage);

                    recordCount += ProcessDimensionTable(connection, "Police_Force", "[silver].[accident1015]", 
                        "[gold].[Dim_Police]", null, ref errorMessage);

                    recordCount += ProcessDimensionTable(connection, "Road_Type", "[silver].[accident1015]", 
                        "[gold].[Dim_RoadType]", GetRoadTypeDescription, ref errorMessage);

                    recordCount += ProcessDimensionTable(connection, "Accident_Severity", "[silver].[accident1015]", 
                        "[gold].[Dim_AccidentSeverity]", GetAccidentSeverityDescription, ref errorMessage);

                    recordCount += ProcessDimensionTable(connection, "Road_Surface_Conditions", "[silver].[accident1015]", 
                        "[gold].[Dim_RoadSurfaceConditions]", GetRoadSurfaceConditionsDescription, ref errorMessage);
                    

                    // Process Fact_Accidents
                    _logger.LogInformation("Truncating table [gold].[Fact_Accidents]");
                    string truncateQuery = "TRUNCATE TABLE [gold].[Fact_Accidents];";
                    using (SqlCommand cmdTruncate = new SqlCommand(truncateQuery, connection))
                    {
                        cmdTruncate.CommandTimeout = 600;
                        cmdTruncate.ExecuteNonQuery();
                    }

                    _logger.LogInformation("Inserting data into [gold].[Fact_Accidents]");
                    string insertQuery = @"
                        INSERT INTO [gold].[Fact_Accidents] (
                            [Accident_Index], [LightConditionsKey], [WeatherConditionsKey], [UrbanRuralAreaKey],
                            [PoliceForceKey], [RoadTypeKey], [AccidentSeverityKey], [RoadSurfaceConditionsKey],
                            [Longitude], [Latitude], [Local_Authority_District], [Local_Authority_Highway],
                            [Date], [Time], [Number_of_Vehicles], [Number_of_Casualties], [Speed_Limit]
                        )
                        SELECT 
                            a.[Accident_Index], l.[LightConditionsKey], w.[WeatherConditionsKey], ur.[UrbanRuralAreaKey],
                            p.[PoliceForceKey], rt.[RoadTypeKey], asv.[AccidentSeverityKey], rsc.[RoadSurfaceConditionsKey],
                            a.[Longitude], a.[Latitude], a.[Local_Authority_District], a.[Local_Authority_Highway],
                            a.[Date], a.[Time], a.[Number_of_Vehicles], a.[Number_of_Casualties], a.[Speed_Limit]
                        FROM [silver].[accident1015] a
                        INNER JOIN [gold].[Dim_LightConditions] l ON a.[Light_Conditions] = l.[Light_Conditions]
                        INNER JOIN [gold].[Dim_WeatherConditions] w ON a.[Weather_Conditions] = w.[Weather_Conditions]
                        INNER JOIN [gold].[Dim_UrbanorRuralArea] ur ON a.[Urban_or_Rural_Area] = ur.[Urban_or_Rural_Area]
                        INNER JOIN [gold].[Dim_Police] p ON a.[Police_Force] = p.[Police_Force]
                        INNER JOIN [gold].[Dim_RoadType] rt ON a.[Road_Type] = rt.[Road_Type]
                        INNER JOIN [gold].[Dim_AccidentSeverity] asv ON a.[Accident_Severity] = asv.[Accident_Severity]
                        INNER JOIN [gold].[Dim_RoadSurfaceConditions] rsc ON a.[Road_Surface_Conditions] = rsc.[Road_Surface_Conditions]
                        WHERE l.[Status] = 1 AND w.[Status] = 1 AND ur.[Status] = 1 AND p.[Status] = 1 
                        AND rt.[Status] = 1 AND asv.[Status] = 1 AND rsc.[Status] = 1;";

                    using (SqlCommand cmd = new SqlCommand(insertQuery, connection))
                    {
                        cmd.CommandTimeout = 600;
                        int rowsAffected = cmd.ExecuteNonQuery();
                        _logger.LogInformation("Inserted {RowsAffected} rows into [gold].[Fact_Accidents]", rowsAffected);
                        recordCount += rowsAffected;
                    }

                    var logEntry = new FunctionLog
                    {
                        FunctionName = functionName,
                        Status = status,
                        TriggeredBy = triggeredBy,
                        RecordCount = recordCount,
                        Timestamp = timestamp,
                        Message = $"Processed {recordCount} records successfully"
                    };

                    string jsonLog = JsonSerializer.Serialize(logEntry);
                    _logger.LogInformation("Log entry JSON: {JsonLog}", jsonLog);

                    return new gold_OutputType
                    {
                        FunctionLogs = new List<FunctionLog> { logEntry }
                    };
                }
            }
            catch (Exception ex)
            {
                status = "Failed";
                errorMessage = ex.Message;
                _logger.LogError("Error processing TimeTriggertoGold: {ErrorMessage}", ex.Message);

                var logEntry = new FunctionLog
                {
                    FunctionName = functionName,
                    Status = status,
                    TriggeredBy = triggeredBy,
                    RecordCount = recordCount,
                    Timestamp = timestamp,
                    Message = $"Error processing: {errorMessage}"
                };

                string jsonLog = JsonSerializer.Serialize(logEntry);
                _logger.LogInformation("Log entry JSON: {JsonLog}", jsonLog);

                return new gold_OutputType
                {
                    FunctionLogs = new List<FunctionLog> { logEntry }
                };
            }
        }

        private int ProcessDimensionTable(SqlConnection connection, string columnName, string sourceTable, 
            string targetTable, Func<int, string> getDescription, ref string errorMessage)
        {
            int recordCount = 0;
            try
            {
                _logger.LogInformation("Processing {ColumnName} from {SourceTable} to {TargetTable}", columnName, sourceTable, targetTable);

                // Lấy dữ liệu từ source table và lưu vào danh sách tạm thời
                var values = new List<(int Value, string Description)>();
                string selectQuery = $"SELECT DISTINCT [{columnName}] FROM {sourceTable}";
                using (SqlCommand selectCmd = new SqlCommand(selectQuery, connection))
                using (SqlDataReader reader = selectCmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        int value = reader.GetInt32(0);
                        string description = getDescription != null ? getDescription(value) : null;
                        values.Add((value, description));
                    }
                } 

                DataTable dataTable = new DataTable();
                dataTable.Columns.Add(columnName, typeof(int));
                if (getDescription != null)
                    dataTable.Columns.Add("Description", typeof(string));
                dataTable.Columns.Add("Start_Date", typeof(DateTime));
                dataTable.Columns.Add("End_Date", typeof(DateTime));
                dataTable.Columns.Add("Status", typeof(int));

                DateTime currentDate = DateTime.Today;

                foreach (var (value, description) in values)
                {
                    string checkExistingQuery = $@"
                        SELECT COUNT(1) FROM {targetTable}
                        WHERE [{columnName}] = @Value AND {(getDescription != null ? "[Description] = @Description AND " : "")}[Status] = 1";

                    using (SqlCommand checkCmd = new SqlCommand(checkExistingQuery, connection))
                    {
                        checkCmd.Parameters.AddWithValue("@Value", value);
                        if (getDescription != null)
                            checkCmd.Parameters.AddWithValue("@Description", description ?? (object)DBNull.Value);

                        int existingCount = (int)checkCmd.ExecuteScalar();
                        if (existingCount > 0) continue;

                        string updateQuery = $@"
                            UPDATE {targetTable} SET [End_Date] = @EndDate, [Status] = 0
                            WHERE [{columnName}] = @Value AND [Status] = 1";

                        using (SqlCommand updateCmd = new SqlCommand(updateQuery, connection))
                        {
                            updateCmd.Parameters.AddWithValue("@EndDate", currentDate);
                            updateCmd.Parameters.AddWithValue("@Value", value);
                            updateCmd.ExecuteNonQuery();
                        }

                        DataRow row = dataTable.NewRow();
                        row[columnName] = value;
                        if (getDescription != null) row["Description"] = description ?? (object)DBNull.Value;
                        row["Start_Date"] = currentDate;
                        row["End_Date"] = DBNull.Value;
                        row["Status"] = 1;
                        dataTable.Rows.Add(row);
                        recordCount++;
                    }
                }

                if (dataTable.Rows.Count > 0)
                {
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                    {
                        bulkCopy.DestinationTableName = targetTable;
                        bulkCopy.BatchSize = 10000;
                        bulkCopy.BulkCopyTimeout = 300;
                        bulkCopy.ColumnMappings.Add(columnName, columnName);
                        if (getDescription != null) bulkCopy.ColumnMappings.Add("Description", "Description");
                        bulkCopy.ColumnMappings.Add("Start_Date", "Start_Date");
                        bulkCopy.ColumnMappings.Add("End_Date", "End_Date");
                        bulkCopy.ColumnMappings.Add("Status", "Status");
                        bulkCopy.WriteToServer(dataTable);
                    }
                }

                _logger.LogInformation("Inserted {RecordCount} records into {TargetTable}", recordCount, targetTable);
                return recordCount;
            }
            catch (Exception ex)
            {
                errorMessage = $"Error processing {columnName}: {ex.Message}";
                _logger.LogError("Error processing {ColumnName}: {ErrorMessage}", columnName, ex.Message);
                return recordCount;
            }
        }

        // Description dimensions
        private string GetLightConditionsDescription(int value) => value switch
        {
            1 => "Daylights",
            4 => "Darkness with street lighting",
            5 => "Darkness without street lighting",
            6 => "Darkness with no lighting",
            7 => "Darkness with unknown lighting status",
            _ => "Unknown"
        };

        private string GetWeatherConditionsDescription(int value) => value switch
        {
            1 => "Fine (no high winds)",
            2 => "Fine with high winds",
            3 => "Rain (no high winds)",
            4 => "Rain with high winds",
            5 => "Snow (no high winds)",
            6 => "Snow with high winds",
            7 => "Fog/mist",
            8 => "Other conditions",
            9 => "Unspecified",
            -1 => "Not recorded",
            _ => "Unknown"
        };

        private string GetRoadTypeDescription(int value) => value switch
        {
            1 => "Single carriageway",
            2 => "Dual carriageway",
            3 => "Other classified road types",
            6 => "One-way street",
            7 => "Special/other road types",
            9 => "Unspecified/other",
            _ => "Unknown"
        };

        private string GetAccidentSeverityDescription(int value) => value switch
        {
            1 => "Fatal",
            2 => "Serious",
            3 => "Slight",
            _ => "Unknown"
        };

        private string GetRoadSurfaceConditionsDescription(int value) => value switch
        {
            -1 => "Not recorded",
            1 => "Dry",
            2 => "Wet/Damp",
            3 => "Snow",
            4 => "Ice",
            5 => "Flooded",
            _ => "Unknown"
        };
        private string GetUrbanOrRural (int value) => value switch
        {
            1 => "Urban",
            2 => "Rural",
            _ => "Unknown"
        };
    }

    // public class gold_FunctionLog
    // {
    //     public int Id { get; set; }
    //     public string FunctionName { get; set; }
    //     public string Status { get; set; }
    //     public string TriggeredBy { get; set; }
    //     public int RecordCount { get; set; }

    //     [JsonConverter(typeof(CustomDateTimeConverter))]
    //     public DateTime Timestamp { get; set; }
    //     public string Message { get; set; }
    // }

    // public class CustomDateTimeConverter_gold : JsonConverter<DateTime>
    // {
    //     public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    //     {
    //         string dateString = reader.GetString();
    //         return DateTime.ParseExact(dateString, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
    //     }

    //     public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
    //     {
    //         writer.WriteStringValue(value.ToString("yyyy-MM-dd HH:mm:ss"));
    //     }
    // }

    public class gold_OutputType
    {
        [SqlOutput("function_logs", connectionStringSetting: "SqlConnectionString")] 
        public List<FunctionLog> FunctionLogs { get; set; }
    }
}