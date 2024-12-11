using dataPARC.Authorization.CertificateValidation;
using dataPARC.Store.EnterpriseCore.DataPoints;
using dataPARC.Store.EnterpriseCore.Entities;
using dataPARC.Store.EnterpriseCore.History.Enums;
using dataPARC.Store.EnterpriseCore.History.Inputs;
using dataPARC.Store.SDK;
using dataPARC.TimeSeries.Core.Enums;
using System.Text.Json;
using WebServiceTracability.Models;

namespace WebServiceTracability.Services
{
    public class StringHelpers
    {
        /// <summary>
        /// Retrieves the value of an OPC tag for a specified time range.
        /// </summary>
        /// <param name="beginDT">The start time of the lookup period</param>
        /// <param name="endDT">The end time of the lookup period</param>
        /// <param name="tagName">The name of the OPC tag to query</param>
        /// <returns>The value of the OPC tag as a string, or "null" if no value is found</returns>
        /// <exception cref="OpcLookupException">Thrown when an error occurs during OPC tag value retrieval</exception>
        public static string OpcItemValueLookup(DateTimeOffset beginDT, DateTimeOffset endDT, string tagName, out string error)
        {
            // Initialize error value
            error = string.Empty;

            // Validate input parameters
            if (!DateTimeHelpers.IsValidDateTimeRange(beginDT, endDT))
            {
                return string.Empty;
            }

            // Load application configuration
            var appSetting = new AppSettings();

            // Initialize OPC client and tag configuration
            var tag = InterfaceHelpers.CreateOpcTag(appSetting, tagName);
            var client = InterfaceHelpers.CreateOpcClient(appSetting);

            try
            {
                // First attempt: Try to read raw data
                var points = ReadRawHelpers.TryReadRawData(appSetting, tag, beginDT, endDT, out ReadRawStatus rwStatus);

                // Second attempt: If no raw data found, try reading at specific timestamps
                if (points == null || !points.Any())
                {
                    points = ReadAtTimeHelpers.TryReadAtTime(client, tag, beginDT, endDT.AddSeconds(-1), 10, out ReadAtTimeStatus ratStatus);
                }

                // Return the first point's value if available, otherwise return "null"
                return points?.FirstOrDefault()?.Value?.ToString() ?? "null";
            }
            catch (Exception ex)
            {
                error = new OpcLookupException(beginDT, endDT, tagName, ex).ToString();
                return string.Empty;
            }
        }

        /// <summary>
        /// Deeps the copy.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source">The source.</param>
        /// <returns></returns>
        public static T DeepCopy<T>(T source)
        {
            var serialized = JsonSerializer.Serialize(source);
            return JsonSerializer.Deserialize<T>(serialized);
        }

        /// <summary>
        /// Custom exception for OPC lookup errors
        /// </summary>
        public class OpcLookupException : Exception
        {
            public OpcLookupException(DateTimeOffset beginDT, DateTimeOffset endDT, string tagName, Exception innerException)
                : base($"Error while retrieving OPC Tag Value between {beginDT} and {endDT} (OPC Tag: {tagName})", innerException)
            {
            }

            public OpcLookupException(DateTimeOffset startTime, string tagName, Exception innerException)
                : base($"Error while retrieving OPC Tag Value before {startTime} (OPC Tag: {tagName})", innerException)
            {
            }
        }
    }

    public class DateTimeHelpers
    {
        /// <summary>
        /// Adjusts the provided datetime to be within the specified limit.
        /// </summary>
        /// <param name="dt">The datetime to adjust.</param>
        /// <param name="limitDt">The limit datetime.</param>
        /// <returns>The adjusted datetime.</returns>
        public static DateTimeOffset AdjustDateTimeRange(DateTimeOffset dt, DateTimeOffset limitDt)
        {
            // If the datetime is before the limit, use the limit
            // If the datetime is after the current time, use the current time
            // Otherwise, use the original datetime
            return dt < limitDt ? limitDt : dt > DateTimeOffset.UtcNow ? DateTimeOffset.UtcNow : dt;
        }

        /// <summary>
        /// Validates if the provided date range is within acceptable bounds
        /// </summary>
        public static bool IsValidDateTimeRange(DateTimeOffset beginDT, DateTimeOffset endDT) => !(beginDT == DateTimeOffset.MinValue || beginDT == DateTimeOffset.MaxValue || endDT == DateTimeOffset.MinValue || endDT == DateTimeOffset.MaxValue);

        /// <summary>
        /// Validates if the provided date range is within acceptable bounds
        /// </summary>
        public static bool IsValidDateTime(DateTimeOffset dt) => !(dt == DateTimeOffset.MinValue || dt == DateTimeOffset.MaxValue);

        /// <summary>
        /// Gets the six hour intervals.
        /// </summary>
        public static List<KeyValuePair<DateTimeOffset, DateTimeOffset>> GetSixHourIntervals(DateTimeOffset fromDt, DateTimeOffset toDt, int hour)
        {
            var intervals = new List<KeyValuePair<DateTimeOffset, DateTimeOffset>>();

            // Ensure fromDt is earlier than toDt
            if (fromDt > toDt)
            {
                var temp = fromDt;
                fromDt = toDt;
                toDt = temp;
            }

            // Save the initial fromdt
            var currentStart = fromDt;

            while (currentStart < toDt)
            {
                // Calculate the end of current 6-hour interval
                var currentEnd = currentStart.AddHours(hour);

                // If currentEnd exceeds toDt, use toDt as the end
                if (currentEnd > toDt)
                {
                    currentEnd = toDt;
                }

                intervals.Add(new KeyValuePair<DateTimeOffset, DateTimeOffset>(currentStart, currentEnd));

                // Set start of next interval
                currentStart = currentEnd;
            }

            return intervals;
        }
    }

    public class InterfaceHelpers
    {
        /// <summary>
        /// Creates and configures an OPC client
        /// </summary>
        public static ReadClient CreateOpcClient(AppSettings appSetting)
        {
            return new ReadClient(
                appSetting.HOST,
                appSetting.PORT,
                CertificateValidation.AcceptAllCertificates
            );
        }

        /// <summary>
        /// Creates an OPC tag with the specified configuration
        /// </summary>
        public static FullyQualifiedTagName CreateOpcTag(AppSettings appSetting, string tagName)
        {
            return FullyQualifiedTagName.CreateWithInterface(
                appSetting.INTERFACEGROUPNAME,
                appSetting.INTERFACENAME,
                tagName
            );
        }
    }

    public class ReadAtTimeHelpers
    {
        /// <summary>
        /// Attempts to read data at specific timestamps from the dataPARC
        /// </summary>
        public static List<IDataPoint> TryReadAtTime(
            ReadClient client,
            FullyQualifiedTagName tag,
            DateTimeOffset beginDT,
            DateTimeOffset endDT,
            int timeOutSeconds,
            out ReadAtTimeStatus status)
        {
            // Read the data points with a 5-second timeout
            using (CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeOutSeconds)))
            {
                var readAtTimeParam = new ReadAtTimeParameters
                {
                    TagIdentifier = new TagQueryIdentifier(tag),
                    Timestamps = [beginDT.UtcDateTime, endDT.UtcDateTime],
                    UseSimpleBounds = true,
                    DigitalTextReturnType = DigitalTextReturnType.Text,
                    TreatUncertainAsBad = true,
                    InterpolateType = InterpolateType.State
                };

                var result = client.ReadAtTimeAsync(readAtTimeParam, null, cts.Token).GetAwaiter().GetResult();
                status = result.Status;
                return result.Status == ReadAtTimeStatus.Successful
                    ? result.DataPoints.ToList()
                    : new List<IDataPoint>();
            }
        }

        public static List<SuperDataPoint> TryReadAtSequencedIntervals(
            FullyQualifiedTagName tag,
            FullyQualifiedTagName triggerTag,
            DateTimeOffset beginDT,
            DateTimeOffset endDT,
            int timeOutSeconds,
            int hourInterval,
            string datatolookup,
            out ReadAtTimeStatus status)
        {
            // Initialize datetime limit for searching the data in dataPARC for settings stored on appsettings.json
            AppSettings settings = new AppSettings();

            // Create the OPC client and tag configuration
            var client = InterfaceHelpers.CreateOpcClient(settings);

            List<KeyValuePair<DateTimeOffset, DateTimeOffset>> intervals = DateTimeHelpers.GetSixHourIntervals(beginDT, endDT, hourInterval);

            List<SuperDataPoint> datapoints = new List<SuperDataPoint>();

            status = ReadAtTimeStatus.NoValueFound;

            foreach (var interval in intervals)
            {
                var readAtTimeParam = new ReadAtTimeParameters()
                {
                    TagIdentifier = new TagQueryIdentifier(tag),
                    Timestamps = [interval.Key.UtcDateTime, interval.Value.UtcDateTime],
                    UseSimpleBounds = true,
                    DigitalTextReturnType = DigitalTextReturnType.Text,
                    TreatUncertainAsBad = true,
                    InterpolateType = InterpolateType.State
                };

                // Read the data points with a 10-second timeout
                CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeOutSeconds));

                // Perform a directional read raw operation 
                var readAtTimeResult = client.ReadAtTimeAsync(readAtTimeParam, null, cts.Token).GetAwaiter().GetResult();

                // Get the data points if succeded
                if (readAtTimeResult.DataPoints != null && readAtTimeResult.DataPoints.Count() > 0)
                {
                    var dps = readAtTimeResult.DataPoints.Where(dp => dp.Value?.ToString() == datatolookup).ToList();
                    status = ReadAtTimeStatus.Successful;
                    foreach (var dp in dps)
                    {
                        DateTimeOffset? triggerDt = DirectionalReadHelpers.LookupTriggerDateTimeNextTo(client, triggerTag, new DateTimeOffset(dp.Time, TimeSpan.FromHours(0)));
                        DateTimeOffset? triggerBeforeDt = DirectionalReadHelpers.LookUpDataMatrixDateTimeJustBefore(client, triggerTag, new DateTimeOffset(dp.Time, TimeSpan.FromHours(0)));

                        if (!datapoints.Any(dp => dp.DataPoint.Value.ToString() == datatolookup))
                        {
                            datapoints.Add(new SuperDataPoint()
                            {
                                DataPoint = dp,
                                TagAddress = new TagQueryIdentifier(tag),
                                FromDT = triggerBeforeDt ?? new DateTimeOffset(dp.Time, TimeSpan.FromHours(0)),
                                ToDT = triggerDt ?? DateTimeOffset.UtcNow                                       
                            });
                        }
                        
                    }
                }
            }

            return datapoints;
        }
    }

    public class ReadRawHelpers
    {
        /// <summary>
        /// Attempts to read raw data from the dataPARC
        /// </summary>
        public static List<IDataPoint> TryReadRawData(
            AppSettings settings,
            FullyQualifiedTagName tag,
            DateTimeOffset beginDT,
            DateTimeOffset endDT,
            out ReadRawStatus status)
        {
            // Create the OPC client and tag configuration
            var client = InterfaceHelpers.CreateOpcClient(settings);

            // Read the data points with a 15-second timeout
            using (CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
            {
                var readParam = new ReadRawParameters
                {
                    TagIdentifier = new TagQueryIdentifier(tag),
                    StartTime = beginDT.UtcDateTime,
                    EndTime = endDT.UtcDateTime,/*.AddSeconds(-1),*/
                    ReturnStartBounds = ReturnRawBoundMode.NoBound,
                    ReturnEndBounds = ReturnRawBoundMode.NoBound,
                    DigitalTextReturnType = DigitalTextReturnType.Text,
                    MaxClientPoints = 100000,
                };
                var result = client.ReadRawAsync(readParam, null, cts.Token).GetAwaiter().GetResult();
                status = result.Status;
                return result.Status == ReadRawStatus.NoValueFound ? new List<IDataPoint>() : result.DataPoints.ToList();
            }
        }

        /// <summary>
        /// Tries the read raw.
        /// </summary>
        public static List<IDataPoint> TryReadRawData(
            AppSettings settings,
            FullyQualifiedTagName tag,
            DateTimeOffset[] dateTimes,
            out ReadRawStatus status)
        {
            return TryReadRawData(settings, tag, dateTimes[0], dateTimes[1], out status);
        }

        public static List<IDataPoint> TryReadRawLookup(
            FullyQualifiedTagName tag,
            DateTimeOffset beginDT,
            DateTimeOffset endDT,
            int timeOutSeconds,
            string datatolookup,
            out ReadRawStatus status)
        {
            // Initialize datetime limit for searching the data in dataPARC for settings stored on appsettings.json
            AppSettings settings = new AppSettings();

            // Create the OPC client and tag configuration
            var client = InterfaceHelpers.CreateOpcClient(settings);

            List<KeyValuePair<DateTimeOffset, DateTimeOffset>> intervals = DateTimeHelpers.GetSixHourIntervals(beginDT, endDT, 6);

            List<IDataPoint> datapoints = new List<IDataPoint>();

            status = ReadRawStatus.NoValueFound;

            foreach (var interval in intervals)
            {
                var readRawParam = new ReadRawParameters()
                {
                    TagIdentifier = new TagQueryIdentifier(tag),
                    StartTime = interval.Key.UtcDateTime,
                    EndTime = interval.Value.UtcDateTime,
                    DigitalTextReturnType = DigitalTextReturnType.Text,
                };

                // Read the data points with a 10-second timeout
                CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeOutSeconds));

                // Perform a directional read raw operation 
                var readRawResult = client.ReadRawAsync(readRawParam, null, cts.Token).GetAwaiter().GetResult();

                // Get the data points if succeded
                if (readRawResult.DataPoints != null && readRawResult.DataPoints.Count() > 0)
                {
                    status = ReadRawStatus.Successful;
                    datapoints.AddRange(readRawResult.DataPoints.Where(dp => dp.Value?.ToString() == datatolookup).ToList());
                }
            }

            return datapoints;
        }

        // Override 
        public static List<SuperDataPoint> TryReadRawSequencedIntervalsPart(
            FullyQualifiedTagName tag,
            FullyQualifiedTagName triggerTag,
            DateTimeOffset beginDT,
            DateTimeOffset endDT,
            int timeOutSeconds,
            int hourInterval,
            string datatolookup,
            out ReadRawStatus status)
        {
            // Initialize datetime limit for searching the data in dataPARC for settings stored on appsettings.json
            AppSettings settings = new AppSettings();

            // Create the OPC client and tag configuration
            var client = InterfaceHelpers.CreateOpcClient(settings);

            List<KeyValuePair<DateTimeOffset, DateTimeOffset>> intervals = DateTimeHelpers.GetSixHourIntervals(beginDT, endDT, hourInterval);

            List<SuperDataPoint> datapoints = new List<SuperDataPoint>();

            status = ReadRawStatus.NoValueFound;

            foreach (var interval in intervals)
            {
                var readRawParam = new ReadRawParameters()
                {
                    TagIdentifier = new TagQueryIdentifier(tag),
                    StartTime = interval.Key.UtcDateTime,
                    EndTime = interval.Value.UtcDateTime,
                    //ReturnStartBounds = ReturnRawBoundMode.NoBound,
                    //ReturnEndBounds = ReturnRawBoundMode.NoBound,
                    DigitalTextReturnType = DigitalTextReturnType.Text,
                };

                // Read the data points with a 10-second timeout
                CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeOutSeconds));

                // Perform a directional read raw operation 
                var readRawResult = client.ReadRawAsync(readRawParam, null, cts.Token).GetAwaiter().GetResult();

                // Get the data points if succeded
                if (readRawResult.DataPoints != null && readRawResult.DataPoints.Count() > 0)
                {
                    var dps = readRawResult.DataPoints.Where(dp => dp.Value?.ToString() == datatolookup).ToList();
                    status = ReadRawStatus.Successful;
                    foreach (var dp in dps)
                    {
                        DateTimeOffset? tagAfterDT = DirectionalReadHelpers.LookupTriggerDateTimeNextTo(client, tag, new DateTimeOffset(dp.Time, TimeSpan.FromHours(0)));
                        DateTimeOffset? triggerAfterDt = DirectionalReadHelpers.LookUpDataMatrixDateTimeJustBefore(client, triggerTag, tagAfterDT ?? endDT);
                        DateTimeOffset? triggerBeforeDt = DirectionalReadHelpers.LookUpDataMatrixDateTimeJustBefore(client, triggerTag, new DateTimeOffset(dp.Time, TimeSpan.FromHours(0)));
                        
                        datapoints.Add(new SuperDataPoint()
                        {
                            DataPoint = dp,
                            TagAddress = new TagQueryIdentifier(tag),
                            FromDT = triggerBeforeDt ?? new DateTimeOffset(dp.Time, TimeSpan.FromHours(0)),
                            ToDT = triggerAfterDt > endDT ? endDT : triggerAfterDt ?? endDT
                        });
                    }
                }
            }

            return datapoints;
        }

        public static List<IDataPoint> TryReadRawSequencedIntervalsNoLookUp(
            FullyQualifiedTagName tag,
            DateTimeOffset beginDT,
            DateTimeOffset endDT,
            int timeOutSeconds,
            int hourInterval,
            out ReadRawStatus status)
        {
            // Initialize datetime limit for searching the data in dataPARC for settings stored on appsettings.json
            AppSettings settings = new AppSettings();

            // Create the OPC client and tag configuration
            var client = InterfaceHelpers.CreateOpcClient(settings);

            List<KeyValuePair<DateTimeOffset, DateTimeOffset>> intervals = DateTimeHelpers.GetSixHourIntervals(beginDT, endDT, hourInterval);

            List<IDataPoint> datapoints = new List<IDataPoint>();

            status = ReadRawStatus.NoValueFound;

            foreach (var interval in intervals)
            {
                var readRawParam = new ReadRawParameters()
                {
                    TagIdentifier = new TagQueryIdentifier(tag),
                    StartTime = interval.Key.UtcDateTime,
                    EndTime = interval.Value.UtcDateTime,
                    //ReturnStartBounds = ReturnRawBoundMode.NoBound,
                    //ReturnEndBounds = ReturnRawBoundMode.NoBound,
                    DigitalTextReturnType = DigitalTextReturnType.Text,
                };

                // Read the data points with a 10-second timeout
                CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeOutSeconds));

                // Perform a directional read raw operation 
                var readRawResult = client.ReadRawAsync(readRawParam, null, cts.Token).GetAwaiter().GetResult();

                // Get the data points if succeded
                if (readRawResult.DataPoints != null && readRawResult.DataPoints.Count() > 0)
                {
                    var dps = readRawResult.DataPoints.ToList();
                    status = ReadRawStatus.Successful;
                    foreach (var dp in dps)
                    {
                        datapoints.Add(dp);
                    }
                }
            }
            if (datapoints.Count() > 0)
                Console.WriteLine("Data point found.");
            return datapoints;
        }

    }

    public class DirectionalReadHelpers
    {
        /// <summary>
        /// Looks up the datetime associated with the trigger tag, starting from the provided datetime.
        /// </summary>
        /// <param name="client">The OPC client.</param>
        /// <param name="tag">The trigger tag.</param>
        /// <param name="startTime">The starting datetime for the lookup.</param>
        /// <param name="limitDt">The datetime limit for the lookup.</param>
        /// <returns>The datetime associated with the trigger tag, or null if not found.</returns>
        public static DateTimeOffset? LookupTriggerDateTimeNextTo(
            ReadClient client,
            FullyQualifiedTagName tag,
            DateTimeOffset startTime)
        {
            // Configure the read raw parameters to look forwards from the start time
            var param = new DirectionalReadRawParameters
            {
                TagIdentifier = new TagQueryIdentifier(tag),
                StartTime = startTime.UtcDateTime.AddSeconds(1),
                Direction = Direction.Forwards,
                NumberOfPoints = 1,
                ReturnStartBounds = ReturnRawBoundMode.NoBound,
                DigitalTextReturnType = DigitalTextReturnType.Numeric
            };

            // Read the data points with a 5-second timeout
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var result = client.DirectionalReadRawAsync(param, null, cts.Token).GetAwaiter().GetResult();

            // Return the first data point's datetime if the read was successful
            if (result.Status == ReadRawStatus.Successful && result.DataPoints.Count > 0)
            {
                return new DateTimeOffset(result.DataPoints.First().Time, TimeSpan.FromHours(0));
            }

            // No trigger datetime found
            return null;
        }

        /// <summary>
        /// Looks up the datetime associated with the trigger tag, starting from the provided datetime.
        /// </summary>
        /// <param name="client">The OPC client.</param>
        /// <param name="tag">The trigger tag.</param>
        /// <param name="startTime">The starting datetime for the lookup.</param>
        /// <param name="limitDt">The datetime limit for the lookup.</param>
        /// <returns>The datetime associated with the trigger tag, or null if not found.</returns>
        public static DateTimeOffset? LookupTriggerDateTimeNextTo(
            AppSettings settings,
            string tag,
            DateTimeOffset startTime)
        {
            // Create the OPC client and tag configuration
            var client = InterfaceHelpers.CreateOpcClient(settings);

            var ftag = InterfaceHelpers.CreateOpcTag(settings, tag);

            return LookupTriggerDateTimeNextTo(client, ftag, startTime);
        }
        /// <summary>
        /// Looks up the datetime associated with the datamatrix tag, starting from the provided datetime.
        /// </summary>
        /// <param name="client">The OPC client.</param>
        /// <param name="tag">The trigger tag.</param>
        /// <param name="startTime">The starting datetime for the lookup.</param>
        /// <param name="limitDt">The datetime limit for the lookup.</param>
        /// <returns>The datetime associated with the trigger tag, or null if not found.</returns>
        public static DateTimeOffset? LookUpDataMatrixDateTimeJustBefore(
            ReadClient client,
            FullyQualifiedTagName tag,
            DateTimeOffset startTime)
        {
            // Configure the read raw parameters to look forwards from the start time
            var param = new DirectionalReadRawParameters
            {
                TagIdentifier = new TagQueryIdentifier(tag),
                StartTime = startTime.UtcDateTime.AddSeconds(-1),
                Direction = Direction.Backwards,
                NumberOfPoints = 1,
                DigitalTextReturnType = DigitalTextReturnType.Text
            };

            // Read the data points with a 5-second timeout
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            var result = client.DirectionalReadRawAsync(param, null, cts.Token)
                .GetAwaiter()
                .GetResult();

            // Return the first data point's datetime if the read was successful
            if (result.Status == ReadRawStatus.Successful && result.DataPoints.Count > 0)
            {
                return new DateTimeOffset(result.DataPoints.First().Time, TimeSpan.FromHours(0));
            }

            // No trigger datetime found
            return null;
        }

        public static DateTimeOffset? LookUpDataMatrixDateTimeJustBefore(
            AppSettings appSettings,
            string tag,
            DateTimeOffset startTime)
        {
            // Initialize OPC client and tag configuration
            var client = InterfaceHelpers.CreateOpcClient(appSettings);
            return LookUpDataMatrixDateTimeJustBefore(client, InterfaceHelpers.CreateOpcTag(appSettings, tag), startTime);
        }

        /// <summary>
        /// Lookups the trigger date time next to.
        /// </summary>
        public static DateTimeOffset? LookupTriggerDateTimeNextTo(
        ReadClient client,
        AppSettings appSettings,
        string tag,
        DateTimeOffset startTime)
        => LookupTriggerDateTimeNextTo(client, InterfaceHelpers.CreateOpcTag(appSettings, tag), startTime);

        /// <summary>
        /// Retrieves the value of an OPC tag for a specified time range.
        /// </summary>
        /// <param name="beginDT">The start time of the lookup period</param>
        /// <param name="endDT">The end time of the lookup period</param>
        /// <param name="tagName">The name of the OPC tag to query</param>
        /// <returns>The value of the OPC tag as a string, or "null" if no value is found</returns>
        /// <exception cref="OpcLookupException">Thrown when an error occurs during OPC tag value retrieval</exception>
        public static string DirectionalOpcItemValueLookup(DateTimeOffset startTime, string tagName, out string error)
        {
            // Initialize error value
            error = string.Empty;

            // Validate input parameters
            if (!DateTimeHelpers.IsValidDateTime(startTime))
                return string.Empty;

            // Load application configuration
            var appSetting = new AppSettings();

            // Initialize OPC client and tag configuration
            var tag = InterfaceHelpers.CreateOpcTag(appSetting, tagName);
            var client = InterfaceHelpers.CreateOpcClient(appSetting);

            try
            {
                // First attempt: Try to read raw data
                var point = TryDirectionalReadRawData(client, tag, startTime);

                // Return the first point's value if available, otherwise return "null"
                return point?.Value?.ToString() ?? "null";
            }
            catch (Exception ex)
            {
                error = new StringHelpers.OpcLookupException(startTime, tagName, ex).ToString();
                return string.Empty;
            }
        }

        /// <summary>
        /// Attempts to read raw data from the OPC server
        /// </summary>
        public static IDataPoint? TryDirectionalReadRawData(ReadClient client, FullyQualifiedTagName tag, DateTimeOffset startTime)
        {
            // Read the data points with a 10-second timeout
            using (CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
            {
                var dirReadParam = new DirectionalReadRawParameters
                {
                    TagIdentifier = new TagQueryIdentifier(tag),
                    StartTime = startTime.UtcDateTime,
                    Direction = Direction.Backwards,
                    NumberOfPoints = 1
                };

                var result = client.DirectionalReadRawAsync(dirReadParam, null, cts.Token).GetAwaiter().GetResult();
                return result.Status == ReadRawStatus.NoValueFound ? null : result.DataPoints.FirstOrDefault();
            }
        }
    }

    public class ParameterHelpers
    {
        /// <summary>
        /// Creates the read raw parameter collection.
        /// </summary>
        public static List<DirectionalReadRawParameters> CreateDirectionalReadRawParameterCollection(
            AppSettings appSettings,
            IEnumerable<KeyValuePair<string, string>> TagNameValues,
            DateTimeOffset startTime)
        {
            List<DirectionalReadRawParameters> parameters = new List<DirectionalReadRawParameters>();

            foreach (var tag in TagNameValues)
            {
                parameters.Add(new DirectionalReadRawParameters()
                {
                    TagIdentifier = new TagQueryIdentifier(InterfaceHelpers.CreateOpcTag(appSettings, tag.Key)),
                    StartTime = startTime.UtcDateTime,
                    Direction = Direction.Backwards,
                    NumberOfPoints = 1,
                    DigitalTextReturnType = DigitalTextReturnType.Text,
                });
            }
            return parameters;
        }

        /// <summary>
        /// Creates the read raw parameter collection.
        /// </summary>
        public static List<ReadRawParameters> CreateReadRawParameterCollection(
            AppSettings appSettings,
            IEnumerable<KeyValuePair<string, string>> TagNameValues,
            DateTimeOffset[] dateTimes)
        {
            List<ReadRawParameters> readRawParams = new List<ReadRawParameters>();

            foreach (var tag in TagNameValues)
            {
                readRawParams.Add(new ReadRawParameters()
                {
                    TagIdentifier = new TagQueryIdentifier(InterfaceHelpers.CreateOpcTag(appSettings, tag.Key)),
                    StartTime = dateTimes[0].UtcDateTime,
                    EndTime = dateTimes[1].UtcDateTime,
                    DigitalTextReturnType = DigitalTextReturnType.Text,
                });
            }
            return readRawParams;
        }

        /// <summary>
        /// Creates the read at time parameter collection.
        /// </summary>
        public static List<ReadAtTimeParameters> CreateReadAtTimesParameterCollection(
            AppSettings appSettings,
            IEnumerable<KeyValuePair<string, string>> TagNameValues,
            DateTimeOffset[] dateTimes)
        {
            List<ReadAtTimeParameters> readAtTimeParams = new List<ReadAtTimeParameters>();
            dateTimes[1] = dateTimes[1].AddSeconds(-1);
            Parallel.ForEach(TagNameValues, tag =>
            {
                readAtTimeParams.Add(new ReadAtTimeParameters
                {
                    TagIdentifier = new TagQueryIdentifier(InterfaceHelpers.CreateOpcTag(appSettings, tag.Key)),
                    Timestamps = dateTimes.Select(dto => dto.UtcDateTime).ToArray(),
                    UseSimpleBounds = true,
                    DigitalTextReturnType = DigitalTextReturnType.Text,
                    TreatUncertainAsBad = true,
                    InterpolateType = InterpolateType.State
                });
            });
            return readAtTimeParams;
        }
    }
}
