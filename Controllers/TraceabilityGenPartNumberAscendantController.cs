﻿using dataPARC.Authorization.CertificateValidation;
using dataPARC.Store.EnterpriseCore.Entities;
using dataPARC.Store.EnterpriseCore.History.Enums;
using dataPARC.Store.EnterpriseCore.History.Inputs;
using dataPARC.Store.SDK;
using dataPARC.TimeSeries.Core.Enums;
using Microsoft.AspNetCore.Mvc;
using System.Net;
using WebServiceTracability.Models;
using WebServiceTracability.Services;

namespace WebServiceTracability.Controllers
{

    /// <summary>
    /// Traceability Data Request Controller for Ascendant Genealogy
    /// </summary>
    /// <seealso cref="Microsoft.AspNetCore.Mvc.ControllerBase" />
    [Route("api/v1/TraceabilityGenAscPartNumber")]
    [ApiController]
    public class TraceabilityGenPartNumberAscendantController : ControllerBase
    {
        /// <summary>
        /// The logger
        /// </summary>
        private readonly ILogger<TraceabilityGenPartNumberAscendantController> _logger;

        /// <summary>
        /// Define constants for HTTP status codes
        /// </summary>
        private const int OK = (int)HttpStatusCode.OK;
        private const int PartialContent = (int)HttpStatusCode.PartialContent;
        private new const int NoContent = (int)HttpStatusCode.NoContent;
        private const int RequestTimeout = (int)HttpStatusCode.RequestTimeout;
        private new const int NotFound = (int)HttpStatusCode.NotFound;
        private new const int Unauthorized = (int)HttpStatusCode.Unauthorized;
        private const int InternalServerError = (int)HttpStatusCode.InternalServerError;
        private const int ServiceUnavailable = (int)HttpStatusCode.ServiceUnavailable;

        /// <summary>
        /// Initializes a new instance of the <see cref="TraceabilityGenPartNumberAscendantController"/> class.
        /// </summary>
        /// <param name="logger">The logger.</param>
        public TraceabilityGenPartNumberAscendantController(ILogger<TraceabilityGenPartNumberAscendantController> logger)
        {
            _logger = logger;
        }

        [HttpPost]
        public IActionResult ActionResult([FromBody] QualityGenAsc request)
        {
            // Validate the input data
            if (!ModelState.IsValid)
            {
                _logger.LogWarning("PartNumber Ascending Genealogy - Invalid input data: {ModelState}", ModelState);
                return BadRequest(ModelState);
            }

            // Initialize datetime limit for searching the data in dataPARC for settings stored on appsettings.json
            AppSettings settings = new AppSettings();

            // Create the OPC client and tag configuration
            var client = new ReadClient(settings.HOST, settings.PORT, CertificateValidation.AcceptAllCertificates);

            // datatolookup
            string datatolookup = request.DataToLookUp;

            // Initiliaze limit date time
            DateTimeOffset limitDT = DateTimeHelpers.AdjustDateTimeRange(request.FromDT, settings.LimitDT);

            // Datetime validation
            request.FromDT = DateTimeHelpers.AdjustDateTimeRange(request.FromDT, limitDT);
            request.ToDT = request.ToDT > DateTimeOffset.UtcNow ? DateTimeOffset.UtcNow : request.ToDT;

            // datetime
            DateTimeOffset beginDT = request.FromDT;
            DateTimeOffset endDT = request.ToDT;

            // Initialize the variable to put the http status code
            HttpStatusCode httpStatusCode = HttpStatusCode.NotFound;
            string errorMsg = string.Empty;

            // Parallel options for parallel foreach
            var options = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount, CancellationToken = CancellationToken.None };

            // Create parameters
            Parallel.ForEach(request.MachineStations, options, machineStation =>
            {
                List<SuperDataPoint> spdatapoints = new List<SuperDataPoint>();

                foreach (var tagAddress in machineStation.TagAddresses)
                {
                    // Create a FullyQualifiedTagName object with the interface and tag address from the request body
                    var tag = InterfaceHelpers.CreateOpcTag(settings, tagAddress);

                    List<KeyValuePair<DateTimeOffset, DateTimeOffset>> intervals = DateTimeHelpers.GetSixHourIntervals(beginDT, endDT, 12);

                    foreach (var interval in intervals)
                    {
                        List<SuperDataPoint> tmpspDps = ReadRawHelpers.TryReadRawSequencedIntervalsPart(
                                tag,
                                InterfaceHelpers.CreateOpcTag(settings, machineStation.TagNameTrigger),
                                interval.Key,
                                interval.Value,
                                15,
                                12,
                                datatolookup,
                                out ReadRawStatus status);

                        if (tmpspDps.Count() == 0 || status != ReadRawStatus.Successful)
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

                            // Perform a directional read raw operation 
                            var readAtTimeResult = client.ReadAtTimeAsync(readAtTimeParam).GetAwaiter().GetResult();

                            // Get the data points if succeded
                            if (readAtTimeResult.DataPoints != null)
                            {
                                foreach (var sdp in readAtTimeResult.DataPoints.Where(dp => dp.Value.ToString() == datatolookup))
                                {
                                    DateTimeOffset? triggeAfterDt = DirectionalReadHelpers.LookupTriggerDateTimeNextTo(settings, machineStation.TagNameTrigger, interval.Value);
                                    DateTimeOffset? triggerBeforeDt = DirectionalReadHelpers.LookUpDataMatrixDateTimeJustBefore(settings, machineStation.TagNameTrigger, interval.Key);

                                    bool test = spdatapoints.Any(dp => dp.FromDT == triggerBeforeDt);
                                    test = spdatapoints.Any(dp => dp.ToDT == triggeAfterDt);
                                    test = spdatapoints.Any(dp => dp.TagAddress.FullyQualifiedTagName.Tag == tagAddress);

                                    if (triggeAfterDt != null && triggerBeforeDt != null)
                                    {
                                        triggeAfterDt = triggeAfterDt.Value > request.ToDT ? request.ToDT : triggeAfterDt;
                                        triggerBeforeDt = triggerBeforeDt.Value < request.FromDT ? request.FromDT : triggerBeforeDt;

                                        if (!spdatapoints.Any(dp => dp.FromDT == triggerBeforeDt && dp.ToDT == triggeAfterDt && dp.TagAddress.FullyQualifiedTagName.Tag == tagAddress))
                                        {
                                            spdatapoints.Add(new SuperDataPoint()
                                            {
                                                DataPoint = sdp,
                                                TagAddress = new TagQueryIdentifier(tag),
                                                FromDT = triggerBeforeDt ?? new DateTimeOffset(sdp.Time, TimeSpan.FromHours(0)),
                                                ToDT = triggeAfterDt > request.ToDT ? request.ToDT : triggeAfterDt ?? request.ToDT,
                                            });
                                        }
                                    }
                                }
                            }
                        }

                        else
                        {
                            spdatapoints.AddRange(tmpspDps);
                        }
                    }
                }

                foreach (var sdatapoint in spdatapoints)
                {
                    // Initialize a dm to hold result 
                    List<DataMatrix> dms = new List<DataMatrix>();

                    // Create a FullyQualifiedTagName object with the interface and tag address from the machinestation
                    FullyQualifiedTagName tagDmKey = InterfaceHelpers.CreateOpcTag(settings, machineStation.TagNameDatamatrix);

                    // Split the timespan into an interval
                    List<KeyValuePair<DateTimeOffset, DateTimeOffset>> intervals =
                        DateTimeHelpers.GetSixHourIntervals(sdatapoint.FromDT, sdatapoint.ToDT, 6);

                    // Read data for each interval
                    foreach (var interval in intervals)
                    {
                        var readRawParam = new ReadRawParameters()
                        {
                            TagIdentifier = new TagQueryIdentifier(tagDmKey),
                            StartTime = interval.Key.UtcDateTime,
                            EndTime = interval.Value.UtcDateTime,
                            ReturnStartBounds = ReturnRawBoundMode.NoBound,
                            ReturnEndBounds = ReturnRawBoundMode.NoBound,
                            DigitalTextReturnType = DigitalTextReturnType.Text,
                        };

                        // Read the data points with a 5-second timeout
                        CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

                        // Perform a directional read raw operation 
                        var readRawResult = client.ReadRawAsync(readRawParam, null, cts.Token).GetAwaiter().GetResult();

                        // Get the data points if succeded
                        if (readRawResult.DataPoints != null && readRawResult.DataPoints.Count() > 0)
                        {
                            var dps = readRawResult.DataPoints.ToList();
                            foreach (var dp in dps)
                            {
                                if (!machineStation.ListOfDataMatrix.Any(dm => dm.Datamatrix == dp.Value.ToString()))
                                {
                                    machineStation.ListOfDataMatrix.Add(new DataMatrix
                                    {
                                        CreatedDT = new DateTimeOffset(dp.Time, TimeSpan.FromHours(0)),
                                        Nameplate = string.Empty,
                                        Datamatrix = dp.Value.ToString(),
                                    });
                                }
                            }
                        }
                    }
                }

                if (machineStation.ListOfDataMatrix.Count() > 0)
                {
                    machineStation.ToDT = machineStation.ListOfDataMatrix.Max(dm => dm.CreatedDT);
                    machineStation.FromDT = machineStation.ListOfDataMatrix.Min(dm => dm.CreatedDT);
                }
            });

            if (request.MachineStations.Select(ms => ms.TagNameDatamatrix).Any())
                httpStatusCode = HttpStatusCode.OK;

            // Returns conditional responseDataMatrices
            switch ((int)httpStatusCode)
            {
                case OK:
                    // Return OK status code with responseDataMatrices object
                    _logger.LogInformation($"PartNumber Ascending Genealogy - Operation success on retreiving data for {request.DataToLookUp}");
                    return Ok(request);

                case PartialContent:
                    // Return PartialContent status code with responseDataMatrices object
                    _logger.LogWarning($"PartNumber Ascending Genealogy - Could not retreive all data refer to {request.DataToLookUp}. Partial result.");
                    return StatusCode(PartialContent, request);

                case NoContent:
                    // Return NoContent status code with no responseDataMatrices object
                    _logger.LogError($"PartNumber Ascending Genealogy - Could not retreive data refer to {request.DataToLookUp}. No Content.");
                    return NoContent();

                case RequestTimeout:
                    // Return RequestTimeout status code with no responseDataMatrices object
                    _logger.LogError($"PartNumber Ascending Genealogy - Request timeout on retreiving data refer to {request.DataToLookUp}." + errorMsg);
                    return StatusCode(RequestTimeout);

                case NotFound:
                    // Return Not Found status with the same request object
                    _logger.LogError($"PartNumber Ascending Genealogy - Not Found data refer to {request.DataToLookUp}." + errorMsg);
                    return NotFound();

                case Unauthorized:
                    // Return Unauthorized status code with no responseDataMatrices object
                    _logger.LogError($"PartNumber Ascending Genealogy - Unauthorized status on retreiving data refer to {request.DataToLookUp}." + errorMsg);
                    return Unauthorized();

                case InternalServerError:
                    // Return Internal Server Error status code 
                    _logger.LogError($"PartNumber Ascending Genealogy - Internal Server Error status on retreiving data refer to {request.DataToLookUp}." + errorMsg);
                    return StatusCode((int)HttpStatusCode.InternalServerError);

                case ServiceUnavailable:
                    // Return 
                    _logger.LogError($"PartNumber Ascending Genealogy - Service unavailable on retreiving data refer to {request.DataToLookUp}." + errorMsg);
                    return StatusCode((int)HttpStatusCode.ServiceUnavailable);

                default:
                    // Return BadRequest status code with responseDataMatrices object
                    _logger.LogError($"PartNumber Ascending Genealogy - Return bad request status on retreiving data refer to {request.DataToLookUp}." + errorMsg);
                    return BadRequest(request);
            }
        }
    }
}