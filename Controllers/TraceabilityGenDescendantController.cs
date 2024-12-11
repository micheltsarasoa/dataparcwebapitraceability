using dataPARC.Store.EnterpriseCore.Entities;
using dataPARC.Store.EnterpriseCore.History.Enums;
using dataPARC.Store.EnterpriseCore.History.Inputs;
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
    [Route("api/v1/TraceabilityGenDesc")]
    [ApiController]
    public class TraceabilityGenDescendantController : ControllerBase
    {
        /// <summary>
        /// The logger
        /// </summary>
        private readonly ILogger<TraceabilityGenDescendantController> _logger;

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
        /// Initializes a new instance of the <see cref="TraceabilityGenDescendantController"/> class.
        /// </summary>
        /// <param name="logger">The logger.</param>
        public TraceabilityGenDescendantController(ILogger<TraceabilityGenDescendantController> logger)
        {
            _logger = logger;
        }

        [HttpPost]
        public IActionResult ActionResultAsync([FromBody] GenDescObject request)
        {
            // Get parameters 
            AppSettings settings = new AppSettings();

            // Validate the input data
            if (!ModelState.IsValid)
            {
                _logger.LogWarning("Descending Genealogy - Invalid input data: {ModelState}", ModelState);
                return BadRequest(ModelState);
            }

            // Validate ToDT and FromDT
            if (request.FromDT < settings.LimitDT)
                request.FromDT = settings.LimitDT;

            if (request.ToDT > DateTimeOffset.UtcNow)
                request.ToDT = DateTimeOffset.UtcNow;

            // Initialize the variable to put the http status code
            HttpStatusCode httpStatusCode = HttpStatusCode.OK;
            string errorMsg = string.Empty;

            // Parallel options for parallel foreach
            var options = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount, CancellationToken = CancellationToken.None };

            // Process foreach machinestation 
            Parallel.ForEach(request.MachineStations.OrderByDescending(ms => ms.Machine), options, machineStation =>
            {
                // Get all machinestations for each existing point in the master timestamp
                var newMachineStations = GetEveryPointsHavingDatamatrix(machineStation: machineStation,
                                                                        tag: InterfaceHelpers.CreateOpcTag(settings, machineStation.TagNameDatamatrix),
                                                                        triggertag: InterfaceHelpers.CreateOpcTag(settings, machineStation.TagNameTrigger),
                                                                        dataMatrix: request.Datamatrix,
                                                                        toDT: request.ToDT,
                                                                        limitDT: request.FromDT,
                                                                        includeRework: request.IncludeRework);
                request.MachineStations.Remove(machineStation);
                request.MachineStations.AddRange(newMachineStations);
            });

            // Process foreach machinestation
            Parallel.ForEach(request.MachineStations, options, machineStation =>
            {
                try
                {
                    string error = string.Empty;

                    foreach (var keyValuePair in machineStation.TagNames)
                    {
                        string value = DirectionalReadHelpers.DirectionalOpcItemValueLookup(machineStation.ToDT, keyValuePair.Key, out error);

                        if (!string.IsNullOrEmpty(value))
                        {
                            machineStation.TagNames[keyValuePair.Key] = value;
                            errorMsg += error;
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Handle exception properly
                    _logger.LogError("Descending Genealogy - Error on controller." + ex.Message);
                    httpStatusCode = HttpStatusCode.InternalServerError;
                    errorMsg = ex.Message;
                }
            });

            if (request.MachineStations.Count > 0)
            {
                request.FromDT = request.MachineStations.Min(x => x.FromDT);
                request.ToDT = request.MachineStations.Max(x => x.ToDT);
            }

            // Returns conditional responseDataMatrices
            switch ((int)httpStatusCode)
            {
                case OK:
                    // Return OK status code with responseDataMatrices object
                    _logger.LogInformation($"Descending Genealogy - Operation success on retreiving data for {request.Datamatrix}");
                    return Ok(request);

                case PartialContent:
                    // Return PartialContent status code with responseDataMatrices object
                    _logger.LogWarning($"Descending Genealogy - Could not retreive all data refer to {request.Datamatrix}. Partial result.");
                    return StatusCode(PartialContent, request);

                case NoContent:
                    // Return NoContent status code with no responseDataMatrices object
                    _logger.LogError($"Descending Genealogy - Could not retreive data refer to {request.Datamatrix}. No Content.");
                    return NoContent();

                case RequestTimeout:
                    // Return RequestTimeout status code with no responseDataMatrices object
                    _logger.LogError($"Descending Genealogy - Request timeout on retreiving data refer to {request.Datamatrix}." + errorMsg);
                    return StatusCode(RequestTimeout);

                case NotFound:
                    // Return Not Found status with the same request object
                    _logger.LogError($"Descending Genealogy - Not Found data refer to {request.Datamatrix}." + errorMsg);
                    return NotFound();

                case Unauthorized:
                    // Return Unauthorized status code with no responseDataMatrices object
                    _logger.LogError($"Descending Genealogy - Unauthorized status on retreiving data refer to {request.Datamatrix}." + errorMsg);
                    return Unauthorized();

                case InternalServerError:
                    // Return Internal Server Error status code 
                    _logger.LogError($"Descending Genealogy - Internal Server Error status on retreiving data refer to {request.Datamatrix}." + errorMsg);
                    return StatusCode((int)HttpStatusCode.InternalServerError);

                case ServiceUnavailable:
                    // Return 
                    _logger.LogError($"Descending Genealogy - Service unavailable on retreiving data refer to {request.Datamatrix}." + errorMsg);
                    return StatusCode((int)HttpStatusCode.ServiceUnavailable);

                default:
                    // Return BadRequest status code with responseDataMatrices object
                    _logger.LogError($"Descending Genealogy - Return bad request status on retreiving data refer to {request.Datamatrix}." + errorMsg);
                    return BadRequest(request);
            }
        }

        /// <summary>
        /// Gets the every points having datamatrix.
        /// </summary>
        /// <param name="request">The request.</param>
        /// <returns></returns>
        public static List<GenDescMachineStation> GetEveryPointsHavingDatamatrix(
            GenDescMachineStation machineStation,
            FullyQualifiedTagName tag,
            FullyQualifiedTagName triggertag,
            string dataMatrix,
            DateTimeOffset toDT,
            DateTimeOffset limitDT,
            bool includeRework)
        {
            // Load the application settings
            var appSettings = new AppSettings();

            // Create the OPC client and tag configuration
            var client = InterfaceHelpers.CreateOpcClient(appSettings);

            // Initialize the list of gendescmachinestation to be returned
            List<GenDescMachineStation> newMachineStations = new List<GenDescMachineStation>();

            // Create intervals of datetime to be use at 
            int hourInterval = 6;
            List<KeyValuePair<DateTimeOffset, DateTimeOffset>> intervals = DateTimeHelpers.GetSixHourIntervals(limitDT, toDT, hourInterval);

            // Boolean to put status found
            bool isDpFound = false;

            try
            {
                foreach (var interval in intervals.OrderByDescending(i => i.Value))
                {
                    // Initialize the read raw parameters
                    var readRawParam = new ReadRawParameters()
                    {
                        TagIdentifier = new TagQueryIdentifier(tag),
                        StartTime = interval.Key.UtcDateTime,
                        EndTime = interval.Value.UtcDateTime,
                        ReturnStartBounds = ReturnRawBoundMode.NoBound,
                        ReturnEndBounds = ReturnRawBoundMode.NoBound,
                        DigitalTextReturnType = DigitalTextReturnType.Text,
                    };

                    // Read the data points with a 10-second timeout
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

                    // Perform a directional read raw operation 
                    var readRawResult = client.ReadRawAsync(readRawParam, null, cts.Token).GetAwaiter().GetResult();

                    // Get the data points if succeded
                    if (readRawResult.DataPoints != null && readRawResult.DataPoints.Count() > 0)
                    {
                        var points = readRawResult.DataPoints.ToList().Where(dp => dp.Value.ToString() == dataMatrix).OrderByDescending(dp => dp.Time);

                        if (points != null && points.Count() > 0)
                        {
                            isDpFound = true;

                            foreach (var dp in points)
                            {
                                var triggerAfterDt = DirectionalReadHelpers.LookupTriggerDateTimeNextTo(client, triggertag, new DateTimeOffset(dp.Time, TimeSpan.FromHours(0)));
                                var triggerBeforeDt = DirectionalReadHelpers.LookUpDataMatrixDateTimeJustBefore(client, triggertag, new DateTimeOffset(dp.Time, TimeSpan.FromHours(0)));

                                IDictionary<string, string> tags = StringHelpers.DeepCopy(machineStation.TagNames);

                                GenDescMachineStation currentMachineStation = new GenDescMachineStation()
                                {
                                    FromDT = new DateTimeOffset(dp.Time, TimeSpan.FromHours(0)),
                                    ToDT = triggerAfterDt ?? triggerBeforeDt.Value,
                                    Machine = machineStation.Machine,
                                    Station = machineStation.Station,
                                    TagNameDatamatrix = machineStation.TagNameDatamatrix,
                                    TagNameTrigger = machineStation.TagNameTrigger,
                                    TagNames = tags,
                                };

                                newMachineStations.Add(currentMachineStation);

                                // break for no include rework
                                if (isDpFound && !includeRework)
                                    break;
                            }
                        }
                    }
                }
            }
            catch (Exception)
            {

                throw;
            }

            return newMachineStations;
        }
        
    }
}