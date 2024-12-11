using dataPARC.Store.EnterpriseCore.DataPoints;
using dataPARC.Store.EnterpriseCore.History.Entities;
using dataPARC.Store.EnterpriseCore.History.Enums;
using dataPARC.Store.EnterpriseCore.History.Inputs;
using Google.Apis.Util;
using Microsoft.AspNetCore.Mvc;
using System.Net;
using WebServiceTracability.Models;
using WebServiceTracability.Services;

namespace WebServiceTracability.Controllers
{
    /// <summary>
    /// Look Up Datamatrices At Times
    /// </summary>
    /// <seealso cref="Microsoft.AspNetCore.Mvc.ControllerBase" />
    [Route("api/v1/GetAllDataMatrixAtTimes")]
    [ApiController]
    public class TraceabilityGetAllDatamatrixAtTimeController : ControllerBase
    {
        /// <summary>
        /// The logger
        /// </summary>
        private readonly ILogger<TraceabilityGetAllDatamatrixAtTimeController> _logger;

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
        /// Initializes a new instance of the <see cref="TraceabilityGetAllDatamatrixAtTimeController"/> class.
        /// </summary>
        /// <param name="logger">The logger.</param>
        public TraceabilityGetAllDatamatrixAtTimeController(ILogger<TraceabilityGetAllDatamatrixAtTimeController> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Actions the result.
        /// </summary>
        /// <param name="request">The request.</param>
        [HttpPost]
        public IActionResult ActionResult([FromBody] List<MachineStationDatamatricesAtTime> listOfObjects)
        {
            // Validate the input data
            if (!ModelState.IsValid)
            {
                _logger.LogError("Quality Data Report - Invalid input data: {ModelState}", ModelState);
                return BadRequest(ModelState);
            }

            // Load the application settings
            var appSettings = new AppSettings();

            // Create the OPC client and tag configuration
            var client = InterfaceHelpers.CreateOpcClient(appSettings);

            // Initialize the variable to put the http status code
            HttpStatusCode httpStatusCode = HttpStatusCode.NotFound;
            string errorMsg = string.Empty;

            // Foreach whole list
            Parallel.ForEach(listOfObjects, obj =>
            {
                try
                {
                    // Create the fullqualifytagname for the datamatrix tag 
                    var tagKey = InterfaceHelpers.CreateOpcTag(appSettings, obj.DataMatrixTagName);

                    // Read the data points with a 5-second timeout
                    using var ctsReadRaw = new CancellationTokenSource(TimeSpan.FromSeconds(5));

                    // Get the result from dataPARC
                    List<IDataPoint> points = ReadRawHelpers.TryReadRawData(appSettings, tagKey, obj.FromDT, obj.ToDT, out ReadRawStatus rwStatus);

                    // Get the list of found datapoints if successful 
                    if (rwStatus == ReadRawStatus.Successful && points.Count() > 0)
                    {
                        for (int i = 0; i < points.Count; i++)
                        {
                            IDataPoint? point = points[i];
                            if (obj.DatamatrixAtTimes.Where(d => d.DataMatrix == point.Value.ToString()).Count() == 0 && i < points.Count - 1)
                            {
                                obj.DatamatrixAtTimes.Add(new DataMatrixAtTime
                                {
                                    DataMatrix = point.Value.ToString(),
                                    DateTimes = [new DateTimeOffset(point.Time, TimeSpan.FromHours(0)), new DateTimeOffset(points.Where(p => p.Time > point.Time).First().Time, TimeSpan.FromHours(0))],
                                    TagNameValues = StringHelpers.DeepCopy(obj.TagNameEmptyValues),
                                });
                            }
                            else if (obj.DatamatrixAtTimes.Where(d => d.DataMatrix == point.Value.ToString()).Count() == 0 && i == points.Count - 1)
                            {
                                DateTimeOffset? boundDT = DirectionalReadHelpers.LookupTriggerDateTimeNextTo(client, appSettings, obj.TagNameTrigger, point.Time);
                                obj.DatamatrixAtTimes.Add(new DataMatrixAtTime
                                {
                                    DataMatrix = point.Value.ToString(),
                                    DateTimes = [new DateTimeOffset(point.Time, TimeSpan.FromHours(0)), boundDT ?? DateTimeOffset.UtcNow.DateTime],
                                    TagNameValues = StringHelpers.DeepCopy(obj.TagNameEmptyValues),
                                });
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Throw an exception with a custom error message and the original exception details
                    _logger.LogTrace($"Quality Data Report - Error while retrieving Opc Tag Value at the timestamp, between {obj.FromDT} and {obj.ToDT} (Opc Tag: {obj.DataMatrixTagName}). {ex}");
                    throw new Exception($"Quality Data Report - Error while retrieving Opc Tag Value at the timestamp, between {obj.FromDT} and {obj.ToDT} (Opc Tag: {obj.DataMatrixTagName}). {ex}");
                }

                if (obj.DatamatrixAtTimes.Count() > 0)
                {
                    obj.ToDT = obj.DatamatrixAtTimes.Max(dm => dm.DateTimes[1]);
                    obj.FromDT = obj.DatamatrixAtTimes.Min((dm => dm.DateTimes[0]));
                    httpStatusCode = HttpStatusCode.OK;
                }
            });

            List<Task> tasks = new List<Task>();

            // ReadAtTimeBulk and populate tagnamevalues to be returned
            foreach (var obj in listOfObjects)
            {
                var options = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount, CancellationToken = CancellationToken.None };

                try
                {
                    Parallel.ForEach(obj.DatamatrixAtTimes, options, dm =>
                    {
                        List<DirectionalReadRawParameters> readRawParameters = ParameterHelpers.CreateDirectionalReadRawParameterCollection(
                           appSettings: appSettings,
                           TagNameValues: dm.TagNameValues,
                           startTime: dm.DateTimes[1]);

                        List<ReadRawResult> readRawResults = readRawParameters.Select(rw => client.DirectionalReadRawAsync(rw).GetAwaiter().GetResult()).ToList();

                        foreach (var readRawResult in readRawResults.Where<ReadRawResult>(rw => rw.Status == ReadRawStatus.Successful && rw.DataPoints.Any()))
                        {
                            // Find the matching DataMatrixAtTime and update its TagNameValues
                            var matchingDm = obj.DatamatrixAtTimes.First(d => d.DataMatrix == dm.DataMatrix);
                            if (matchingDm != null && readRawResult.DataPoints.FirstOrDefault()?.Value != null)
                            {
                                matchingDm.TagNameValues[readRawResult.TagIdentifier.FullyQualifiedTagName.Tag] = readRawResult.DataPoints.OrderByDescending(dp => dp.Time).First().Value.ToString();
                            }
                        }
                    });
                }
                catch (Exception ex)
                {
                    // Return Internal Server Error status code 
                    _logger.LogError($"Quality Data Report - Internal Server Error status on retreiving data. {0}", ex);
                    return StatusCode((int)HttpStatusCode.InternalServerError);
                }
            }

            // Returns conditional 
            switch ((int)httpStatusCode)
            {
                case OK:
                    // Return OK status code with responseDataMatrices object
                    _logger.LogInformation($"Quality Data Report - Operation success on retreiving data.");
                    return Ok(listOfObjects);

                case PartialContent:
                    // Return PartialContent status code with responseDataMatrices object
                    _logger.LogWarning($"Quality Data Report - Could not retreive all data. Partial result.");
                    return StatusCode(PartialContent, listOfObjects);

                case NoContent:
                    // Return NoContent status code with no responseDataMatrices object
                    _logger.LogError($"Quality Data Report - Could not retreive data. No Content.");
                    return NoContent();

                case RequestTimeout:
                    // Return RequestTimeout status code with no responseDataMatrices object
                    _logger.LogError($"Quality Data Report - Request timeout on retreiving data.");
                    return StatusCode(RequestTimeout);

                case NotFound:
                    // Return Not Found status with the same request object
                    _logger.LogError($"Quality Data Report - Not Found data.");
                    return NotFound();

                case Unauthorized:
                    // Return Unauthorized status code with no responseDataMatrices object
                    _logger.LogError($"Quality Data Report - Unauthorized status on retreiving data.");
                    return Unauthorized();

                case InternalServerError:
                    // Return Internal Server Error status code 
                    _logger.LogError($"Quality Data Report - Internal Server Error status on retreiving data.");
                    return StatusCode((int)HttpStatusCode.InternalServerError);

                case ServiceUnavailable:
                    // Return 
                    _logger.LogError($"Quality Data Report - Service unavailable on retreiving data refer");
                    return StatusCode((int)HttpStatusCode.ServiceUnavailable);

                default:
                    // Return BadRequest status code with responseDataMatrices object
                    _logger.LogError($"Quality Data Report - Return bad request status on retreiving data.");
                    return BadRequest(listOfObjects);
            }
        }
    }
}
