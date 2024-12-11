using dataPARC.Store.EnterpriseCore.Entities;
using dataPARC.Store.EnterpriseCore.History.Enums;
using dataPARC.Store.EnterpriseCore.History.Inputs;
using Microsoft.AspNetCore.Mvc;
using System.Net;
using WebServiceTracability;
using WebServiceTracability.Services;

namespace webapitraceability.Controllers
{
    [Route("api/v1/LookUpDatamatrix")]
    [ApiController]
    public class TraceabilityLookUpDatamatrixController: ControllerBase
    {
        /// <summary>
        /// The logger
        /// </summary>
        private readonly ILogger<TraceabilityLookUpDatamatrixController> _logger;

        /// <summary>
        /// Define constants for HTTP status codes
        /// </summary>
        private const int OK = (int)HttpStatusCode.OK;
        private new const int NoContent = (int)HttpStatusCode.NoContent;
        private const int RequestTimeout = (int)HttpStatusCode.RequestTimeout;
        private new const int NotFound = (int)HttpStatusCode.NotFound;
        private new const int Unauthorized = (int)HttpStatusCode.Unauthorized;
        private const int InternalServerError = (int)HttpStatusCode.InternalServerError;
        private const int ServiceUnavailable = (int)HttpStatusCode.ServiceUnavailable;

        /// <summary>
        /// Initializes a new instance of the <see cref="TraceabilityLookUpDatamatrixController"/> class.
        /// </summary>
        /// <param name="logger">The logger.</param>
        public TraceabilityLookUpDatamatrixController(ILogger<TraceabilityLookUpDatamatrixController> logger)
        {
            _logger = logger;
        }

        [HttpPost]
        public IActionResult ActionResult([FromBody] LineGroupSeqsResult requestObject)
        {
            // Validate the input data
            if (!ModelState.IsValid)
            {
                _logger.LogWarning("Look Up Datamatrix position - Invalid input data: {ModelState}", ModelState);
                return BadRequest(ModelState);
            }

            // Load the application settings
            var appSettings = new AppSettings();

            // Create the OPC client and tag configuration
            var client = InterfaceHelpers.CreateOpcClient(appSettings);

            // Set the start datetime 
            DateTime startDatetime = requestObject.ToDT.UtcDateTime.AddHours(-6) < appSettings.LimitDT.UtcDateTime ? appSettings.LimitDT.UtcDateTime : requestObject.ToDT.UtcDateTime.AddHours(-6);
            DateTime limitDatetime = requestObject.FromDT.UtcDateTime < appSettings.LimitDT.UtcDateTime ? appSettings.LimitDT.UtcDateTime : requestObject.FromDT.UtcDateTime;
            DateTime endDateTime = requestObject.ToDT.UtcDateTime;

            // Initialize the variable to put the http status code
            HttpStatusCode httpStatusCode = HttpStatusCode.NotFound;
            string errorMsg = string.Empty;

            // Process foreach
            foreach (LineLineGroupSeq lineLineGroupSeq in requestObject.LineLineGroupSeqs.OrderByDescending(ls => ls.LineGroupSeq).ThenByDescending(ls => ls.LineSeq))
            {
                // Create the fullqualifytagname for the datamatrix tag 
                var tagKey = InterfaceHelpers.CreateOpcTag(appSettings, lineLineGroupSeq.TagName);

                // Construct the parameter needed for Read Raw Parameter
                var param = new ReadRawParameters
                {
                    TagIdentifier = new TagQueryIdentifier(tagKey),
                    StartTime = startDatetime,
                    EndTime = endDateTime,
                    DigitalTextReturnType = DigitalTextReturnType.Text
                };

                // Initialize bool for checker
                bool isDataPointFound = false;

                try
                {
                    while (!isDataPointFound && param.StartTime > limitDatetime)
                    {
                        // Read the data points with a 5-second timeout
                        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

                        // Get the result from dataPARC
                        var result = client.ReadRawAsync(param, null, cts.Token).GetAwaiter().GetResult();

                        if (result.Status == ReadRawStatus.Successful)
                        {
                            // Look for the first data point with the desired value
                            var dataPoint = result.DataPoints.ToList().Where(dp => dp.Value.ToString() == requestObject.DataMatrix).FirstOrDefault();

                            if (dataPoint != null)
                            {
                                httpStatusCode = HttpStatusCode.OK;
                                isDataPointFound = true;
                                endDateTime = dataPoint.Time;
                                lineLineGroupSeq.IsFound = true;
                                requestObject.FirstDatetime = new DateTimeOffset(dataPoint.Time, TimeSpan.FromHours(0));
                                break;
                            }
                        }
                        // Update the start time to the endtime minus 6 hours 
                        param.EndTime = param.StartTime;
                        param.StartTime = param.EndTime.AddHours(-6) < limitDatetime ? limitDatetime : param.EndTime.AddHours(-6);
                    }
                }
                catch (Exception ex)
                {
                    // Handle exception properly
                    _logger.LogError("Look Up Datamatrix position - Error on controller." + ex.Message);
                    return StatusCode((int)HttpStatusCode.InternalServerError, ex.Message);
                }
                if (isDataPointFound)
                    break;
            }

            // Returns conditional responseDataMatrices
            switch ((int)httpStatusCode)
            {
                case OK:
                    // Return OK status code with responseDataMatrices object
                    _logger.LogInformation($"Look Up Datamatrix position - Operation success on retreiving data for {requestObject.DataMatrix}");
                    return Ok(requestObject);

                case NoContent:
                    // Return NoContent status code with no responseDataMatrices object
                    _logger.LogError($"Look Up Datamatrix position - Could not retreive data refer to {requestObject.DataMatrix}. No Content.");
                    return NoContent();

                case RequestTimeout:
                    // Return RequestTimeout status code with no responseDataMatrices object
                    _logger.LogError($"Look Up Datamatrix position - Request timeout on retreiving data refer to {requestObject.DataMatrix}." + errorMsg);
                    return StatusCode(RequestTimeout);

                case NotFound:
                    // Return Not Found status with the same request object
                    _logger.LogError($"Look Up Datamatrix position - Not Found data refer to {requestObject.DataMatrix}." + errorMsg);
                    return NotFound(requestObject);

                case Unauthorized:
                    // Return Unauthorized status code with no responseDataMatrices object
                    _logger.LogError($"Look Up Datamatrix position - Unauthorized status on retreiving data refer to {requestObject.DataMatrix}." + errorMsg);
                    return Unauthorized();

                case InternalServerError:
                    // Return Internal Server Error status code 
                    _logger.LogError($"Look Up Datamatrix position - Internal Server Error status on retreiving data refer to {requestObject.DataMatrix}." + errorMsg);
                    return StatusCode((int)HttpStatusCode.InternalServerError);

                case ServiceUnavailable:
                    // Return 
                    _logger.LogError($"Look Up Datamatrix position - Service unavailable on retreiving data refer to {requestObject.DataMatrix}." + errorMsg);
                    return StatusCode((int)HttpStatusCode.ServiceUnavailable);

                default:
                    // Return BadRequest status code with responseDataMatrices object
                    _logger.LogError($"Look Up Datamatrix position - Return bad request status on retreiving data refer to {requestObject.DataMatrix}." + errorMsg);
                    return BadRequest();
            }
        }
     }

    /// <summary>
    /// Main object
    /// </summary>
    public class LineGroupSeqsResult
    {
        public required string DataMatrix { get; set; }
        public DateTimeOffset FromDT { get; set; }
        public DateTimeOffset ToDT { get; set; }
        public DateTimeOffset FirstDatetime { get; set; }
        public required List<LineLineGroupSeq> LineLineGroupSeqs { get; set; }
    }

    /// <summary>
    /// Class for LineGroup, Line Sequence
    /// </summary>
    public class LineLineGroupSeq
    {
        public required int LineGroupSeq { get; set; }
        public required int LineSeq { get; set; }
        public required int MachineStageId { get; set; }
        public required string TagName { get; set; }
        public required bool IsFound { get; set; }
    }
}
