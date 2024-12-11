using dataPARC.Store.EnterpriseCore.Entities;
using dataPARC.Store.EnterpriseCore.History.Enums;
using dataPARC.Store.EnterpriseCore.History.Inputs;
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
    [Route("api/v1/Reliability")]
    [ApiController]
    public class TraceabilityReliabilityController : ControllerBase
    {
        /// <summary>
        /// The logger
        /// </summary>
        private readonly ILogger<TraceabilityReliabilityController> _logger;

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
        /// Initializes a new instance of the <see cref="TraceabilityReliabilityController"/> class.
        /// </summary>
        /// <param name="logger">The logger.</param>
        public TraceabilityReliabilityController(ILogger<TraceabilityReliabilityController> logger)
        {
            _logger = logger;
        }

        [HttpPost]
        public IActionResult ActionResultAsync([FromBody] ReliabilityObject request)
        {
            // Validate the input data
            if (!ModelState.IsValid)
            {
                _logger.LogWarning("Reliability - Invalid input data: {ModelState}", ModelState);
                return BadRequest(ModelState);
            }

            // Load the application settings
            var appSettings = new AppSettings();

            // Create the OPC client and tag configuration
            var client = InterfaceHelpers.CreateOpcClient(appSettings);

            // Set the start datetime 
            DateTime startDatetime = request.EndTime.UtcDateTime.AddHours(-6);
            DateTime limitDatetime = request.StartTime.UtcDateTime;
            DateTime endDateTime = request.EndTime.UtcDateTime;

            // Initialize the variable to put the http status code
            HttpStatusCode httpStatusCode = HttpStatusCode.NotFound;
            string errorMsg = string.Empty;
                        
            // Process foreach machinestation 
            foreach (var tag in request.TagNames.OrderByDescending(tag => tag.Sequence))
            {
                // Create the fullqualifytagname for the datamatrix tag 
                var tagKey = InterfaceHelpers.CreateOpcTag(appSettings, tag.TagAddress);

                // Construct the parameter needed
                var param = new ReadRawParameters
                {
                    TagIdentifier = new TagQueryIdentifier(tagKey),
                    StartTime = startDatetime,
                    EndTime =  endDateTime,
                    DigitalTextReturnType = DigitalTextReturnType.Text
                };

                // Initialize bool for checker
                bool isDataPointFound = false;

                try
                {
                    while(!isDataPointFound && param.StartTime > limitDatetime) 
                    {
                        // Read the data points with a 5-second timeout
                        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

                        var result = client.ReadRawAsync(param, null, cts.Token).GetAwaiter().GetResult();

                        if (result.Status == ReadRawStatus.Successful)
                        {
                            // Look for the first data point with the desired value
                            var dataPoint = result.DataPoints.ToList().Where(dp => dp.Value.ToString() == request.DataMatrix).FirstOrDefault();

                            if (dataPoint != null)
                            {
                                httpStatusCode = HttpStatusCode.OK;
                                isDataPointFound = true;
                                endDateTime = dataPoint.Time;
                                tag.IsRetrieved = true;
                                break;
                            }
                        }
                        // Update the start time to the endtime minus 6 hours 
                        param.EndTime = param.StartTime;
                        param.StartTime = param.EndTime.AddHours(-6) < limitDatetime ? limitDatetime : param.EndTime.AddHours(-6) ;
                    }
                }
                catch (Exception ex)
                {
                    // Handle exception properly
                    _logger.LogError("Reliability - Error on controller." + ex.Message);
                    return StatusCode((int)HttpStatusCode.InternalServerError, ex.Message);
                }
            }

            // Take the status of the reliability
            bool hasSuccess = !(request.TagNames.Any(tag => tag.IsRetrieved == false));

            // Returns conditional responseDataMatrices
            switch ((int)httpStatusCode)
            {
                case OK:
                    // Return OK status code with responseDataMatrices object
                    _logger.LogInformation($"Reliability - Operation success on retreiving data for {request.DataMatrix}");
                    return Ok(request);

                case NoContent:
                    // Return NoContent status code with no responseDataMatrices object
                    _logger.LogError($"Reliability - Could not retreive data refer to {request.DataMatrix}. No Content.");
                    return NoContent();

                case RequestTimeout:
                    // Return RequestTimeout status code with no responseDataMatrices object
                    _logger.LogError($"Reliability - Request timeout on retreiving data refer to {request.DataMatrix}." + errorMsg);
                    return StatusCode(RequestTimeout);

                case NotFound:
                    // Return Not Found status with the same request object
                    _logger.LogError($"Reliability - Not Found data refer to {request.DataMatrix}." + errorMsg);
                    return Ok(request);

                case Unauthorized:
                    // Return Unauthorized status code with no responseDataMatrices object
                    _logger.LogError($"Reliability - Unauthorized status on retreiving data refer to {request.DataMatrix}." + errorMsg);
                    return Unauthorized();

                case InternalServerError:
                    // Return Internal Server Error status code 
                    _logger.LogError($"Reliability - Internal Server Error status on retreiving data refer to {request.DataMatrix}." + errorMsg);
                    return StatusCode((int)HttpStatusCode.InternalServerError);

                case ServiceUnavailable:
                    // Return 
                    _logger.LogError($"Reliability - Service unavailable on retreiving data refer to {request.DataMatrix}." + errorMsg);
                    return StatusCode((int)HttpStatusCode.ServiceUnavailable);

                default:
                    // Return BadRequest status code with responseDataMatrices object
                    _logger.LogError($"Reliability - Return bad request status on retreiving data refer to {request.DataMatrix}." + errorMsg);
                    return BadRequest(request);
            }
        }
    }
}