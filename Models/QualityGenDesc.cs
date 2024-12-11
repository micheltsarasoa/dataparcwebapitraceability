namespace WebServiceTracability.Models
{
    /// <summary>
    /// class for main object to send to the web service
    /// </summary>
    public class GenDescObject
    {
        public required DateTimeOffset FromDT { get; set; }
        public required DateTimeOffset ToDT { get; set; }
        public required bool IncludeRework { get; set; }
        public required string Datamatrix { get; set; }
        public required List<GenDescMachineStation> MachineStations { get; set; }
    }
    /// <summary>
    /// Class of level 1 of the obejct to send to the web service
    /// </summary>
    public class GenDescMachineStation
    {
        public required DateTimeOffset FromDT { get; set; }
        public required DateTimeOffset ToDT { get; set; }
        public required string Machine { get; set; }
        public required string Station { get; set; }
        public required string TagNameDatamatrix { get; set; }
        public required string TagNameTrigger { get; set; }
        public required IDictionary<string, string> TagNames { get; set; }
    }
}
