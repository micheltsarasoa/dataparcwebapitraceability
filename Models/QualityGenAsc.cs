using dataPARC.Store.EnterpriseCore.DataPoints;
using dataPARC.Store.EnterpriseCore.Entities;

namespace WebServiceTracability.Models
{
    /// <summary>
    /// Class for the object to send to the web service
    /// </summary>
    public class QualityGenAsc
    {
        public required DateTimeOffset FromDT { get; set; }
        public required DateTimeOffset ToDT { get; set; }
        public required string DataToLookUp { get; set; }
        public required List<GenAscMachineStation> MachineStations { get; set; }
    }

    /// <summary>
    /// Class of level 1 of the obejct to send to the web service
    /// </summary>
    public class GenAscMachineStation
    {
        public required string Machine { get; set; }
        public required string Station { get; set; }
        public required DateTimeOffset FromDT { get; set; }
        public required DateTimeOffset ToDT { get; set; }
        public required string TagNameDatamatrix { get; set; }
        public required string TagNameTrigger { get; set; }
        public List<string> TagAddresses { get; set; }
        public List<DataMatrix> ListOfDataMatrix { get; set; }
    }

    /// <summary>
    /// Class of level 1 of the obejct to send to the web service
    /// </summary>
    public class DataMatrix
    {
        public required string Nameplate { get; set; }
        public string Datamatrix { get; set; }
        public required DateTimeOffset CreatedDT { get; set; }
    }

    /// <summary>
    /// Class of object Tag Item
    /// </summary>
    public class SuperDataPoint 
    {
        public required TagQueryIdentifier TagAddress { get; set; }
        public IDataPoint? DataPoint { get; set; }
        public DateTimeOffset FromDT { get; set; }
        public DateTimeOffset ToDT { get; set; }
    }
}
