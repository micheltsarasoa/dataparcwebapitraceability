namespace WebServiceTracability.Models
{
    public class MachineStationDatamatricesAtTime
    {
        public required string Machine { get; set; }
        public required string Station { get; set; }
        public required DateTimeOffset FromDT { get; set; }
        public required DateTimeOffset ToDT { get; set; }
        public required string DataMatrixTagName { get; set; }
        public required string TagNameTrigger { get; set; }
        public IDictionary<string, string> TagNameEmptyValues { get; set; }
        public required List<DataMatrixAtTime> DatamatrixAtTimes { get; set; }
    }

    public class DataMatrixAtTime
    {
        public string DataMatrix { get; set; }
        public DateTimeOffset[] DateTimes { get; set; }
        public IDictionary<string, string> TagNameValues { get; set; }
    }
}
