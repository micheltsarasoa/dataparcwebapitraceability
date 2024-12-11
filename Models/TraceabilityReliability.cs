namespace WebServiceTracability.Models
{
    public class ReliabilityObject
    {
        public required string DataMatrix { get; set; }
        public required DateTimeOffset StartTime { get; set; }
        public required DateTimeOffset EndTime { get; set; }
        public required HashSet<TagName> TagNames { get; set; }
    }

    public class TagName
    {
        public required int Sequence { get; set; }
        public required string TagAddress { get; set; }
        public bool IsRetrieved { get; set; } = false;
    }
}
