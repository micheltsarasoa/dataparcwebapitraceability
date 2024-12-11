using System.Text.Json;

namespace WebServiceTracability
{
    public class AppSettings
    {
        public DateTimeOffset LimitDT { get; internal set; }
        public string HOST { get; set; } = "2K19EN-SN-DPARC.projet.lan";
        public int PORT { get; set; } = 12340;
        public string INTERFACEGROUPNAME { get; set; } = "Simulation";
        public string INTERFACENAME { get; set; } = "Simulation";

        public AppSettings()
        {
            // Read the JSON file
            string jsonString = File.ReadAllText("appsettings.json");

            // Parse the JSON
            JsonDocument jsonDocument = JsonDocument.Parse(jsonString);

            // Extract the text 
            JsonElement root = jsonDocument.RootElement;
            JsonElement appSettings = root.GetProperty("AppSettings");
            string beginDTString = appSettings.GetProperty("LimitDT").GetString(); 

            // Get the value of "LastDT"
            LimitDT = DateTimeOffset.Parse(beginDTString);
        }
    }
}
