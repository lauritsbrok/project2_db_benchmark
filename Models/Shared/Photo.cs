using System.Text.Json.Serialization;

#pragma warning disable CS8618

namespace project2_db_benchmark.Models.Shared
{
    public class Photo
    {
        [JsonPropertyName("photo_id")]
        public string PhotoId { get; set; }

        [JsonPropertyName("business_id")]
        public string BusinessId { get; set; }

        [JsonPropertyName("caption")]
        public string Caption { get; set; }

        [JsonPropertyName("label")]
        public string Label { get; set; }
    }
}

#pragma warning restore CS8618