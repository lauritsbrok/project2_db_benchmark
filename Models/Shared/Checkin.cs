using System;
using System.Text.Json.Serialization;

namespace project2_db_benchmark.Models.Shared
{
    public class Checkin
    {
        [JsonPropertyName("business_id")]
        public string BusinessId { get; set; }

        [JsonPropertyName("date")]
        public string Date { get; set; }
    }
}