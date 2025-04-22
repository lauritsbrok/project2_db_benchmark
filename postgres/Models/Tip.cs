using System;
using System.Text.Json.Serialization;

namespace project2_db_benchmark.postgres.Models
{
    public class Tip
    {
        [JsonPropertyName("text")]
        public string Text { get; set; }

        [JsonPropertyName("date")]
        public string Date { get; set; }

        [JsonPropertyName("compliment_count")]
        public int ComplimentCount { get; set; }

        [JsonPropertyName("business_id")]
        public string BusinessId { get; set; }

        [JsonPropertyName("user_id")]
        public string UserId { get; set; }
    }
}