using System;
using System.Text.Json.Serialization;

namespace project2_db_benchmark.Models.Shared
{
    public class Tip
    {
        [JsonPropertyName("text")]
        required public string Text { get; set; }

        [JsonPropertyName("date")]
        required public string Date { get; set; }

        [JsonPropertyName("compliment_count")]
        required public int ComplimentCount { get; set; }

        [JsonPropertyName("business_id")]
        required public string BusinessId { get; set; }

        [JsonPropertyName("user_id")]
        required public string UserId { get; set; }
    }
}