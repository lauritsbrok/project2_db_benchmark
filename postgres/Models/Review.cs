using System;
using System.Text.Json.Serialization;

namespace project2_db_benchmark.postgres.Models
{
    public class Review
    {
        [JsonPropertyName("review_id")]
        public string ReviewId { get; set; }

        [JsonPropertyName("user_id")]
        public string UserId { get; set; }

        [JsonPropertyName("business_id")]
        public string BusinessId { get; set; }

        [JsonPropertyName("stars")]
        public double Stars { get; set; }

        [JsonPropertyName("date")]
        public string Date { get; set; }

        [JsonPropertyName("text")]
        public string Text { get; set; }

        [JsonPropertyName("useful")]
        public int Useful { get; set; }

        [JsonPropertyName("funny")]
        public int Funny { get; set; }

        [JsonPropertyName("cool")]
        public int Cool { get; set; }
    }
}