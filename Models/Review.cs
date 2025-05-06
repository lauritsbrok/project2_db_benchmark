using System.Text.Json.Serialization;
using MongoDB.Bson.Serialization.Attributes;

#pragma warning disable CS8618

namespace project2_db_benchmark.Models
{
    [BsonIgnoreExtraElements]
    public class Review
    {
        [JsonPropertyName("review_id")]
        required public string ReviewId { get; set; }

        [JsonPropertyName("user_id")]
        required public string UserId { get; set; }

        [JsonPropertyName("business_id")]
        required public string BusinessId { get; set; }

        [JsonPropertyName("stars")]
        required public double Stars { get; set; }

        [JsonPropertyName("date")]
        required public string Date { get; set; }

        [JsonPropertyName("text")]
        public string? Text { get; set; }

        [JsonPropertyName("useful")]
        public int? Useful { get; set; }

        [JsonPropertyName("funny")]
        public int? Funny { get; set; }

        [JsonPropertyName("cool")]
        public int? Cool { get; set; }
    }
}

#pragma warning restore CS8618