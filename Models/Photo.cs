using System.Text.Json.Serialization;
using MongoDB.Bson.Serialization.Attributes;

#pragma warning disable CS8618

namespace project2_db_benchmark.Models
{
    [BsonIgnoreExtraElements]
    public class Photo
    {
        [JsonPropertyName("photo_id")]
        required public string PhotoId { get; set; }

        [JsonPropertyName("business_id")]
        required public string BusinessId { get; set; }

        [JsonPropertyName("caption")]
        required public string Caption { get; set; }

        [JsonPropertyName("label")]
        required public string Label { get; set; }
    }
}

#pragma warning restore CS8618