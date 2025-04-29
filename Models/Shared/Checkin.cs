using System;
using System.Text.Json.Serialization;
using MongoDB.Bson.Serialization.Attributes;

#pragma warning disable CS8618

namespace project2_db_benchmark.Models.Shared
{
    [BsonIgnoreExtraElements]
    public class Checkin
    {
        [JsonPropertyName("business_id")]
        required public string BusinessId { get; set; }

        [JsonPropertyName("date")]
        required public string Date { get; set; }
    }
}

#pragma warning restore CS8618