using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace project2_db_benchmark.Models.Postgres
{
    public class Business
    {
        [JsonPropertyName("business_id")]
        required public string BusinessId { get; set; }

        [JsonPropertyName("name")]
        required public string Name { get; set; }

        [JsonPropertyName("address")]
        required public string Address { get; set; }

        [JsonPropertyName("city")]
        required public string City { get; set; }

        [JsonPropertyName("state")]
        required public string State { get; set; }

        [JsonPropertyName("postal_code")]
        required public string PostalCode { get; set; }

        [JsonPropertyName("latitude")]
        public double? Latitude { get; set; }

        [JsonPropertyName("longitude")]
        public double? Longitude { get; set; }

        [JsonPropertyName("stars")]
        public double? Stars { get; set; }

        [JsonPropertyName("review_count")]
        public int? ReviewCount { get; set; }

        [JsonPropertyName("is_open")]
        public int? IsOpen { get; set; }

        [JsonPropertyName("attributes")]
        public Dictionary<string, object>? Attributes { get; set; }

        [JsonPropertyName("categories")]
        public string? Categories { get; set; }

        [JsonPropertyName("hours")]
        public Dictionary<string, string>? Hours { get; set; }
    }
}