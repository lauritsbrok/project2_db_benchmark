using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace project2_db_benchmark.Models.Shared
{
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