using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace project2_db_benchmark.Models.Shared
{
    public class User
    {
        [JsonPropertyName("user_id")]
        public string UserId { get; set; }

        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("review_count")]
        public int? ReviewCount { get; set; }

        [JsonPropertyName("yelping_since")]
        public string? YelpingSince { get; set; }

        [JsonPropertyName("friends")]
        public string? Friends { get; set; }

        [JsonPropertyName("useful")]
        public int? Useful { get; set; }

        [JsonPropertyName("funny")]
        public int? Funny { get; set; }

        [JsonPropertyName("cool")]
        public int? Cool { get; set; }

        [JsonPropertyName("fans")]
        public int? Fans { get; set; }

        [JsonPropertyName("elite")]
        public string? Elite { get; set; }

        [JsonPropertyName("average_stars")]
        public double? AverageStars { get; set; }

        [JsonPropertyName("compliment_hot")]
        public int? ComplimentHot { get; set; }

        [JsonPropertyName("compliment_more")]
        public int? ComplimentMore { get; set; }

        [JsonPropertyName("compliment_profile")]
        public int? ComplimentProfile { get; set; }

        [JsonPropertyName("compliment_cute")]
        public int? ComplimentCute { get; set; }

        [JsonPropertyName("compliment_list")]
        public int? ComplimentList { get; set; }

        [JsonPropertyName("compliment_note")]
        public int? ComplimentNote { get; set; }

        [JsonPropertyName("compliment_plain")]
        public int? ComplimentPlain { get; set; }

        [JsonPropertyName("compliment_cool")]
        public int? ComplimentCool { get; set; }

        [JsonPropertyName("compliment_funny")]
        public int? ComplimentFunny { get; set; }

        [JsonPropertyName("compliment_writer")]
        public int? ComplimentWriter { get; set; }

        [JsonPropertyName("compliment_photos")]
        public int? ComplimentPhotos { get; set; }
    }
}