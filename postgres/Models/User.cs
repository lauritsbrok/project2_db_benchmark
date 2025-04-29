using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace project2_db_benchmark.postgres.Models
{
    public class User
    {
        [JsonPropertyName("user_id")]
        public string UserId { get; set; }

        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("review_count")]
        public int ReviewCount { get; set; }

        [JsonPropertyName("yelping_since")]
        public string YelpingSince { get; set; }

        [JsonPropertyName("friends")]
        [JsonConverter(typeof(FriendsConverter))]
        public List<string> Friends { get; set; }

        [JsonPropertyName("useful")]
        public int Useful { get; set; }

        [JsonPropertyName("funny")]
        public int Funny { get; set; }

        [JsonPropertyName("cool")]
        public int Cool { get; set; }

        [JsonPropertyName("fans")]
        public int Fans { get; set; }

        [JsonPropertyName("elite")]
        public List<int> Elite { get; set; }

        [JsonPropertyName("average_stars")]
        public double AverageStars { get; set; }

        [JsonPropertyName("compliment_hot")]
        public int ComplimentHot { get; set; }

        [JsonPropertyName("compliment_more")]
        public int ComplimentMore { get; set; }

        [JsonPropertyName("compliment_profile")]
        public int ComplimentProfile { get; set; }

        [JsonPropertyName("compliment_cute")]
        public int ComplimentCute { get; set; }

        [JsonPropertyName("compliment_list")]
        public int ComplimentList { get; set; }

        [JsonPropertyName("compliment_note")]
        public int ComplimentNote { get; set; }

        [JsonPropertyName("compliment_plain")]
        public int ComplimentPlain { get; set; }

        [JsonPropertyName("compliment_cool")]
        public int ComplimentCool { get; set; }

        [JsonPropertyName("compliment_funny")]
        public int ComplimentFunny { get; set; }

        [JsonPropertyName("compliment_writer")]
        public int ComplimentWriter { get; set; }

        [JsonPropertyName("compliment_photos")]
        public int ComplimentPhotos { get; set; }
    }

    public class FriendsConverter : JsonConverter<List<string>>
    {
        public override List<string> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                string friendsString = reader.GetString();
                if (string.IsNullOrEmpty(friendsString))
                    return new List<string>();

                return friendsString.Split(',').Select(f => f.Trim()).ToList();
            }
            else if (reader.TokenType == JsonTokenType.StartArray)
            {
                return JsonSerializer.Deserialize<List<string>>(ref reader, options);
            }

            throw new JsonException("Unexpected token type for friends field");
        }

        public override void Write(Utf8JsonWriter writer, List<string> value, JsonSerializerOptions options)
        {
            if (value == null)
                writer.WriteNullValue();
            else
                JsonSerializer.Serialize(writer, value, options);
        }
    }
}