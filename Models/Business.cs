using System.Text.Json;
using System.Text.Json.Serialization;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

#pragma warning disable CS8618


namespace project2_db_benchmark.Models
{
    [BsonIgnoreExtraElements]
    public class Business
    {
        [JsonPropertyName("business_id")]
        public string BusinessId { get; set; }

        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("address")]
        public string Address { get; set; }

        [JsonPropertyName("city")]
        public string City { get; set; }

        [JsonPropertyName("state")]
        public string State { get; set; }

        [JsonPropertyName("postal_code")]
        public string PostalCode { get; set; }

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

        // The JsonElement that comes from JSON deserialization
        [JsonPropertyName("attributes")]
        public JsonElement RawAttributes { get; set; }

        // MongoDB specific handling of attributes
        [BsonElement("attributes")]
        [BsonIgnoreIfNull]
        [JsonIgnore]
        public object Attributes
        {
            get
            {
                if (RawAttributes.ValueKind != JsonValueKind.Undefined && 
                    BsonDocument.TryParse(RawAttributes.GetRawText(), out var bsonDoc))
                {
                    return bsonDoc;
                }
                return BsonNull.Value;
            }
            set
            {
                if (value is BsonDocument bsonDoc)
                {
                    RawAttributes = JsonDocument.Parse(bsonDoc.ToJson()).RootElement;
                }
            }
        }

        // Postgres specific attribute handling
        [JsonIgnore]
        public Dictionary<string, object>? PostgresAttributes { get; set; }

        [JsonPropertyName("categories")]
        public string Categories { get; set; }

        [JsonPropertyName("hours")]
        public Dictionary<string, string> Hours { get; set; }
    }
}

#pragma warning restore CS8618 