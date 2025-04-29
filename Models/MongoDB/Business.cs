using System.Text.Json;
using System.Text.Json.Serialization;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using project2_db_benchmark.Models.Shared;

#pragma warning disable CS8618

namespace project2_db_benchmark.Models.MongoDB
{
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
        public double Latitude { get; set; }

        [JsonPropertyName("longitude")]
        public double Longitude { get; set; }

        [JsonPropertyName("stars")]
        public double Stars { get; set; }

        [JsonPropertyName("review_count")]
        public int ReviewCount { get; set; }

        [JsonPropertyName("is_open")]
        public int IsOpen { get; set; }

        [JsonPropertyName("attributes")]
        [BsonIgnore] // ignore this raw JsonElement in Mongo
        public JsonElement RawAttributes { get; set; }
        
        [BsonElement("attributes")]
        [BsonIgnoreIfNull]
        public object Attributes
        {
            get
            {
                if (BsonDocument.TryParse(RawAttributes.GetRawText(), out var bsonDoc))
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

        [JsonPropertyName("categories")]
        public string Categories { get; set; }

        [JsonPropertyName("hours")]
        public Dictionary<string, string> Hours { get; set; }
        [JsonIgnore]
        public IEnumerable<Photo> Photos { get; set; }
        public static IEnumerable<Business> AttachPhotosToBusinesses(IEnumerable<Business> businesses, IEnumerable<Photo> photos)
        {
            var photoLookup = photos
                .GroupBy(p => p.BusinessId)
                .ToDictionary(g => g.Key, g => g.ToList());

            foreach (var business in businesses)
            {
                photoLookup.TryGetValue(business.BusinessId, out var matchedPhotos);
                business.Photos = matchedPhotos ?? [];
            }

            return businesses;
        }
    }
}

#pragma warning restore CS8618