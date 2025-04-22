using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver.GeoJsonObjectModel;
using System.Collections.Generic;

namespace project2_db_benchmark.mongodb.Models
{
    public class Business
    {
        [BsonId]
        [BsonRepresentation(BsonType.String)]
        public string Id { get; set; }

        public string Name { get; set; }
        public Address Address { get; set; }
        
        // Updated GeoJSON location format
        [BsonElement("location")]
        public GeoJsonPoint<GeoJson2DGeographicCoordinates> Location { get; set; }
        
        public List<string> Categories { get; set; }
        public Dictionary<string, object> Attributes { get; set; }
        public Dictionary<string, string> Hours { get; set; }
        public bool IsOpen { get; set; }

        // Denormalized counters
        public double RatingAvg { get; set; }
        public int RatingCount { get; set; }
        public int CheckinCount { get; set; }
        public int PhotoCount { get; set; }
        public int TipCount { get; set; }

        // Embedded "glance" data
        public List<RecentReview> RecentReviews { get; set; }
        public List<HeroPhoto> HeroPhotos { get; set; }

        // Bookkeeping
        [BsonDateTimeOptions]
        public DateTime CreatedAt { get; set; }
        [BsonDateTimeOptions]
        public DateTime UpdatedAt { get; set; }
    }

    public class Address
    {
        public string Street { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string PostalCode { get; set; }
    }

    public class RecentReview
    {
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }
        public ReviewUser User { get; set; }
        public int Stars { get; set; }
        [BsonDateTimeOptions]
        public DateTime Date { get; set; }
        public string TextSnip { get; set; }
    }

    public class ReviewUser
    {
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }
        public string Name { get; set; }
        public int? EliteSince { get; set; }
    }

    public class HeroPhoto
    {
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }
        public string Label { get; set; }
        public string Caption { get; set; }
        public string Storage { get; set; }
    }
} 