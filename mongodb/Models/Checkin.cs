using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace project2_db_benchmark.mongodb.Models
{
    public class Checkin
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        // Time-series meta-field
        [BsonRepresentation(BsonType.String)]
        public string BusinessId { get; set; }

        // Time-series fields
        [BsonDateTimeOptions]
        public DateTime Ts { get; set; }

        public int Count { get; set; } // Always 1; rollups auto-total per hour/day
    }
} 