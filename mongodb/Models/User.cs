using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;

namespace project2_db_benchmark.mongodb.Models
{
    public class User
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        public string Name { get; set; }

        [BsonDateTimeOptions]
        public DateTime YelpingSince { get; set; }

        public UserStats Stats { get; set; }

        public List<int> EliteYears { get; set; }

        public Dictionary<string, int> Compliments { get; set; }

        // Friendships are huge -> soft-cap then spill to friendEdges
        [BsonRepresentation(BsonType.ObjectId)]
        public List<string> Friends { get; set; }

        [BsonDateTimeOptions]
        public DateTime LastActive { get; set; }

        public LastReview LastReview { get; set; }
    }

    public class UserStats
    {
        public int ReviewCount { get; set; }
        public int Useful { get; set; }
        public int Funny { get; set; }
        public int Cool { get; set; }
        public int Fans { get; set; }
    }

    public class LastReview
    {
        [BsonRepresentation(BsonType.String)]
        public string BusinessId { get; set; }
        
        [BsonDateTimeOptions]
        public DateTime Date { get; set; }
        
        public int Stars { get; set; }
        
        public string TextSnip { get; set; }
    }
} 