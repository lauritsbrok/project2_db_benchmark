using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace project2_db_benchmark.mongodb.Models
{
    public class Tip
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        [BsonRepresentation(BsonType.String)]
        public string BusinessId { get; set; }

        public BusinessSnapshot BusinessSnapshot { get; set; }

        [BsonRepresentation(BsonType.ObjectId)]
        public string UserId { get; set; }

        [BsonDateTimeOptions]
        public DateTime Date { get; set; }

        public string Text { get; set; }

        public int Likes { get; set; }
    }
} 