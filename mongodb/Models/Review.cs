using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;

namespace project2_db_benchmark.mongodb.Models
{
    public class Review
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        [BsonRepresentation(BsonType.String)]
        public string BusinessId { get; set; }

        public BusinessSnapshot BusinessSnapshot { get; set; }

        [BsonRepresentation(BsonType.ObjectId)]
        public string UserId { get; set; }

        public int Stars { get; set; }

        [BsonDateTimeOptions]
        public DateTime Date { get; set; }

        public string Text { get; set; }

        public Votes Votes { get; set; }
    }

    public class BusinessSnapshot
    {
        public string Name { get; set; }
        public string City { get; set; }
        public List<string> Categories { get; set; }
    }

    public class Votes
    {
        public int Useful { get; set; }
        public int Funny { get; set; }
        public int Cool { get; set; }
    }
} 