using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace project2_db_benchmark.mongodb.Models
{
    public class Photo
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        [BsonRepresentation(BsonType.String)]
        public string BusinessId { get; set; }

        public string Label { get; set; }

        public string Caption { get; set; }

        public string Storage { get; set; } // gridfs://photos/<photo_id>.jpg or s3://bucket/...
    }
} 