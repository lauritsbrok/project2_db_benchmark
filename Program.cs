﻿using MongoDB.Bson;
using MongoDB.Driver;
using DotNetEnv;

var username = Environment.GetEnvironmentVariable("MONGO_INITDB_ROOT_USERNAME");
var password = Environment.GetEnvironmentVariable("MONGO_INITDB_ROOT_PASSWORD");

var client = new MongoClient("mongodb://localhost:27017");
var database = client.GetDatabase("foo");
var collection = database.GetCollection<BsonDocument>("bar");

await collection.InsertOneAsync(new BsonDocument("Name", "Jack"));

var list = await collection.Find(new BsonDocument("Name", "Jack"))
    .ToListAsync();

foreach(var document in list)
{
    Console.WriteLine(document["Name"]);
}
