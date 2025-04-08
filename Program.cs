using MongoDB.Bson;
using MongoDB.Driver;
using DotNetEnv;

Env.Load();

var mongouser = Environment.GetEnvironmentVariable("MONGO_INITDB_ROOT_USERNAME");
var mongopass = Environment.GetEnvironmentVariable("MONGO_INITDB_ROOT_PASSWORD");
var port = 27017;

var client = new MongoClient($"mongodb://{mongouser}:{mongopass}@localhost:{port}");
var database = client.GetDatabase("admin");
var collection = database.GetCollection<BsonDocument>("system.users");

await collection.InsertOneAsync(new BsonDocument("Name", "Jack"));

var list = await collection.Find(new BsonDocument("Name", "Jack"))
    .ToListAsync();

foreach(var document in list)
{
    Console.WriteLine(document["Name"]);
}
