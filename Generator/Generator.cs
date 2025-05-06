namespace project2_db_benchmark.Generator;

using project2_db_benchmark.Helpers;
using project2_db_benchmark.Models;

public class Generator(int seed = 42)
{
    private List<Business> _businesses = [];
    private List<User> _users = [];
    private List<Review> _reviews = [];

    private readonly Random _random = new(seed); // Same seed = same instruction set
    private readonly List<Instruction> _instructionSet = [];

    private readonly List<string> _cities =
    [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego",
        "Dallas", "San Jose",
        "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte", "San Francisco", "Indianapolis", "Seattle",
        "Denver", "Washington",
        "Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City", "Portland", "Las Vegas", "Memphis", "Louisville",
        "Baltimore",
        "Milwaukee", "Albuquerque", "Tucson", "Fresno", "Mesa", "Sacramento", "Atlanta", "Kansas City",
        "Colorado Springs", "Miami",
        "Raleigh", "Omaha", "Long Beach", "Virginia Beach", "Oakland", "Minneapolis", "Tulsa", "Arlington",
        "New Orleans", "Wichita"
    ];


    private void GenerateInstructions(int count)
    {
        for (var i = 0; i < count; i++)
        {
            var type = (InstructionType)_random.Next(Enum.GetValues<InstructionType>().Length);
            var instruction = GenerateInstruction(type);
            _instructionSet.Add(instruction);
        }
    }

    private Instruction GenerateInstruction(InstructionType type)
    {
        return type switch
        {   
            InstructionType.SearchForBusinesses => new Instruction
            {
                Type = type,
                Parameters = new Dictionary<string, string>
                {
                    { "name_prefix", GetRandomBusinessNamePrefix() }
                }
            },
            
            InstructionType.SubmitReviews => new Instruction
            {
                Type = type,
                Parameters = new Dictionary<string, string>
                {
                    { "name_prefix", GetRandomBusinessNamePrefix() },
                    { "user_name", $"new_user_{_random.Next(10000)}" },
                    { "stars", (_random.Next(1, 6)).ToString() },
                    { "text", "This is a new test review for the benchmark." }
                }
            },

            _ => throw new NotImplementedException()
        };
    }

    private List<Instruction> GetInstructions() => _instructionSet;

    private async Task InitialiseAsync()
    {
        _businesses = (await Parser.Parse<Business>($"yelp_dataset/{Globals.BUSINESS_JSON_FILE_NAME}")).ToList();
        _users = (await Parser.Parse<User>($"yelp_dataset/{Globals.USER_JSON_FILE_NAME}")).ToList();
        _reviews = (await Parser.Parse<Review>($"yelp_dataset/{Globals.REVIEW_JSON_FILE_NAME}")).ToList();
    }

    private User GetRandomUser()
    {
        return _users.Count > 0
            ? _users[_random.Next(_users.Count)]
            : new User { UserId = "user_fallback" };
    }

    private string GetRandomUserId()
    {
        return GetRandomUser().UserId ?? "user_fallback";
    }

    private string GetRandomBusinessId()
    {
        return _businesses.Count > 0
            ? _businesses[_random.Next(_businesses.Count)].BusinessId
            : "business_fallback";
    }

    private string GetRandomCity()
    {
        return _cities[_random.Next(_cities.Count)];
    }

    private string GetRandomBusinessNamePrefix()
    {
        if (_businesses.Count == 0)
            return "Rest"; // Default prefix if no businesses

        // Get a random business and take first 4 chars of its name
        string businessName = _businesses[_random.Next(_businesses.Count)].Name ?? "Restaurant";
        return businessName.Length >= 4 ? businessName.Substring(0, 4) : businessName;
    }

    public async Task Generate(int count, string filePath = "instruction-set.json")
    {
        await InitialiseAsync(); // Load your sample data

        // Generate smaller set of instructions for testing
        GenerateInstructions(count);

        // Get the generated instructions
        var instructions = GetInstructions();

        // Save instructions to a test file
        await InstructionSerializer.SaveInstructionsAsync(instructions, filePath);
        Console.WriteLine($"{instructions.Count} instructions saved to {filePath}");
    }
}
