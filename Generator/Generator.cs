namespace project2_db_benchmark.Generator;

using project2_db_benchmark.Generator.SampleModels;

public class Generator
{
    private List<BusinessSample> _businesses = [];
    private List<UserSample> _users = [];
    private List<ReviewSample> _reviews = [];

    private Random _random;
    private List<Instruction> _instructionSet = [];

    private List<string> _cities = new()
    {
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
        "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte", "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington",
        "Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City", "Portland", "Las Vegas", "Memphis", "Louisville", "Baltimore",
        "Milwaukee", "Albuquerque", "Tucson", "Fresno", "Mesa", "Sacramento", "Atlanta", "Kansas City", "Colorado Springs", "Miami",
        "Raleigh", "Omaha", "Long Beach", "Virginia Beach", "Oakland", "Minneapolis", "Tulsa", "Arlington", "New Orleans", "Wichita"
    };


    public Generator(int seed = 42)
    {
        _random = new Random(seed); // Same seed = same instruction set
    }

    private void GenerateInstructions(int count)
    {
        for (int i = 0; i < count; i++)
        {
            var type = (InstructionType)_random.Next(Enum.GetValues(typeof(InstructionType)).Length);
            var instruction = GenerateInstruction(type);
            _instructionSet.Add(instruction);
        }
    }

    private Instruction GenerateInstruction(InstructionType type)
    {
        return type switch
        {
            InstructionType.CreateUser => new Instruction
            {
                Type = type,
                Parameters = new Dictionary<string, string>
                {
                    { "name", $"user_{_random.Next(10000)}" }
                }
            },

            InstructionType.SearchBusiness => new Instruction
            {
                Type = type,
                Parameters = new Dictionary<string, string>
                {
                    { "category", GetRandomBusinessCategory() },
                    { "city", GetRandomCity() }
                }
            },

            InstructionType.PostReview => new Instruction
            {
                Type = type,
                Parameters = new Dictionary<string, string>
                {
                    { "user_id", GetRandomUserId() },
                    { "business_id", GetRandomBusinessId() },
                    { "stars", (_random.Next(1, 6)).ToString() },
                    { "text", "This is a test review." }
                }
            },

            // Add other instruction types here...

            _ => throw new NotImplementedException()
        };
    }

    public List<Instruction> GetInstructions() => _instructionSet;

    private async Task InitialiseAsync()
    {
        _businesses = await SampleLoader.LoadFromJsonFile<BusinessSample>("samples/businesses_sample.json");
        _users = await SampleLoader.LoadFromJsonFile<UserSample>("samples/users_sample.json");
        _reviews = await SampleLoader.LoadFromJsonFile<ReviewSample>("samples/reviews_sample.json");
    }

    private string GetRandomBusinessCategory()
    {
        return _businesses.Count > 0 
            ? _businesses[_random.Next(_businesses.Count)].Categories ?? "Restaurants" 
            : "Restaurants";
    }

    private UserSample GetRandomUser()
    {
        return _users.Count > 0 
            ? _users[_random.Next(_users.Count)] 
            : new UserSample { UserId = "user_fallback" };
    }

    private string GetRandomUserId()
    {
        return GetRandomUser()?.UserId ?? "user_fallback";
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

    public async void Generate(){
        var generator = new Generator(seed: 123); // Using a fixed seed for reproducibility
        await generator.InitialiseAsync(); // Load your sample data

        // Generate 10000 instructions
        generator.GenerateInstructions(10000);

        // Get the generated instructions
        var instructions = generator.GetInstructions();

        // Save instructions to a file (optional)
        await InstructionSerializer.SaveInstructionsAsync(instructions, "instruction-set.json");
    }
}
