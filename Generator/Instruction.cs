namespace project2_db_benchmark.Generator
{
    public enum InstructionType
    {
        SearchForRestaurants,
        SubmitReviews
    }

    public class Instruction
    {
        public InstructionType Type { get; set; }
        public Dictionary<string, string> Parameters { get; set; } = [];
    }
}