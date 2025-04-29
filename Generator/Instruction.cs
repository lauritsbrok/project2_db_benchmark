public enum InstructionType
{
    CreateUser,
    SearchBusiness,
    ViewBusiness,
    PostReview,
    PostTip,
    ViewUser,
    ViewPhotos,
    Checkin
}

public class Instruction
{
    public InstructionType Type { get; set; }
    public Dictionary<string, string> Parameters { get; set; } = [];
}
