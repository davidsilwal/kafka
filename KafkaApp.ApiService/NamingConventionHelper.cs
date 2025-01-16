namespace KafkaApp.ApiService;

public static class NamingConventionHelper
{
    public static string ToKebabCase(string input)
    {
        return string.Concat(input.Select((x, i) =>
            char.IsUpper(x) && i > 0 ? "-" + char.ToLower(x) : char.ToLower(x).ToString()));
    }

    public static string GetTopicName(Type messageType)
    {
        return ToKebabCase(messageType.Name);
    }
}