namespace KafkaApp.ApiService;

public class OrderCreated
{
    public string OrderId { get; set; }
    public DateTime CreatedAt { get; set; }
}