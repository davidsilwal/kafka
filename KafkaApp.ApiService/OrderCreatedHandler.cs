namespace KafkaApp.ApiService;

public class OrderCreatedHandler : IKafkaMessageHandler<OrderCreated>
{
    public async Task HandleAsync(OrderCreated message, CancellationToken cancellationToken = default)
    {
        throw new Exception("Wz");
        // Simulate processing the message
        // Console.WriteLine($"Processing OrderCreated: OrderId={message.OrderId}, CreatedAt={message.CreatedAt}");

        // Simulate async work
        await Task.Delay(100, cancellationToken);
    }
}