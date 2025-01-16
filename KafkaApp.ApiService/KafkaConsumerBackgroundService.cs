namespace KafkaApp.ApiService;

public class KafkaConsumerHostedService<TMessage> : IHostedService
{
    private readonly IKafkaConsumer _consumer;
    private readonly IKafkaMessageHandler<TMessage> _handler;
    private readonly string _topic;

    public KafkaConsumerHostedService(IKafkaConsumer consumer, IKafkaMessageHandler<TMessage> handler, string topic)
    {
        _consumer = consumer;
        _handler = handler;
        _topic = topic;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        Task.Run(
            () => _consumer.Consume<TMessage>(_topic,
                async message =>
                {
                    await _handler.HandleAsync(message.Value, cancellationToken);
                }, cancellationToken),
            cancellationToken);

        Console.WriteLine($"Started consuming topic: {_topic}");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine($"Stopped consuming topic: {_topic}");
        return Task.CompletedTask;
    }
}