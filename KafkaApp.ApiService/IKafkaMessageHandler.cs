namespace KafkaApp.ApiService;

public interface IKafkaMessageHandler<in TMessage>
{
    Task HandleAsync(TMessage message, CancellationToken cancellationToken = default);
}