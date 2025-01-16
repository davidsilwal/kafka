namespace KafkaApp.ApiService;

public interface IKafkaProducer
{
    Task ProduceAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default);
}