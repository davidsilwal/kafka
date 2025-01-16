using Confluent.Kafka;

namespace KafkaApp.ApiService;

public interface IKafkaConsumer
{
    void Consume<TMessage>(string topic, Func<ConsumeResult<string, TMessage>, Task> onMessage,
        CancellationToken cancellationToken = default);
}