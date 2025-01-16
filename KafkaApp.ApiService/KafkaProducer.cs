using System.Text.Json;
using Confluent.Kafka;

namespace KafkaApp.ApiService;

public class KafkaProducer : IKafkaProducer
{
    private readonly IProducer<string, string> _producer;

    public KafkaProducer(IProducer<string, string> producer)
    {
        _producer = producer;
    }

    public async Task ProduceAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
    {
        // Get topic name dynamically based on TMessage class name
        var topic = NamingConventionHelper.GetTopicName(typeof(TMessage));

        // Serialize the message to JSON
        var serializedMessage = JsonSerializer.Serialize(message);

        var kafkaMessage = new Message<string, string>
        {
            Key = Guid.CreateVersion7().ToString(),
            Value = serializedMessage,
        };

        try
        {
            // Produce the message to the Kafka topic
            var result = await _producer.ProduceAsync(topic, kafkaMessage, cancellationToken);
            Console.WriteLine($"Message produced to {result.TopicPartitionOffset}");
        }
        catch (ProduceException<string, string> ex)
        {
            Console.WriteLine($"Error producing message: {ex.Error.Reason}");
            throw;
        }
    }
}