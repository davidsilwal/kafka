using Confluent.Kafka;

namespace KafkaApp.ApiService;

public class KafkaSettings
{
    public string BootstrapServers { get; set; }
    public string ConsumerGroupId { get; set; }
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Earliest;
    public bool EnableAutoCommit { get; set; } = true;
    public int RetryCount { get; set; } = 3;
    public int RetryDelayMs { get; set; } = 100;
}