using System.Reflection;
using Confluent.Kafka;
using KafkaApp.ApiService;

var builder = WebApplication.CreateBuilder(args);

// Add service defaults & Aspire client integrations.
builder.AddServiceDefaults();

// Add services to the container.
builder.Services.AddProblemDetails();

// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

builder.Services.AddTransient<IKafkaConsumer, KafkaConsumer>();

var kafka = builder.Configuration.GetConnectionString("kafka");

var handlerTypes = Assembly.GetExecutingAssembly()
    .GetTypes()
    .Where(type => type.GetInterfaces()
        .Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IKafkaMessageHandler<>)));

foreach (var handlerType in handlerTypes)
{
    var messageType = handlerType.GetInterfaces()
        .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IKafkaMessageHandler<>))
        .GetGenericArguments()[0];

    builder.Services.AddTransient(handlerType);

    var hostedServiceType = typeof(KafkaConsumerHostedService<>).MakeGenericType(messageType);

    builder.Services.AddTransient<IHostedService>(
        provider =>
        {
            var consumer = provider.GetRequiredService<IKafkaConsumer>();
            var handler = provider.GetRequiredService(handlerType);

            // Use dynamic topic name
            var topicName = NamingConventionHelper.GetTopicName(messageType);

            return (IHostedService)Activator.CreateInstance(hostedServiceType, consumer, handler, topicName);
        }
    );
}

builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));

builder.Services.AddScoped<IKafkaProducer, KafkaProducer>();

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = kafka, // Replace with your Kafka broker
    GroupId = "my-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest, // Start from the beginning if no offset is found
    EnableAutoCommit = true, // Automatically commit offsets
    AllowAutoCreateTopics = true // Allow Kafka to auto-create topics
};

// Register the Kafka Consumer instance in DI
builder.Services.AddSingleton<IConsumer<string, string>>(provider =>
{
    return new ConsumerBuilder<string, string>(consumerConfig)
        .SetKeyDeserializer(Deserializers.Utf8)
        .SetValueDeserializer(Deserializers.Utf8)
        .Build();
});


var producerConfig = new ProducerConfig
{
    AllowAutoCreateTopics = true,
    BootstrapServers = kafka, // Replace with your Kafka broker
    Acks = Acks.All, // Ensure all replicas acknowledge the message
    EnableIdempotence = true // Ensure exactly-once delivery
};

// Register the Kafka Producer instance in DI
builder.Services.AddSingleton<IProducer<string, string>>(provider =>
{
    return new ProducerBuilder<string, string>(producerConfig)
        .SetKeySerializer(Serializers.Utf8)
        .SetValueSerializer(Serializers.Utf8)
        .Build();
});


var app = builder.Build();

// Configure the HTTP request pipeline.
app.UseExceptionHandler();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

string[] summaries =
    ["Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"];

app.MapGet("/weatherforecast", async (IKafkaProducer producer) =>
    {
        await producer.ProduceAsync(new OrderCreated
        {
            OrderId = Guid.NewGuid().ToString(),
            CreatedAt = DateTime.Now
        });

        var forecast = Enumerable.Range(1, 5).Select(index =>
                new WeatherForecast
                (
                    DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
                    Random.Shared.Next(-20, 55),
                    summaries[Random.Shared.Next(summaries.Length)]
                ))
            .ToArray();
        return forecast;
    })
    .WithName("GetWeatherForecast");

app.MapDefaultEndpoints();

app.Run();

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}