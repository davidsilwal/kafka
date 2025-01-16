using KafkaApp.ApiService;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;

var builder = WebApplication.CreateBuilder(args);

// Add service defaults & Aspire client integrations.
builder.AddServiceDefaults();

// Add services to the container.
builder.Services.AddProblemDetails();

// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

builder.Services.AddSingleton<KafkaProducer<string, string>>(sp =>
    new KafkaProducer<string, string>(builder.Configuration.GetConnectionString("kafka")));

builder.Services.AddSingleton<KafkaConsumer<string, string>>(sp =>
    new KafkaConsumer<string, string>(builder.Configuration.GetConnectionString("kafka"), "group"));

builder.Services.AddHostedService<KafkaConsumerService>();

builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));


var app = builder.Build();

// Configure the HTTP request pipeline.
app.UseExceptionHandler();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

string[] summaries = ["Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"];

app.MapGet("/weatherforecast", async ([FromServices] KafkaProducer<string, string> producer) =>
{

    await producer.ProduceAsync("my-topic", "hello", "Hello There");


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
