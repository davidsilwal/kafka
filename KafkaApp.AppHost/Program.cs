var builder = DistributedApplication.CreateBuilder(args);

var kafka = builder.AddKafka("kafka")
                   .WithKafkaUI(kafkaUI => kafkaUI.WithHostPort(9100))
                   .WithDataVolume(isReadOnly: false);


var apiService = builder.AddProject<Projects.KafkaApp_ApiService>("apiservice")
    .WithReference(kafka);

builder.AddProject<Projects.KafkaApp_Web>("webfrontend")
    .WithExternalHttpEndpoints()
    .WithReference(apiService)
    .WaitFor(apiService);

builder.Build().Run();
