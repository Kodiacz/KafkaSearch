using KafkaSearch.API;
using KafkaSearch.API.BacgroundServices;
using KafkaSearch.Core.Options;
using Scalar.AspNetCore;
using System.Text.Json.Serialization;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services
	.AddControllers()
	.AddJsonOptions(options => 
	{
		options.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter());
	});

// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddHostedService<AppStartupService>();

builder.Services.AddKafkaSearchServices();

builder.Services.AddOptions<KafkaOptions>()
	.BindConfiguration("KafkaOptions")
	.Configure<IWebHostEnvironment>((opt, env) =>
	{
		opt.ClusterProfileDataPath = Path.Combine(
			env.ContentRootPath,
			opt.ClusterProfileDataPath);
	})
	.ValidateDataAnnotations()
	.ValidateOnStart();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
	app.MapOpenApi();
	app.MapScalarApiReference();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
