using KafkaSearch.API.BacgroundServices;
using KafkaSearch.API.Infrastructure;
using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Options;
using KafkaSearch.Core.Services;
using KafkaSearch.Core.Services.Interfaces;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi(); 
builder.Services.AddHostedService<AppStartupService>();

builder.Services.AddSingleton<IFileSystem, FileSystem>();
builder.Services.AddScoped<IClusterProfileService, ClusterProfileService>();

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
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
