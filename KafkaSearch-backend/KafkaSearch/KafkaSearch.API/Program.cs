using KafkaSearch.Core.Options;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddOptions<KafkaOptions>()
	.Configure<IWebHostEnvironment>((opt, env) =>
	{
		opt.ClusterProfileDataPath = Path.Combine(
			env.ContentRootPath,
			opt.ClusterProfileDataPath);
	})
	.BindConfiguration("KafkaOptions")
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
