using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using System;
using System.Text.Json;
using System.Threading.Tasks;

namespace Redistest
{
    class Employee
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }

        public Employee(string id, string name, int age)
        {
            Id = id;
            Name = name;
            Age = age;
        }
    }

    class Program
    {
        private static RedisConnection _redisConnection;

        static async Task Main(string[] args)
        {
            // Initialize
            var builder = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddUserSecrets<Program>();
            var configuration = builder.Build();
            
            string redisHostName = configuration["RedisHostName"];
            string redisAccessKey = configuration["RedisAccessKey"];
            string authenticationType = configuration["AuthenticationType"] ?? "WORKLOAD_IDENTITY";
            
            Console.WriteLine($"Using Redis cache at {redisHostName}");
            Console.WriteLine($"Authentication type: {authenticationType}");

            // Validate required parameters based on authentication type
            switch (authenticationType.ToUpperInvariant())
            {
                case "ACCESS_KEY":
                    if (string.IsNullOrEmpty(redisAccessKey))
                    {
                        Console.Error.WriteLine("ACCESS_KEY authentication selected but no access key provided in configuration.");
                        return;
                    }
                    break;
                case "WORKLOAD_IDENTITY":
                    // No additional validation needed for workload identity
                    break;
                default:
                    Console.Error.WriteLine($"Invalid AuthenticationType: {authenticationType}. Supported values: ACCESS_KEY, WORKLOAD_IDENTITY");
                    return;
            }

            _redisConnection = await RedisConnection.InitializeAsync(redisHostName, redisAccessKey, authenticationType);

            try
            {
                // Perform cache operations using the cache object...
                Console.WriteLine("Running... Press any key to quit.");

                // while (!Console.KeyAvailable)
                // {
                    Task thread1 = Task.Run(() => RunRedisCommandsAsync("Thread 1"));
                    Task thread2 = Task.Run(() => RunRedisCommandsAsync("Thread 2"));

                    Task.WaitAll(thread1, thread2);
                // }
            }
            finally
            {
                _redisConnection.Dispose();
            }
        }

        private static async Task RunRedisCommandsAsync(string prefix)
        {

            // Simple PING command
            Console.WriteLine($"{Environment.NewLine}{prefix}: Cache command: PING");
            RedisResult pingResult = await _redisConnection.BasicRetryAsync(async (db) => await db.ExecuteAsync("PING"));
            Console.WriteLine($"{prefix}: Cache response: {pingResult}");

            // Simple get and put of integral data types into the cache
            string key = "Message";
            string value = "Hello! The cache is working from a .NET Core console app!";

            // Clear the key from the database first
            if (prefix == "Thread 1")
            {
                Console.WriteLine($"{Environment.NewLine}{prefix}: Cache command: DEL {key} via KeyDeleteAsync()");
                bool deleteResult = await _redisConnection.BasicRetryAsync(async (db) => await db.KeyDeleteAsync(key));
                Console.WriteLine($"{prefix}: Cache DEL response: {deleteResult}");
            }
            else
            {
                Console.WriteLine($"{Environment.NewLine}{prefix}: Taking a quick nap");
                await Task.Delay(100); 
            }

            Console.WriteLine($"{Environment.NewLine}{prefix}: Cache command: GET {key} via StringGetAsync()");
            RedisValue getMessageResult = await _redisConnection.BasicRetryAsync(async (db) => await db.StringGetAsync(key));
            Console.WriteLine($"{prefix}: Cache GET response 1: {getMessageResult}");

            Console.WriteLine($"{Environment.NewLine}{prefix}: Cache command: SET {key} \"{value}\" via StringSetAsync()");
            bool stringSetResult = await _redisConnection.BasicRetryAsync(async (db) => await db.StringSetAsync(key, value));
            Console.WriteLine($"{prefix}: Cache SET response 2: {stringSetResult}");

            Console.WriteLine($"{Environment.NewLine}{prefix}: Cache command: GET {key} via StringGetAsync()");
            getMessageResult = await _redisConnection.BasicRetryAsync(async (db) => await db.StringGetAsync(key));
            Console.WriteLine($"{prefix}: Cache response 3: {getMessageResult}");

            // Store serialized object to cache
            Employee e007 = new Employee("007", "Davide Columbo", 100);
            stringSetResult = await _redisConnection.BasicRetryAsync(async (db) => await db.StringSetAsync("e007", JsonSerializer.Serialize(e007)));
            Console.WriteLine($"{Environment.NewLine}{prefix}: Cache response from storing serialized Employee object: {stringSetResult}");

            // Retrieve serialized object from cache
            getMessageResult = await _redisConnection.BasicRetryAsync(async (db) => await db.StringGetAsync("e007"));
            Employee e007FromCache = JsonSerializer.Deserialize<Employee>(getMessageResult.ToString());
            Console.WriteLine($"{prefix}: Deserialized Employee .NET object:{Environment.NewLine}");
            Console.WriteLine($"{prefix}: Employee.Name : {e007FromCache.Name}");
            Console.WriteLine($"{prefix}: Employee.Id   : {e007FromCache.Id}");
            Console.WriteLine($"{prefix}: Employee.Age  : {e007FromCache.Age}{Environment.NewLine}");
        }
    }
}