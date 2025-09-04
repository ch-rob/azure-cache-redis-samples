using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using System;
using System.Text.Json;
using System.Threading;
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
            string redisHostNameFailover = configuration["RedisHostName-Failover"];
            string redisAccessKeyFailover = configuration["RedisAccessKey-Failover"];
            string authenticationType = configuration["AuthenticationType"] ?? "WORKLOAD_IDENTITY";
            
            Console.WriteLine($"Primary Redis cache: {redisHostName}");
            Console.WriteLine($"Failover Redis cache: {redisHostNameFailover}");
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
                    if (string.IsNullOrEmpty(redisAccessKeyFailover))
                    {
                        Console.Error.WriteLine("ACCESS_KEY authentication selected but no failover access key provided in configuration.");
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

            // Circuit breaker pattern: Try primary first, then failover
            _redisConnection = await TryConnectWithCircuitBreaker(
                redisHostName, redisAccessKey, 
                redisHostNameFailover, redisAccessKeyFailover, 
                authenticationType);

            try
            {
                // Perform cache operations using the cache object...
                Console.WriteLine("Running... Press any key to quit.");

                Task thread1 = Task.Run(() => RunRedisCommandsAsync("Thread 1"));
                Task.WaitAll(thread1);
            }
            finally
            {
                _redisConnection.Dispose();
            }
        }

        private static async Task<RedisConnection> TryConnectWithCircuitBreaker(
            string primaryHost, string primaryKey,
            string failoverHost, string failoverKey,
            string authenticationType)
        {
            const int connectionTimeoutMs = 5000; // 5 second timeout
            
            // Try primary Redis instance first
            Console.WriteLine($"🔄 Attempting to connect to primary Redis: {primaryHost}");
            try
            {
                var primaryConnection = await Task.Run(async () =>
                {
                    var cts = new CancellationTokenSource(connectionTimeoutMs);
                    try
                    {
                        return await RedisConnection.InitializeAsync(primaryHost, primaryKey, authenticationType);
                    }
                    catch (OperationCanceledException)
                    {
                        throw new TimeoutException($"Connection to primary Redis {primaryHost} timed out after {connectionTimeoutMs}ms");
                    }
                });

                // Test the connection with a simple ping
                await primaryConnection.BasicRetryAsync(async (db) => await db.ExecuteAsync("PING"));
                
                Console.WriteLine($"✅ Successfully connected to primary Redis: {primaryHost}");
                return primaryConnection;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Failed to connect to primary Redis {primaryHost}: {ex.Message}");
                
                // Circuit breaker: Try failover Redis instance
                if (!string.IsNullOrEmpty(failoverHost))
                {
                    Console.WriteLine($"🔄 Circuit breaker activated - attempting failover to: {failoverHost}");
                    try
                    {
                        var failoverConnection = await Task.Run(async () =>
                        {
                            var cts = new CancellationTokenSource(connectionTimeoutMs);
                            try
                            {
                                return await RedisConnection.InitializeAsync(failoverHost, failoverKey, authenticationType);
                            }
                            catch (OperationCanceledException)
                            {
                                throw new TimeoutException($"Connection to failover Redis {failoverHost} timed out after {connectionTimeoutMs}ms");
                            }
                        });

                        // Test the failover connection
                        await failoverConnection.BasicRetryAsync(async (db) => await db.ExecuteAsync("PING"));
                        
                        Console.WriteLine($"✅ Successfully connected to failover Redis: {failoverHost}");
                        return failoverConnection;
                    }
                    catch (Exception failoverEx)
                    {
                        Console.WriteLine($"❌ Failed to connect to failover Redis {failoverHost}: {failoverEx.Message}");
                        throw new InvalidOperationException($"Both primary ({primaryHost}) and failover ({failoverHost}) Redis instances are unavailable. Primary error: {ex.Message}, Failover error: {failoverEx.Message}");
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Primary Redis {primaryHost} is unavailable and no failover configured. Error: {ex.Message}");
                }
            }
        }

        private static async Task RunRedisCommandsAsync(string prefix)
        {

            // Simple PING command
            Console.WriteLine($"{Environment.NewLine}{prefix}: Cache command: PING");
            RedisResult pingResult = await _redisConnection.BasicRetryAsync(async (db) => await db.ExecuteAsync("PING"));
            Console.WriteLine($"{prefix}: Cache response: {pingResult}");

            // Simple get and put of integral data types into the cache
            string key = "DateBasedMessage";
            string value = $"{DateTime.UtcNow.ToString("o")}: Message from {prefix}";

            // // Clear the key from the database first
            // if (prefix == "Thread 1")
            // {
            //     Console.WriteLine($"{Environment.NewLine}{prefix}: Cache command: DEL {key} via KeyDeleteAsync()");
            //     bool deleteResult = await _redisConnection.BasicRetryAsync(async (db) => await db.KeyDeleteAsync(key));
            //     Console.WriteLine($"{prefix}: Cache DEL response: {deleteResult}");
            // }
            // else
            // {
            //     Console.WriteLine($"{Environment.NewLine}{prefix}: Taking a quick nap");
            //     await Task.Delay(100); 
            // }

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