using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using StackExchange.Redis;

namespace RedisTrial
{
    class Program
    {
        static void Main(string[] args)

        {
            // runChat();
            runLoadTest();
        }

        static void runLoadTest()
        {
            var redis = getConnectionMultiplexer();

            var db = redis.GetDatabase();
            var defaultItemsToSave = 100000;
            var defaultBatchSize = 10000;
            var defaultItemsToLoad = 100;
            var defaultValueStringLength = 100;

            Console.Write($"Items to save [{defaultItemsToSave}]: ");
            var input = Console.ReadLine();
            var itemsToSave = string.IsNullOrWhiteSpace(input) ? defaultItemsToSave : int.Parse(input);
            Console.Write($"Batch size [{defaultBatchSize}]: ");
            input = Console.ReadLine();
            var batchSize = string.IsNullOrWhiteSpace(input) ? defaultBatchSize : int.Parse(input);
            Console.Write($"Items to load [{defaultItemsToLoad}]: ");
            input = Console.ReadLine();
            var itemsToLoad = string.IsNullOrWhiteSpace(input) ? defaultItemsToLoad : int.Parse(input);
            if (itemsToLoad > itemsToSave)
                throw new InvalidOperationException("items to load must be less than items to save");
            Console.Write($"Values string length [{defaultValueStringLength}]: ");
            input = Console.ReadLine();
            var valuesStringLength = string.IsNullOrWhiteSpace(input) ? defaultItemsToLoad : int.Parse(input);

            var rnd = new Random();
            var values = NextStrings(rnd, (valuesStringLength, valuesStringLength), itemsToSave).ToArray();
            var dict = new Dictionary<string, string>();
            int i = 0;
            foreach (var value in values)
                dict[(i++.ToString())] = value;
            var entries = dict.Select(kvp => new KeyValuePair<RedisKey, RedisValue>(kvp.Key, kvp.Value)).ToArray();
            Console.WriteLine($"Saving {itemsToSave} elements...");
            var sw = new Stopwatch();
            sw.Start();
            int batch = 0;
            var batchValues = entries.Skip(batch++ * batchSize).Take(batchSize).ToArray();
            while (batchValues.Length > 0)
            {
                db.StringSet(batchValues);
                Console.Write('.');
                batchValues = entries.Skip(batch++ * batchSize).Take(batchSize).ToArray();
            }            
            sw.Stop();
            Console.WriteLine();
            Console.WriteLine($"Saved in {sw.ElapsedMilliseconds} ms.");

            var keys = new List<RedisKey>();

            while (keys.Count() < itemsToLoad)
            {
                var key = rnd.Next(itemsToSave).ToString();
                if (keys.IndexOf(key) < 0)
                    keys.Add(key);
            }
            Console.WriteLine($"Loading {itemsToLoad} elements...");
            sw.Reset();
            sw.Start();
            var loadedValues = db.StringGet(keys.ToArray());
            sw.Stop();
            Console.WriteLine($"Loaded in {sw.ElapsedMilliseconds} ms.");

            for (i = 0; i < keys.Count(); i++)
            {
                var expectedValue = dict[keys[i]];
                var valueGot = loadedValues[i];

                if (expectedValue != valueGot)
                    Console.WriteLine($"Error! Expected: {expectedValue}. Got: {valueGot}");
                //else
                //    Console.WriteLine($"Ok! {i.ToString()}: {expectedValue}");
            }

            sw.Reset();
            sw.Start();
            db.KeyDelete(keys.ToArray());
            sw.Stop();
            Console.WriteLine($"Keys deleted in {sw.ElapsedMilliseconds} ms.");
        }

        private static ConnectionMultiplexer getConnectionMultiplexer()
        {
            var defaultCs = "localhost:8080";

            Console.Write($"Insert connection string [{defaultCs}]: ");
            var cs = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(cs))
                cs = defaultCs;

            Console.WriteLine("Connecting...");
            var redis = ConnectionMultiplexer.Connect(cs);
            Console.WriteLine("Connected to " + cs);
            return redis;
        }

        static void runChat()
        {
            var guid = Guid.NewGuid().ToString();
            var prompt = "Type a message: ";

            var redis = getConnectionMultiplexer();

            ISubscriber sub = redis.GetSubscriber();
            sub.Subscribe("messages", (channel, message) => {
                var msg = (string)message;
                var parts = msg.Split("|", 2);
                if (parts[0] != guid)
                    Console.Write(System.Environment.NewLine + "Received: " + parts[1] + System.Environment.NewLine + prompt);
            });

            while (true)
            {
                Console.Write(prompt);
                var s = guid + "|" + Console.ReadLine();
                sub.Publish("messages", s);
            }
        }

        private static IEnumerable<string> NextStrings(
            Random rnd,
            (int Min, int Max) length,
            int count)
        {
            const string allowedChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz#@$^*()";
            ISet<string> usedRandomStrings = new HashSet<string>();
            (int min, int max) = length;
            char[] chars = new char[max];
            int setLength = allowedChars.Length;

            while (count-- > 0)
            {
                int stringLength = rnd.Next(min, max + 1);

                for (int i = 0; i < stringLength; ++i)
                {
                    chars[i] = allowedChars[rnd.Next(setLength)];
                }

                string randomString = new string(chars, 0, stringLength);

                if (usedRandomStrings.Add(randomString))
                {
                    yield return randomString;
                }
                else
                {
                    count++;
                }
            }
        }
    }
}
