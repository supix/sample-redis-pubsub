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
            //var db = redis.GetDatabase();
            //string value = "abcdefg";
            //db.StringSet("mykey", value);

            //value = db.StringGet("mykey");
            //Console.WriteLine(value); // writes: "abcdefg"

            var redis = getConnectionMultiplexer();

            var db = redis.GetDatabase();
            var itemsToSave = 500000;
            var itemsToLoad = 1000;
            var entries = Enumerable
                .Range(0, itemsToSave)
                .Select(i => new KeyValuePair<RedisKey, RedisValue>(i.ToString(), Guid.NewGuid().ToString()))
                .ToArray();
            Console.WriteLine($"Saving {itemsToSave} elements...");
            var sw = new Stopwatch();
            sw.Start();
            db.StringSet(entries);
            sw.Stop();
            Console.WriteLine($"Saved in {sw.ElapsedMilliseconds} ms.");
            Thread.Sleep(3000);

            var keys = new List<RedisKey>();
            var rnd = new Random();
            while (keys.Count() < itemsToLoad)
            {
                var key = rnd.Next(itemsToSave).ToString();
                if (keys.IndexOf(key) < 0)
                    keys.Add(key);
            }
            Console.WriteLine($"Loading {itemsToLoad} elements...");
            sw.Reset();
            sw.Start();
            var values = db.StringGet(keys.ToArray());
            sw.Stop();
            Console.WriteLine($"Loaded in {sw.ElapsedMilliseconds} ms.");

            //for (int i = 0; i < keys.Count(); i++)
            //{
            //    var expectedValue = entries.Single(kv => kv.Key == keys[i]).Value;
            //    var valueGot = values[i];
                
            //    if (expectedValue != valueGot)
            //        Console.WriteLine($"Error! Expected: {expectedValue}. Got: {valueGot}");
            //    //else
            //    //    Console.WriteLine($"Ok! {i.ToString()}: {expectedValue}");
            //}

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
    }
}
