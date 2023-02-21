using System;
using System.Threading;
using StackExchange.Redis;

namespace RedisTrial
{
    class Program
    {
        static void Main(string[] args)
        {
            //var db = redis.GetDatabase();
            //string value = "abcdefg";
            //db.StringSet("mykey", value);

            //value = db.StringGet("mykey");
            //Console.WriteLine(value); // writes: "abcdefg"

            var guid = Guid.NewGuid().ToString();
            var defaultCs = "localhost:8080";
            var prompt = "Type a message: ";

            Console.Write($"Insert connection string [{defaultCs}]: ");
            var cs = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(cs))
                cs = defaultCs;
            Console.WriteLine("Connecting...");
            var redis = ConnectionMultiplexer.Connect(cs);
            Console.WriteLine("Connected to " + cs);

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
