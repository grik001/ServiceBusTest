using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace ServiceBusTest
{
    class Program
    {
        private static ServiceBusHelper sbHelper = new ServiceBusHelper();
        private static CancellationTokenSource cancelSource = new CancellationTokenSource();
        private static ConcurrentDictionary<Guid, BrokeredMessage> messagesCompleted = new ConcurrentDictionary<Guid, BrokeredMessage>();

        private static string serviceBusConnectionString = "";
        private static string topic = "consoletest";
        private static string subscription = "default";

        private static int batchSize = 1000;
        private static int timeOutMilliSeconds = 20000;

        private static int totalMessagesToSend = 40000;
        private static int simultaneousSentTasks = 50;

        static void Main(string[] args)
        {
            if (sbHelper.TopicExistsAsync(serviceBusConnectionString, topic).Result == false)
            {
                TopicDescription topicCreated = sbHelper.CreateTopicAsync(serviceBusConnectionString, topic).Result;
                Console.WriteLine("Creating Topic");

                if (topicCreated == null)
                {
                    Console.WriteLine("Failed to create Topic");
                    return;
                }
            }

            if (sbHelper.SubscriptionExistsAsync(serviceBusConnectionString, topic, subscription).Result == false)
            {
                SubscriptionDescription subscriptionCreated = sbHelper.CreateSubscription(serviceBusConnectionString, topic, subscription).Result;
                Console.WriteLine("Creating Subscription");

                if (subscriptionCreated == null)
                {
                    Console.WriteLine("Failed to create Subscription");
                    return;
                }
            }

            Console.WriteLine("Enter 1 to run both Send & Receive - 2 for just Send - 3 for just Receive");
            var choice = "";
            choice = Console.ReadLine();

            if (choice == "1")
            {
                StartTest().Wait();
            }
            else if (choice == "2")
            {
                StartJustSend().Wait();
            }
            else if (choice == "3")
            {
                StartJustReceive().Wait();
            }

            //Calc results 
            if (messagesCompleted.Any())
            {
                var timeTakenForAllMessagesInSeconds = messagesCompleted.Select(x => (Convert.ToDateTime(x.Value.Properties["EndTime"]) - Convert.ToDateTime(x.Value.Properties["StartTime"])).TotalSeconds).ToList();
                var averageSeconds = timeTakenForAllMessagesInSeconds.Average();

                Console.WriteLine($"Average time to Send -> Receive -> Ack a message: {averageSeconds} seconds");
            }

            Console.ReadLine();
        }

        private static async Task StartTest()
        {
            //Trigger publish + subscribe and measure time
            Stopwatch totalTimeStopwatch = new Stopwatch();
            Console.WriteLine($"Starting Test at: {DateTime.Now}");
            totalTimeStopwatch.Start();

            var publishTest = StartPublishTest();
            var subscribeTest = StartSubscribeTest();
            await Task.WhenAll(publishTest, subscribeTest);

            totalTimeStopwatch.Stop();
            Console.WriteLine($"Test Complete on: {DateTime.Now} - TimeTaken: {totalTimeStopwatch.ElapsedMilliseconds}ms");
        }

        private static async Task StartJustSend()
        {
            //Trigger publish and measure time
            Stopwatch totalTimeStopwatch = new Stopwatch();
            Console.WriteLine($"Starting StartJustSend at: {DateTime.Now}");
            totalTimeStopwatch.Start();

            var sendTest = StartPublishTest();
            await Task.WhenAll(sendTest);

            totalTimeStopwatch.Stop();
            Console.WriteLine($"Test Complete on: {DateTime.Now} - TotalMessages: {messagesCompleted.Count} - TimeTaken: {totalTimeStopwatch.ElapsedMilliseconds}ms");
        }

        private static async Task StartJustReceive()
        {
            //Trigger receive and measure time
            Stopwatch totalTimeStopwatch = new Stopwatch();
            Console.WriteLine($"Starting StartJustReceive at: {DateTime.Now}");
            totalTimeStopwatch.Start();

            var subscribeTest = StartSubscribeTest();
            await Task.WhenAll(subscribeTest);

            totalTimeStopwatch.Stop();
            Console.WriteLine($"Test Complete on: {DateTime.Now} - TimeTaken: {totalTimeStopwatch.ElapsedMilliseconds}ms");
        }

        private static async Task StartSubscribeTest()
        {
            do
            {
                if (cancelSource.IsCancellationRequested)
                    break;

                try
                {
                    if(messagesCompleted.Count == totalMessagesToSend)
                        break;

                    Console.WriteLine("Triggering Receive");

                    Stopwatch receiverStopwatch = new Stopwatch();
                    receiverStopwatch.Start();
                    var messages = await sbHelper.ReceiveAsync(serviceBusConnectionString, topic, subscription, ReceiveMode.PeekLock, batchSize, timeOutMilliSeconds);
                    receiverStopwatch.Stop();
                    Console.WriteLine($"Receive took {receiverStopwatch.ElapsedMilliseconds}ms");

                    if (messages == null || !messages.Any())
                    {
                        Console.WriteLine("No Messages!");
                        continue;
                    }

                    Console.WriteLine($"ReceiveAsync from thread: {Thread.CurrentThread.ManagedThreadId} - returned {messages.Count()} messages");

                    List<Task> ackTasks = new List<Task>();

                    foreach (var message in messages)
                    {
                        ackTasks.Add(StartAcknowledge(message));
                    }

                    await Task.WhenAll(ackTasks);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }

            } while (true);
        }

        private static async Task StartAcknowledge(BrokeredMessage message)
        {
            try
            {
                Stopwatch ackStopwatch = new Stopwatch();
                ackStopwatch.Start();
                var lockToken = message.LockToken;
                await sbHelper.CompleteMessageAsync(serviceBusConnectionString, topic, subscription, lockToken);
                ackStopwatch.Stop();

                message.Properties.Add("EndTime", DateTime.UtcNow);

                if (message.Properties.ContainsKey("StartTime") && message.Properties.ContainsKey("EndTime"))
                    messagesCompleted.AddOrUpdate(lockToken, message, (key, oldValue) => message);

                Console.WriteLine($"StartAcknowledge from thread: {Thread.CurrentThread.ManagedThreadId} - completed {lockToken} - Timetaken: {ackStopwatch.ElapsedMilliseconds} ms");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private static async Task StartPublishTest()
        {
            Stopwatch publishStopwatch = new Stopwatch();
            publishStopwatch.Start();

            int countSent = 0;

            do
            {
                List<Task> sendMessageToServiceBusTasks = new List<Task>();

                for (int i = 0; i < simultaneousSentTasks; i++)
                {
                    sendMessageToServiceBusTasks.Add(SendMessageToServiceBus());
                    countSent++;
                }

                await Task.WhenAll(sendMessageToServiceBusTasks);
            } while (totalMessagesToSend > countSent);

            publishStopwatch.Stop();
            Console.WriteLine($"Publish took: {publishStopwatch.ElapsedMilliseconds} ms");
        }

        private static async Task SendMessageToServiceBus()
        {
            try
            {
                Guid randomDataGuid = Guid.NewGuid();

                BrokeredMessage serviceBusMessage = new BrokeredMessage(ToByteArray(randomDataGuid))
                {
                    MessageId = Guid.NewGuid().ToString(),
                    TimeToLive = DateTime.UtcNow.AddMinutes(15).TimeOfDay
                };

                serviceBusMessage.Properties.Add("StartTime", DateTime.UtcNow);

                await sbHelper.SendAsync(serviceBusConnectionString, topic, serviceBusMessage);

                Console.WriteLine($"Sending message from thread: {Thread.CurrentThread.ManagedThreadId}");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        //Generic Helpers 
        public static byte[] ToByteArray<T>(T obj)
        {
            if (obj == null)
                return null;

            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, obj);
                return ms.ToArray();
            }
        }
    }
}
