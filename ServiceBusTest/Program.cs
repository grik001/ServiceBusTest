using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using static Microsoft.Azure.ServiceBus.Message;
using Messaging = Microsoft.ServiceBus.Messaging;
using ServiceBusMessaging = Microsoft.ServiceBus.Messaging;

namespace ServiceBusTest
{
    class Program
    {
        private static ServiceBusHelper sbHelper = new ServiceBusHelper();
        private static CancellationTokenSource cancelSource = new CancellationTokenSource();

        private static string serviceBusConnectionString = "";
        private static string topic = "consoletest";
        private static string subscription = "default";
        private static int batchSize = 10000;
        private static int timeOutMilliSeconds = 20000;

        private static int totalMessagesToSend = 20000;
        private static int simultaneousSentTasks = 50;

        private static MessageReceiver messageReceiver = new MessageReceiver(serviceBusConnectionString, EntityNameHelper.FormatSubscriptionPath(topic, subscription), ReceiveMode.PeekLock);
        //private static ServiceBusConnection ServiceBusConnection = messageReceiver.ServiceBusConnection;

        static void Main(string[] args)
        {
            if (sbHelper.TopicExistsAsync(serviceBusConnectionString, topic).Result == false)
            {
                Messaging.TopicDescription topicCreated = sbHelper.CreateTopicAsync(serviceBusConnectionString, topic).Result;
                Console.WriteLine("Creating Topic");

                if (topicCreated == null)
                {
                    Console.WriteLine("Failed to create Topic");
                    return;
                }
            }

            if (sbHelper.SubscriptionExistsAsync(serviceBusConnectionString, topic, subscription).Result == false)
            {
                Messaging.SubscriptionDescription subscriptionCreated = sbHelper.CreateSubscription(serviceBusConnectionString, topic, subscription).Result;
                Console.WriteLine("Creating Subscription");

                if (subscriptionCreated == null)
                {
                    Console.WriteLine("Failed to create Subscription");
                    return;
                }
            }

            StartTest().Wait();
            Console.ReadLine();
        }

        private static async Task StartTest()
        {
            //Trigger publish and measure time
            Stopwatch totalTimeStopwatch = new Stopwatch();
            Console.WriteLine($"Starting Test at: {DateTime.Now}");
            totalTimeStopwatch.Start();

            var publishTest = StartPublishLoadTest();
            var subscribeTest = StartSubscribeLoadTest();

            await Task.WhenAll(publishTest, subscribeTest);

            totalTimeStopwatch.Stop();
            Console.WriteLine($"Test Complete on: {DateTime.Now} - TimeTaken: {totalTimeStopwatch.ElapsedMilliseconds}ms");
        }

        private static async Task StartSubscribeLoadTest()
        {
            do
            {
                if (cancelSource.IsCancellationRequested)
                    break;

                try
                {
                    //Stopwatch setupMessangerWatch = new Stopwatch();
                    //setupMessangerWatch.Start();
                    ////Testing re use of connection instead of Singleton messageReceiver
                    //MessageReceiver messageReceiverSub = new MessageReceiver(ServiceBusConnection, "consoletest/Subscriptions/default", ReceiveMode.PeekLock);
                    //setupMessangerWatch.Stop();
                    //Console.WriteLine($"Connecting to subscribe took: {setupMessangerWatch.ElapsedMilliseconds} ms");

                    Console.WriteLine("Triggering Receive");

                    Stopwatch receiverStopwatch = new Stopwatch();
                    receiverStopwatch.Start();

                    var messages = await messageReceiver.ReceiveAsync(batchSize); //await sbHelper.ReceiveAsync(serviceBusConnectionString, topic, subscription, ReceiveMode.PeekLock, batchSize, timeOutMilliSeconds);

                    receiverStopwatch.Stop();
                    Console.WriteLine($"Receive took {receiverStopwatch.ElapsedMilliseconds}ms");


                    if (messages == null || !messages.Any())
                    {
                        Console.WriteLine("No Messages!");
                        continue;
                    }

                    Console.WriteLine($"ReceiveAsync from thread: {Thread.CurrentThread.ManagedThreadId} - returned {messages.Count} messages");

                    List<Task> ackTasks = new List<Task>();

                    foreach (var message in messages)
                    {
                        var lockToken = Guid.Parse(message.SystemProperties.LockToken);
                        ackTasks.Add(StartAcknowledge(lockToken));
                    }

                    await Task.WhenAll(ackTasks);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }

            } while (true);
        }

        private static async Task StartAcknowledge(Guid lockToken)
        {
            try
            {
                Stopwatch ackStopwatch = new Stopwatch();
                ackStopwatch.Start();

                await sbHelper.CompleteMessageAsync(serviceBusConnectionString, topic, subscription, lockToken);

                ackStopwatch.Stop();
                Console.WriteLine($"StartAcknowledge from thread: {Thread.CurrentThread.ManagedThreadId} - completed {lockToken} - Timetaken: {ackStopwatch.ElapsedMilliseconds} ms");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private static async Task StartPublishLoadTest()
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

            //do cancel token - TODO
        }

        private static async Task SendMessageToServiceBus()
        {

            try
            {
                Guid randomDataGuid = Guid.NewGuid();
                Message serviceBusMessage = new Message(ToByteArray(randomDataGuid))
                {
                    MessageId = Guid.NewGuid().ToString(),
                    TimeToLive = DateTime.UtcNow.AddMinutes(15).TimeOfDay
                };

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
