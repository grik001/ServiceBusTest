using Microsoft.Azure.ServiceBus;
using Microsoft.ServiceBus;
using System.Collections.Concurrent;
using Microsoft.Azure.ServiceBus.Core;
using ServiceBus = Microsoft.ServiceBus;
using ServiceBusMessaging = Microsoft.ServiceBus.Messaging;

namespace ServiceBusTest
{
    public static class ServiceBusConnectionsFactory
    {
        private static readonly ConcurrentDictionary<string, MessageSender> MessageSenders = new ConcurrentDictionary<string, MessageSender>();
        private static readonly ConcurrentDictionary<string, MessageReceiver> MessageReceivers = new ConcurrentDictionary<string, MessageReceiver>();
        private static readonly ConcurrentDictionary<string, ServiceBus.NamespaceManager> NamespaceManagers = new ConcurrentDictionary<string, ServiceBus.NamespaceManager>();
        private static readonly ConcurrentDictionary<string, ServiceBusMessaging.SubscriptionClient> SubscriptionClients = new ConcurrentDictionary<string, ServiceBusMessaging.SubscriptionClient>();

        public static MessageSender GetMessageSender(string connectionString, string topic)
        {
            string generatedKey = CryptoHelper.GETSHA512Hash($"MessageSender-{connectionString}-{topic}");

            if (!MessageSenders.TryGetValue(generatedKey, out MessageSender messageSender) || messageSender.IsClosedOrClosing)
            {
                messageSender = new MessageSender(connectionString, topic);
                MessageSenders.AddOrUpdate(generatedKey, messageSender, (key, oldValue) => messageSender);
            }

            return messageSender;
        }

        public static MessageReceiver GetMessageReceiver(string connectionString, string topic, string subscription, ReceiveMode receiveMode)
        {
            string generatedKey = CryptoHelper.GETSHA512Hash($"MessageReceiver-{connectionString}-{topic}-{subscription}-{receiveMode.ToString()}");

            if (!MessageReceivers.TryGetValue(generatedKey, out var messageReceiver) || messageReceiver.IsClosedOrClosing)
            {
                messageReceiver = new MessageReceiver(connectionString, EntityNameHelper.FormatSubscriptionPath(topic, subscription), receiveMode);
                MessageReceivers.AddOrUpdate(generatedKey, messageReceiver, (key, oldValue) => messageReceiver);
            }

            return messageReceiver;
        }

        public static ServiceBus.NamespaceManager GetNamespaceManager(string connectionString)
        {
            string generatedKey = CryptoHelper.GETSHA512Hash($"NamespaceManager-{connectionString}");

            if (!NamespaceManagers.TryGetValue(generatedKey, out NamespaceManager namespaceManager))
            {
                namespaceManager = ServiceBus.NamespaceManager.CreateFromConnectionString(connectionString);
                NamespaceManagers.AddOrUpdate(generatedKey, namespaceManager, (key, oldValue) => namespaceManager);
            }

            return namespaceManager;
        }

        public static ServiceBusMessaging.SubscriptionClient GetSubscriptionClient(string connectionString, string topic, string subscription)
        {
            string generatedKey = CryptoHelper.GETSHA512Hash($"SubscriptionClient-{connectionString}-{topic}-{subscription}");

            if (!SubscriptionClients.TryGetValue(generatedKey, out ServiceBusMessaging.SubscriptionClient subscriptionClient) || subscriptionClient.IsClosed)
            {
                subscriptionClient = ServiceBusMessaging.SubscriptionClient.CreateFromConnectionString(connectionString, topic, subscription, ServiceBusMessaging.ReceiveMode.PeekLock);
                SubscriptionClients.AddOrUpdate(generatedKey, subscriptionClient, (key, oldValue) => subscriptionClient);
            }

            return subscriptionClient;
        }
    }
}
