using Microsoft.ServiceBus;
using System.Collections.Concurrent;
using Microsoft.ServiceBus.Messaging;

namespace ServiceBusTest
{
    public static class ServiceBusConnectionsFactory
    {
        private static readonly ConcurrentDictionary<string, TopicClient> TopicClients = new ConcurrentDictionary<string, TopicClient>();
        private static readonly ConcurrentDictionary<string, NamespaceManager> NamespaceManagers = new ConcurrentDictionary<string, NamespaceManager>();
        private static readonly ConcurrentDictionary<string, SubscriptionClient> SubscriptionClients = new ConcurrentDictionary<string, SubscriptionClient>();

        public static TopicClient GetTopicClient(string connectionString, string topic)
        {
            string generatedKey = CryptoHelper.GETSHA512Hash($"MessageSender-{connectionString}-{topic}");

            if (!TopicClients.TryGetValue(generatedKey, out TopicClient topicClient) || topicClient.IsClosed)
            {
                topicClient = TopicClient.CreateFromConnectionString(connectionString, topic);
                TopicClients.AddOrUpdate(generatedKey, topicClient, (key, oldValue) => topicClient);
            }

            return topicClient;
        }

        public static NamespaceManager GetNamespaceManager(string connectionString)
        {
            string generatedKey = CryptoHelper.GETSHA512Hash($"NamespaceManager-{connectionString}");

            if (!NamespaceManagers.TryGetValue(generatedKey, out NamespaceManager namespaceManager))
            {
                namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
                NamespaceManagers.AddOrUpdate(generatedKey, namespaceManager, (key, oldValue) => namespaceManager);
            }

            return namespaceManager;
        }

        public static SubscriptionClient GetSubscriptionClient(string connectionString, string topic, string subscription)
        {
            string generatedKey = CryptoHelper.GETSHA512Hash($"SubscriptionClient-{connectionString}-{topic}-{subscription}");

            if (!SubscriptionClients.TryGetValue(generatedKey, out SubscriptionClient subscriptionClient) || subscriptionClient.IsClosed)
            {
                subscriptionClient = SubscriptionClient.CreateFromConnectionString(connectionString, topic, subscription, ReceiveMode.PeekLock);
                subscriptionClient.PrefetchCount = 1000;
                SubscriptionClients.AddOrUpdate(generatedKey, subscriptionClient, (key, oldValue) => subscriptionClient);
            }

            return subscriptionClient;
        }

        public static SubscriptionClient GetAckClient(string connectionString, string topic, string subscription)
        {
            string generatedKey = CryptoHelper.GETSHA512Hash($"AckClient-{connectionString}-{topic}-{subscription}");

            if (!SubscriptionClients.TryGetValue(generatedKey, out SubscriptionClient subscriptionClient) || subscriptionClient.IsClosed)
            {
                subscriptionClient = SubscriptionClient.CreateFromConnectionString(connectionString, topic, subscription, ReceiveMode.PeekLock);
                subscriptionClient.PrefetchCount = 1000;
                SubscriptionClients.AddOrUpdate(generatedKey, subscriptionClient, (key, oldValue) => subscriptionClient);
            }

            return subscriptionClient;
        }
    }
}
