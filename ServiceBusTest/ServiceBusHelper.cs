using System;
using System.Collections.Generic;
using System.ServiceModel.Channels;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace ServiceBusTest
{
    public class ServiceBusHelper
    {
        public async Task<TopicDescription> CreateTopicAsync(string connectionString, string topic)
        {
            NamespaceManager namespaceManager = ServiceBusConnectionsFactory.GetNamespaceManager(connectionString);

            TopicDescription topicDescription = new TopicDescription(topic)
            {
                RequiresDuplicateDetection = true,
                DuplicateDetectionHistoryTimeWindow = new TimeSpan(0, 60, 0)
            };

            return await namespaceManager.CreateTopicAsync(topicDescription);
        }

        public async Task<IEnumerable<BrokeredMessage>> ReceiveAsync(string connectionString, string topic, string subscription, ReceiveMode receiveMode, int batchSize, int timeoutMilliSeconds)
        {
            SubscriptionClient subscribeClient = ServiceBusConnectionsFactory.GetSubscriptionClient(connectionString, topic, subscription);
            return await subscribeClient.ReceiveBatchAsync(1000, new TimeSpan(0,0,0,0, timeoutMilliSeconds));
        }

        public async Task SendAsync(string connectionString, string topic, BrokeredMessage serviceBusMessage)
        {
            TopicClient topicClient = ServiceBusConnectionsFactory.GetTopicClient(connectionString, topic);
            await topicClient.SendAsync(serviceBusMessage);
        }

        public async Task<bool> SubscriptionExistsAsync(string connectionString, string topic, string subscription)
        {
            NamespaceManager namespaceManager = ServiceBusConnectionsFactory.GetNamespaceManager(connectionString);
            return await namespaceManager.SubscriptionExistsAsync(topic, subscription);
        }

        public async Task<SubscriptionDescription> CreateSubscription(string connectionString, string topic, string subscription)
        {
            NamespaceManager namespaceManager = ServiceBusConnectionsFactory.GetNamespaceManager(connectionString);
            return await namespaceManager.CreateSubscriptionAsync(topic, subscription);
        }

        public async Task<bool> TopicExistsAsync(string connectionString, string topic)
        {
            NamespaceManager namespaceManager = ServiceBusConnectionsFactory.GetNamespaceManager(connectionString);
            return await namespaceManager.TopicExistsAsync(topic);
        }

        public async Task AbandonMessageAsync(string connectionString, string topic, string subscription, Guid lockToken)
        {
            SubscriptionClient subscribeClient = ServiceBusConnectionsFactory.GetAckClient(connectionString, topic, subscription);
            await subscribeClient.AbandonAsync(lockToken);
        }

        public async Task CompleteMessageAsync(string connectionString, string topic, string subscription, Guid lockToken)
        {
            SubscriptionClient subscribeClient = ServiceBusConnectionsFactory.GetAckClient(connectionString, topic, subscription);
            await subscribeClient.CompleteAsync(lockToken);
        }
    }
}
