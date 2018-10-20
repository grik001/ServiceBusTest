using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.ServiceBus;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ServiceBus = Microsoft.ServiceBus;
using ServiceBusMessaging = Microsoft.ServiceBus.Messaging;
using Messaging = Microsoft.ServiceBus.Messaging;
namespace ServiceBusTest
{
    public class ServiceBusHelper
    {
        public async Task<ServiceBusMessaging.TopicDescription> CreateTopicAsync(string connectionString, string topic)
        {
            NamespaceManager namespaceManager = ServiceBusConnectionsFactory.GetNamespaceManager(connectionString);

            Messaging.TopicDescription topicDescription = new Messaging.TopicDescription(topic)
            {
                RequiresDuplicateDetection = true,
                DuplicateDetectionHistoryTimeWindow = new TimeSpan(0, 60, 0)
            };

            return await namespaceManager.CreateTopicAsync(topicDescription);
        }

        public async Task<IList<Message>> ReceiveAsync(string connectionString, string topic, string subscription, ReceiveMode receiveMode, int batchSize, int timeoutMilliSeconds)
        {
            MessageReceiver messageReceiver = ServiceBusConnectionsFactory.GetMessageReceiver(connectionString, topic, subscription, receiveMode);
            return await messageReceiver.ReceiveAsync(batchSize, TimeSpan.FromMilliseconds(timeoutMilliSeconds));
        }

        public async Task SendAsync(string connectionString, string topic, Message serviceBusMessage)
        {
            MessageSender messageSender = ServiceBusConnectionsFactory.GetMessageSender(connectionString, topic);
            await messageSender.SendAsync(serviceBusMessage);
        }

        public async Task<bool> SubscriptionExistsAsync(string connectionString, string topic, string subscription)
        {
            NamespaceManager namespaceManager = ServiceBusConnectionsFactory.GetNamespaceManager(connectionString);
            return await namespaceManager.SubscriptionExistsAsync(topic, subscription);
        }

        public async Task<ServiceBusMessaging.SubscriptionDescription> CreateSubscription(string connectionString, string topic, string subscription)
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
            ServiceBusMessaging.SubscriptionClient subscribeClient = ServiceBusConnectionsFactory.GetSubscriptionClient(connectionString, topic, subscription);
            await subscribeClient.AbandonAsync(lockToken);
        }

        public async Task CompleteMessageAsync(string connectionString, string topic, string subscription, Guid lockToken)
        {
            ServiceBusMessaging.SubscriptionClient subscribeClient = ServiceBusConnectionsFactory.GetSubscriptionClient(connectionString, topic, subscription);
            await subscribeClient.CompleteAsync(lockToken);
        }
    }
}
