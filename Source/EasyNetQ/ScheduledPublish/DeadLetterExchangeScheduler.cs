﻿using System;
using System.Threading.Tasks;
using EasyNetQ.Topology;

namespace EasyNetQ.ScheduledPublish
{
    public class DeadLetterExchangeScheduler : IDeadLetterExchangeScheduler
    {
        private readonly IConventions conventions;
        private readonly IConnectionConfiguration connectionConfiguration;
        private readonly IAdvancedBus advancedBus;

        public DeadLetterExchangeScheduler( IConventions conventions, IConnectionConfiguration connectionConfiguration, IAdvancedBus advancedBus )
        {
            this.conventions = conventions;
            this.connectionConfiguration = connectionConfiguration;
            this.advancedBus = advancedBus;
        }

        public void Schedule<T>( T message, TimeSpan messageDelay ) where T : class
        {
            Preconditions.CheckNotNull(message, "message");

            var delay = Round(messageDelay);
            var delayString = delay.ToString(@"hh\_mm\_ss");
            var exchangeName = conventions.ExchangeNamingConvention(typeof(T));
            var futureExchangeName = exchangeName + "_" + delayString;
            var futureQueueName = conventions.QueueNamingConvention(typeof(T), delayString);
            var futureExchange = advancedBus.ExchangeDeclare(futureExchangeName, ExchangeType.Topic);
            var futureQueue = advancedBus.QueueDeclare(futureQueueName, perQueueTtl: (int)delay.TotalMilliseconds, deadLetterExchange: exchangeName);
            advancedBus.Bind(futureExchange, futureQueue, "#");
            var easyNetQMessage = new Message<T>(message)
                {
                    Properties =
                        {
                            DeliveryMode = (byte)(connectionConfiguration.PersistentMessages ? 2 : 1)
                        }
                };

            advancedBus.Publish(futureExchange, "#", false, false, easyNetQMessage);
        }

        public Task ScheduleAsync<T>( T message, TimeSpan messageDelay ) where T : class
        {
            Preconditions.CheckNotNull(message, "message");

            var delay = Round(messageDelay);
            var delayString = delay.ToString(@"hh\_mm\_ss");
            var exchangeName = conventions.ExchangeNamingConvention(typeof(T));
            var futureExchangeName = exchangeName + "_" + delayString;
            var futureQueueName = conventions.QueueNamingConvention(typeof(T), delayString);
            var futureExchange = advancedBus.ExchangeDeclare(futureExchangeName, ExchangeType.Topic);
            var futureQueue = advancedBus.QueueDeclare(futureQueueName, perQueueTtl: (int)delay.TotalMilliseconds, deadLetterExchange: exchangeName);
            advancedBus.Bind(futureExchange, futureQueue, "#");
            var easyNetQMessage = new Message<T>(message)
                {
                    Properties =
                        {
                            DeliveryMode = (byte)(connectionConfiguration.PersistentMessages ? 2 : 1)
                        }
                };

            return advancedBus.PublishAsync(futureExchange, "#", false, false, easyNetQMessage);
        }

        private static TimeSpan Round(TimeSpan timeSpan)
        {
            return new TimeSpan(timeSpan.Days, timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds, 0);
        }
    }
}