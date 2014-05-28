using System;
using System.Threading.Tasks;
using EasyNetQ.Producer;
using EasyNetQ.Topology;

namespace EasyNetQ.ScheduledPublish
{
    public class DeadLetterExchangeScheduler : IDeadLetterExchangeScheduler
    {
        private readonly IConventions conventions;
        private readonly IConnectionConfiguration connectionConfiguration;
        private readonly IAdvancedBus advancedBus;
        private readonly IPublishExchangeDeclareStrategy publishExchangeDeclareStrategy;

        public DeadLetterExchangeScheduler( IConventions conventions, IConnectionConfiguration connectionConfiguration, IAdvancedBus advancedBus, IPublishExchangeDeclareStrategy publishExchangeDeclareStrategy )
        {
            this.conventions = conventions;
            this.connectionConfiguration = connectionConfiguration;
            this.advancedBus = advancedBus;
            this.publishExchangeDeclareStrategy = publishExchangeDeclareStrategy;
        }

        public void Schedule<T>( T message, TimeSpan messageDelay ) where T : class
        {
            Preconditions.CheckNotNull(message, "message");

            var futureExchange = PrepareSchedulingExchangeAndQueue( messageDelay, typeof( T ) );
            var easyNetQMessage = CreateMessage( message );

            advancedBus.Publish(futureExchange, "#", false, false, easyNetQMessage);
        }

        public Task ScheduleAsync<T>( T message, TimeSpan messageDelay ) where T : class
        {
            Preconditions.CheckNotNull(message, "message");

            var futureExchange = PrepareSchedulingExchangeAndQueue(messageDelay, typeof(T));
            var easyNetQMessage = CreateMessage(message);

            return advancedBus.PublishAsync(futureExchange, "#", false, false, easyNetQMessage);
        }

        private IExchange PrepareSchedulingExchangeAndQueue( TimeSpan messageDelay, Type messageType )
        {
            var delay = Round( messageDelay );
            var delayString = delay.ToString( @"hh\_mm\_ss" );
            var targetExchange = publishExchangeDeclareStrategy.DeclareExchange( advancedBus, messageType, ExchangeType.Topic );
            var exchangeName = targetExchange.Name;
            var futureExchangeName = exchangeName + "_" + delayString;
            var futureQueueName = conventions.QueueNamingConvention( messageType, delayString );
            var futureExchange = advancedBus.ExchangeDeclare( futureExchangeName, ExchangeType.Topic );
            var futureQueue = advancedBus.QueueDeclare( futureQueueName, perQueueTtl: (int) delay.TotalMilliseconds,
                                                        deadLetterExchange: exchangeName );
            advancedBus.Bind( futureExchange, futureQueue, "#" );
            return futureExchange;
        }

        private IMessage<T> CreateMessage<T>( T message ) where T : class
        {
            return new Message<T>( message )
                {
                    Properties =
                        {
                            DeliveryMode = (byte) ( connectionConfiguration.PersistentMessages ? 2 : 1 )
                        }
                };
        }

        private static TimeSpan Round(TimeSpan timeSpan)
        {
            return new TimeSpan(timeSpan.Days, timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds, 0);
        }
    }
}