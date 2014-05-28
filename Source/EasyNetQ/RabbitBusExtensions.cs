using System;
using System.Threading.Tasks;
using EasyNetQ.ScheduledPublish;

namespace EasyNetQ
{
    public static class RabbitBusExtensions
    {
        /// <summary>
        /// Schedule a message to be published at some time in the future.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="bus">The IBus instance to publish on</param>
        /// <param name="messageDelay">The delay time for message to publish in future</param>
        /// <param name="message">The message to response with</param>
        public static void FuturePublish<T>(this IBus bus, TimeSpan messageDelay, T message) where T : class
        {
            Preconditions.CheckNotNull(message, "message");

            var scheduler = bus.Advanced.Container.Resolve<IDeadLetterExchangeScheduler>();
            scheduler.Schedule( message, messageDelay );
        }

        /// <summary>
        /// Schedule a message to be published at some time in the future.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="bus">The IBus instance to publish on</param>
        /// <param name="messageDelay">The delay time for message to publish in future</param>
        /// <param name="message">The message to response with</param>
        public static Task FuturePublishAsync<T>(this IBus bus, TimeSpan messageDelay, T message) where T : class
        {
            Preconditions.CheckNotNull(message, "message");

            var scheduler = bus.Advanced.Container.Resolve<IDeadLetterExchangeScheduler>();
            return scheduler.ScheduleAsync( message, messageDelay );
        }

    }
}