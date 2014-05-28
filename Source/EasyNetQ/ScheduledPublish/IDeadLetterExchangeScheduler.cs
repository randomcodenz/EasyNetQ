using System;
using System.Threading.Tasks;

namespace EasyNetQ.ScheduledPublish
{
    public interface IDeadLetterExchangeScheduler
    {
        /// <summary>
        /// Schedule a message to be published at some time in the future.
        /// </summary>
        /// <remarks>
        /// Uses queues with fixed TTL and dead letter exchange capabilities of RMQ to route the
        /// message to be scheduled to the correct exchange after the desired delay.
        /// </remarks>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="messageDelay">The delay time for message to publish in future</param>
        /// <param name="message">The message to response with</param>
        void Schedule<T>( T message, TimeSpan messageDelay ) where T : class;

        /// <summary>
        /// Schedule a message to be published at some time in the future.
        /// </summary>
        /// <remarks>
        /// Uses queues with fixed TTL and dead letter exchange capabilities of RMQ to route the
        /// message to be scheduled to the correct exchange after the desired delay.
        /// </remarks>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="messageDelay">The delay time for message to publish in future</param>
        /// <param name="message">The message to response with</param>
        /// <returns>A task representing the publish operation.</returns>
        Task ScheduleAsync<T>(T message, TimeSpan messageDelay) where T : class;
    }
}