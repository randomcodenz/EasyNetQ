using System;
using System.Threading.Tasks;

namespace EasyNetQ.ScheduledPublish
{
    public interface IExternalScheduler
    {
        /// <summary>
        /// Schedule a message to be published at some time in the future.
        /// This required the EasyNetQ.Scheduler service to be running.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="message">The message to response with</param>
        /// <param name="futurePublishDate">The time at which the message should be sent (UTC)</param>
        void Schedule<T>( T message, DateTime futurePublishDate ) where T : class;

        /// <summary>
        /// Schedule a message to be published at some time in the future.
        /// This required the EasyNetQ.Scheduler service to be running.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="message">The message to response with</param>
        /// <param name="futurePublishDate">The time at which the message should be sent (UTC)</param>
        /// <param name="cancellationKey">An identifier that can be used with CancelFuturePublish to cancel the sending of this message at a later time</param>
        void Schedule<T>( T message, DateTime futurePublishDate, string cancellationKey ) where T : class;

        /// <summary>
        /// Unschedule all messages matching the cancellationKey.
        /// </summary>
        /// <param name="cancellationKey">The identifier that was used when originally scheduling the message with FuturePublish</param>
        void Unschedule( string cancellationKey );

        /// <summary>
        /// Schedule a message to be published at some time in the future.
        /// This required the EasyNetQ.Scheduler service to be running.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="message">The message to response with</param>
        /// <param name="futurePublishDate">The time at which the message should be sent (UTC)</param>
        Task ScheduleAsync<T>( T message, DateTime futurePublishDate ) where T : class;

        /// <summary>
        /// Schedule a message to be published at some time in the future.
        /// This required the EasyNetQ.Scheduler service to be running.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="message">The message to response with</param>
        /// <param name="futurePublishDate">The time at which the message should be sent (UTC)</param>
        /// <param name="cancellationKey">An identifier that can be used with CancelFuturePublish to cancel the sending of this message at a later time</param>
        Task ScheduleAsync<T>( T message, DateTime futurePublishDate, string cancellationKey ) where T : class;

        /// <summary>
        /// Unschedule all messages matching the cancellationKey.
        /// </summary>
        /// <param name="cancellationKey">The identifier that was used when originally scheduling the message with FuturePublish</param>
        Task UnscheduleAsync( string cancellationKey );
    }
}