using System;
using System.Threading.Tasks;
using EasyNetQ.ScheduledPublish;

namespace EasyNetQ
{
    public static class BusExtensions
    {
        /// <summary>
        /// Schedule a message to be published at some time in the future.
        /// This required the EasyNetQ.Scheduler service to be running.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="bus">The IBus instance to publish on</param>
        /// <param name="futurePublishDate">The time at which the message should be sent (UTC)</param>
        /// <param name="message">The message to response with</param>
        public static void FuturePublish<T>(this IBus bus, DateTime futurePublishDate, T message) where T : class
        {
            var scheduler = bus.ResolveScheduler();
            scheduler.Schedule( message, futurePublishDate );
        }

        /// <summary>
        /// Schedule a message to be published at some time in the future.
        /// This required the EasyNetQ.Scheduler service to be running.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="bus">The IBus instance to publish on</param>
        /// <param name="futurePublishDate">The time at which the message should be sent (UTC)</param>
        /// <param name="cancellationKey">An identifier that can be used with CancelFuturePublish to cancel the sending of this message at a later time</param>
        /// <param name="message">The message to response with</param>
        public static void FuturePublish<T>(this IBus bus, DateTime futurePublishDate, string cancellationKey, T message) where T : class
        {
            var scheduler = bus.ResolveScheduler();
            scheduler.Schedule( message, futurePublishDate, cancellationKey );
        }

        /// <summary>
        /// Unschedule all messages matching the cancellationKey.
        /// </summary>
        /// <param name="bus">The IBus instance to publish on</param>
        /// <param name="cancellationKey">The identifier that was used when originally scheduling the message with FuturePublish</param>
        public static void CancelFuturePublish(this IBus bus, string cancellationKey)
        {
            var scheduler = bus.ResolveScheduler();
            scheduler.Unschedule( cancellationKey );
        }

        /// <summary>
        /// Schedule a message to be published at some time in the future.
        /// This required the EasyNetQ.Scheduler service to be running.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="bus">The IBus instance to publish on</param>
        /// <param name="futurePublishDate">The time at which the message should be sent (UTC)</param>
        /// <param name="message">The message to response with</param>
        public static Task FuturePublishAsync<T>(this IBus bus, DateTime futurePublishDate, T message) where T : class
        {
            var scheduler = bus.ResolveScheduler();
            return scheduler.ScheduleAsync( message, futurePublishDate );
        }

        /// <summary>
        /// Schedule a message to be published at some time in the future.
        /// This required the EasyNetQ.Scheduler service to be running.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="bus">The IBus instance to publish on</param>
        /// <param name="futurePublishDate">The time at which the message should be sent (UTC)</param>
        /// <param name="cancellationKey">An identifier that can be used with CancelFuturePublish to cancel the sending of this message at a later time</param>
        /// <param name="message">The message to response with</param>
        public static Task FuturePublishAsync<T>(this IBus bus, DateTime futurePublishDate, string cancellationKey, T message) where T : class
        {
            var scheduler = bus.ResolveScheduler();
            return scheduler.ScheduleAsync( message, futurePublishDate, cancellationKey );
        }

        /// <summary>
        /// Unschedule all messages matching the cancellationKey.
        /// </summary>
        /// <param name="bus">The IBus instance to publish on</param>
        /// <param name="cancellationKey">The identifier that was used when originally scheduling the message with FuturePublish</param>
        public static Task CancelFuturePublishAsync(this IBus bus, string cancellationKey)
        {
            var scheduler = bus.ResolveScheduler();
            return scheduler.UnscheduleAsync( cancellationKey );
        }

        private static IExternalScheduler ResolveScheduler( this IBus bus )
        {
            return bus.Advanced.Container.Resolve<IExternalScheduler>();
        }
    }
}