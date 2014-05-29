using System;
using System.Threading.Tasks;
using EasyNetQ.SystemMessages;

namespace EasyNetQ.ScheduledPublish
{
    public class ExternalScheduler : IExternalScheduler
    {
        private readonly IBus bus;
        private readonly ITypeNameSerializer typeNameSerializer;
        private readonly ISerializer serializer;

        public ExternalScheduler( IBus bus, ITypeNameSerializer typeNameSerializer, ISerializer serializer )
        {
            this.bus = bus;
            this.typeNameSerializer = typeNameSerializer;
            this.serializer = serializer;
        }

        public void Schedule<T>( T message, DateTime futurePublishDate ) where T : class
        {
            Schedule( message, futurePublishDate, null );
        }

        public void Schedule<T>( T message, DateTime futurePublishDate, string cancellationKey ) where T : class
        {
            Preconditions.CheckNotNull( message, "message" );

            var scheduleMe = CreateScheduleMessage( message, futurePublishDate, cancellationKey );
            bus.Publish(scheduleMe);
        }

        public void Unschedule( string cancellationKey)
        {
            var unscheduleMe = CreateUnscheduleMessage( cancellationKey );
            bus.Publish(unscheduleMe);
        }

        public Task ScheduleAsync<T>( T message, DateTime futurePublishDate ) where T : class
        {
            return ScheduleAsync( message, futurePublishDate, null );
        }

        public Task ScheduleAsync<T>( T message, DateTime futurePublishDate, string cancellationKey ) where T : class
        {
            Preconditions.CheckNotNull( message, "message" );

            var scheduleMe = CreateScheduleMessage( message, futurePublishDate, cancellationKey );
            return bus.PublishAsync( scheduleMe );
        }

        public Task UnscheduleAsync( string cancellationKey )
        {
            var unscheduleMe = CreateUnscheduleMessage(cancellationKey);
            return bus.PublishAsync(unscheduleMe);
        }

        private ScheduleMe CreateScheduleMessage<T>( T message, DateTime futurePublishDate, string cancellationKey ) where T : class
        {
            var typeName = typeNameSerializer.Serialize( typeof( T ) );
            var messageBody = serializer.MessageToBytes( message );
            var scheduleMe = new ScheduleMe
                {
                    WakeTime = futurePublishDate,
                    BindingKey = typeName,
                    CancellationKey = cancellationKey,
                    InnerMessage = messageBody
                };
            return scheduleMe;
        }

        private static UnscheduleMe CreateUnscheduleMessage( string cancellationKey )
        {
            var unscheduleMe = new UnscheduleMe
                {
                    CancellationKey = cancellationKey
                };
            return unscheduleMe;
        }
    }
}