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
            Preconditions.CheckNotNull(message, "message");

            var typeName = typeNameSerializer.Serialize(typeof(T));
            var messageBody = serializer.MessageToBytes(message);

            bus.Publish(new ScheduleMe
                {
                    WakeTime = futurePublishDate,
                    BindingKey = typeName,
                    CancellationKey = cancellationKey,
                    InnerMessage = messageBody
                });
        }

        public void Unschedule( string cancellationKey)
        {
            bus.Publish(new UnscheduleMe
                {
                    CancellationKey = cancellationKey
                });
        }

        public Task ScheduleAsync<T>( T message, DateTime futurePublishDate ) where T : class
        {
            return ScheduleAsync( message, futurePublishDate, null );
        }

        public Task ScheduleAsync<T>( T message, DateTime futurePublishDate, string cancellationKey ) where T : class
        {
            Preconditions.CheckNotNull(message, "message");

            var typeName = typeNameSerializer.Serialize(typeof(T));
            var messageBody = serializer.MessageToBytes(message);

            return bus.PublishAsync(new ScheduleMe
                {
                    WakeTime = futurePublishDate,
                    BindingKey = typeName,
                    CancellationKey = cancellationKey,
                    InnerMessage = messageBody
                });
        }

        public Task UnscheduleAsync( string cancellationKey )
        {
            return bus.PublishAsync(new UnscheduleMe
                {
                    CancellationKey = cancellationKey
                });
        }
    }
}