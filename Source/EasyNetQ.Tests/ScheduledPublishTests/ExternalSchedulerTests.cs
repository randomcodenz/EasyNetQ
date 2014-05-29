// ReSharper disable InconsistentNaming

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EasyNetQ.Consumer;
using EasyNetQ.FluentConfiguration;
using EasyNetQ.ScheduledPublish;
using EasyNetQ.SystemMessages;
using EasyNetQ.Tests.Integration;
using NUnit.Framework;
using Rhino.Mocks;
using System.Linq;

namespace EasyNetQ.Tests.ScheduledPublishTests
{
    [TestFixture]
    public class ExternalSchedulerTests
    {
        [Test]
        public void Schedule_should_publish_ScheduleMe_message()
        {
            var typeNameSerialiser = new TypeNameSerializer();
            var serialiser = new JsonSerializer( typeNameSerialiser );
            var bus = MockRepository.GeneratePartialMock<PartialBusStub>();
            var scheduler = new ExternalScheduler( bus, typeNameSerialiser, serialiser );

            var invitation = new PartyInvitation
            {
                Text = "Please come to my party",
                Date = new DateTime(2011, 5, 24)
            };
            var publishDate = DateTime.UtcNow.AddSeconds( 3 );

            scheduler.Schedule( invitation, publishDate );

            Assert.That( bus.PublishedMessages, Has.Count.EqualTo( 1 ) );
            Assert.That( bus.PublishedMessages[ 0 ], Is.TypeOf<ScheduleMe>() );

            var scheduleMessage = bus.PublishedMessages.OfType<ScheduleMe>().Single();
            Assert.That( scheduleMessage.WakeTime, Is.EqualTo( publishDate ) );
            Assert.That( scheduleMessage.BindingKey, Is.EqualTo( typeNameSerialiser.Serialize( typeof(PartyInvitation)) ) );
            Assert.That( scheduleMessage.CancellationKey, Is.Null );
            Assert.That( scheduleMessage.InnerMessage, Is.EqualTo( serialiser.MessageToBytes( invitation ) ) );
        }

        [Test]
        public void Schedule_should_publish_ScheduleMe_message_with_cancellation_token()
        {
            var typeNameSerialiser = new TypeNameSerializer();
            var serialiser = new JsonSerializer(typeNameSerialiser);
            var bus = MockRepository.GeneratePartialMock<PartialBusStub>();
            var scheduler = new ExternalScheduler(bus, typeNameSerialiser, serialiser);

            var invitation = new PartyInvitation
            {
                Text = "Please come to my party",
                Date = new DateTime(2011, 5, 24)
            };
            var publishDate = DateTime.UtcNow.AddSeconds(3);
            var cancellationToken = Guid.NewGuid().ToString();

            scheduler.Schedule( invitation, publishDate, cancellationToken );

            Assert.That( bus.PublishedMessages, Has.Count.EqualTo( 1 ) );
            Assert.That( bus.PublishedMessages[ 0 ], Is.TypeOf<ScheduleMe>() );

            var scheduleMessage = bus.PublishedMessages.OfType<ScheduleMe>().Single();
            Assert.That( scheduleMessage.WakeTime, Is.EqualTo( publishDate ) );
            Assert.That( scheduleMessage.BindingKey, Is.EqualTo( typeNameSerialiser.Serialize( typeof( PartyInvitation ) ) ) );
            Assert.That(scheduleMessage.CancellationKey, Is.EqualTo( cancellationToken ));
            Assert.That( scheduleMessage.InnerMessage, Is.EqualTo( serialiser.MessageToBytes( invitation ) ) );
        }

        [Test]
        public void ScheduleAsync_should_async_publish_ScheduleMe_message()
        {
            var typeNameSerialiser = new TypeNameSerializer();
            var serialiser = new JsonSerializer(typeNameSerialiser);
            var bus = MockRepository.GeneratePartialMock<PartialBusStub>();
            var scheduler = new ExternalScheduler(bus, typeNameSerialiser, serialiser);

            var invitation = new PartyInvitation
            {
                Text = "Please come to my party",
                Date = new DateTime(2011, 5, 24)
            };
            var publishDate = DateTime.UtcNow.AddSeconds(3);

            var result = scheduler.ScheduleAsync( invitation, publishDate );
            result.Wait( 10000 );

            Assert.That( bus.AsynchPublishedMessages, Has.Count.EqualTo( 1 ) );
            Assert.That(bus.AsynchPublishedMessages[0], Is.TypeOf<ScheduleMe>());

            var scheduleMessage = bus.AsynchPublishedMessages.OfType<ScheduleMe>().Single();
            Assert.That( scheduleMessage.WakeTime, Is.EqualTo( publishDate ) );
            Assert.That( scheduleMessage.BindingKey, Is.EqualTo( typeNameSerialiser.Serialize( typeof( PartyInvitation ) ) ) );
            Assert.That( scheduleMessage.CancellationKey, Is.Null );
            Assert.That( scheduleMessage.InnerMessage, Is.EqualTo( serialiser.MessageToBytes( invitation ) ) );
        }

        [Test]
        public void ScheduleAsync_should_async_publish_ScheduleMe_message_with_cancellation_token()
        {
            var typeNameSerialiser = new TypeNameSerializer();
            var serialiser = new JsonSerializer(typeNameSerialiser);
            var bus = MockRepository.GeneratePartialMock<PartialBusStub>();
            var scheduler = new ExternalScheduler(bus, typeNameSerialiser, serialiser);

            var invitation = new PartyInvitation
            {
                Text = "Please come to my party",
                Date = new DateTime(2011, 5, 24)
            };
            var publishDate = DateTime.UtcNow.AddSeconds(3);
            var cancellationToken = Guid.NewGuid().ToString();

            var result = scheduler.ScheduleAsync( invitation, publishDate, cancellationToken );
            result.Wait( 10000 );

            Assert.That( bus.AsynchPublishedMessages, Has.Count.EqualTo( 1 ) );
            Assert.That(bus.AsynchPublishedMessages[0], Is.TypeOf<ScheduleMe>());

            var scheduleMessage = bus.AsynchPublishedMessages.OfType<ScheduleMe>().Single();
            Assert.That( scheduleMessage.WakeTime, Is.EqualTo( publishDate ) );
            Assert.That( scheduleMessage.BindingKey, Is.EqualTo( typeNameSerialiser.Serialize( typeof( PartyInvitation ) ) ) );
            Assert.That(scheduleMessage.CancellationKey, Is.EqualTo( cancellationToken ));
            Assert.That( scheduleMessage.InnerMessage, Is.EqualTo( serialiser.MessageToBytes( invitation ) ) );
        }

        [Test]
        public void Unschedule_should_publish_UnscheduleMe_message()
        {
            var typeNameSerialiser = new TypeNameSerializer();
            var serialiser = new JsonSerializer(typeNameSerialiser);
            var bus = MockRepository.GeneratePartialMock<PartialBusStub>();
            var scheduler = new ExternalScheduler(bus, typeNameSerialiser, serialiser);

            var cancellationToken = Guid.NewGuid().ToString();

            scheduler.Unschedule( cancellationToken );

            Assert.That( bus.PublishedMessages, Has.Count.EqualTo( 1 ) );
            Assert.That( bus.PublishedMessages[ 0 ], Is.TypeOf<UnscheduleMe>() );

            var unscheduleMessage = bus.PublishedMessages.OfType<UnscheduleMe>().Single();
            Assert.That( unscheduleMessage.CancellationKey, Is.EqualTo( cancellationToken ) );
        }

        [Test]
        public void UnscheduleAsync_should_async_publish_UnscheduleMe_message()
        {
            var typeNameSerialiser = new TypeNameSerializer();
            var serialiser = new JsonSerializer(typeNameSerialiser);
            var bus = MockRepository.GeneratePartialMock<PartialBusStub>();
            var scheduler = new ExternalScheduler(bus, typeNameSerialiser, serialiser);

            var cancellationToken = Guid.NewGuid().ToString();

            var result = scheduler.UnscheduleAsync( cancellationToken );
            result.Wait( 10000 );

            Assert.That( bus.AsynchPublishedMessages, Has.Count.EqualTo( 1 ) );
            Assert.That( bus.AsynchPublishedMessages[ 0 ], Is.TypeOf<UnscheduleMe>() );

            var unscheduleMessage = bus.AsynchPublishedMessages.OfType<UnscheduleMe>().Single();
            Assert.That( unscheduleMessage.CancellationKey, Is.EqualTo( cancellationToken ) );
        }

        public abstract class PartialBusStub : BusStub
        {
            protected PartialBusStub()
            {
                PublishedMessages = new List<object>();
                AsynchPublishedMessages = new List<object>();
            }

            public List<object> PublishedMessages { get; private set; }
            public List<object> AsynchPublishedMessages { get; private set; }

            public override void Publish<T>(T message)
            {
                PublishedMessages.Add( message );
            }

            public override Task PublishAsync<T>(T message)
            {
                AsynchPublishedMessages.Add( message );

                var tcs = new TaskCompletionSource<object>();
                tcs.SetResult( null );
                return tcs.Task;
            }
        }

        public abstract class BusStub : IBus
        {
            public abstract void Dispose();
            public abstract void Publish<T>( T message ) where T : class;
            public abstract void Publish<T>( T message, string topic ) where T : class;
            public abstract Task PublishAsync<T>( T message ) where T : class;
            public abstract Task PublishAsync<T>( T message, string topic ) where T : class;
            public abstract IDisposable Subscribe<T>( string subscriptionId, Action<T> onMessage ) where T : class;
            public abstract IDisposable Subscribe<T>( string subscriptionId, Action<T> onMessage, Action<ISubscriptionConfiguration> configure ) where T : class;
            public abstract IDisposable SubscribeAsync<T>( string subscriptionId, Func<T, Task> onMessage ) where T : class;
            public abstract IDisposable SubscribeAsync<T>( string subscriptionId, Func<T, Task> onMessage, Action<ISubscriptionConfiguration> configure ) where T : class;
            public abstract TResponse Request<TRequest, TResponse>( TRequest request ) where TRequest : class where TResponse : class;
            public abstract Task<TResponse> RequestAsync<TRequest, TResponse>( TRequest request ) where TRequest : class where TResponse : class;
            public abstract IDisposable Respond<TRequest, TResponse>( Func<TRequest, TResponse> responder ) where TRequest : class where TResponse : class;
            public abstract IDisposable RespondAsync<TRequest, TResponse>( Func<TRequest, Task<TResponse>> responder ) where TRequest : class where TResponse : class;
            public abstract void Send<T>( string queue, T message ) where T : class;
            public abstract IDisposable Receive<T>( string queue, Action<T> onMessage ) where T : class;
            public abstract IDisposable Receive<T>( string queue, Func<T, Task> onMessage ) where T : class;
            public abstract IDisposable Receive( string queue, Action<IReceiveRegistration> addHandlers );
            public abstract event Action Connected;
            public abstract event Action Disconnected;
            public abstract bool IsConnected { get; }
            public abstract IAdvancedBus Advanced { get; }
        }
    }
}