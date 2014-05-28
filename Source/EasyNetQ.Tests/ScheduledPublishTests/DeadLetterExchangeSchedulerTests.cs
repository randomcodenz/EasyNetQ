// ReSharper disable InconsistentNaming

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EasyNetQ.Consumer;
using EasyNetQ.Producer;
using EasyNetQ.ScheduledPublish;
using EasyNetQ.Tests.Integration;
using EasyNetQ.Topology;
using NUnit.Framework;
using Rhino.Mocks;
using Rhino.Mocks.Interfaces;

namespace EasyNetQ.Tests.ScheduledPublishTests
{
    [TestFixture]
    public class DeadLetterExchangeSchedulerTests
    {
        [Test]
        public void Schedule_should_prepare_the_target_exchange()
        {
            var publishExchangeDeclareStrategy = new PublishExchangeDeclareStrategyStub();
            var scheduler = CreateScheduler( publishExchangeDeclareStrategy );
            var invitation = new PartyInvitation
                {
                    Text = "Please come to my party",
                    Date = new DateTime( 2011, 5, 24 )
                };
            var delay = TimeSpan.FromSeconds( 3 );

            scheduler.Schedule( invitation, delay );

            AssertTargetExchangeConfigured( publishExchangeDeclareStrategy );
        }

        [Test]
        public void Schedule_should_prepare_the_scheduling_exchange_and_queue()
        {
            var exchangeNamingConvention = new Dictionary<Type, string>
                {
                    {typeof( PartyInvitation ), "Bob"}
                };
            var publishExchangeDeclareStrategy = new PublishExchangeDeclareStrategyStub(exchangeNamingConvention);
            var advancedBus = CreateAdvancedBus();
            var scheduler = CreateScheduler( publishExchangeDeclareStrategy, advancedBus );
            var invitation = new PartyInvitation
            {
                Text = "Please come to my party",
                Date = new DateTime(2011, 5, 24)
            };
            var delay = TimeSpan.FromSeconds(3);

            scheduler.Schedule( invitation, delay );

            AssertSchedulerExchangeAndQueueConfigured( advancedBus );
        }

        [Test]
        public void Schedule_should_publish_the_message_to_the_scheduling_exchange()
        {
            var advancedBus = CreateAdvancedBus();
            var scheduler = CreateScheduler( advancedBus: advancedBus );
            var invitation = new PartyInvitation
            {
                Text = "Please come to my party",
                Date = new DateTime(2011, 5, 24)
            };
            var delay = TimeSpan.FromSeconds(3);

            scheduler.Schedule(invitation, delay);

            Assert.That( advancedBus.DeclaredExchange.MessagesPublished[ "#" ], Is.EqualTo( 1 ) );
        }        
        
        [Test]
        public void ScheduleAsync_should_prepare_the_target_exchange()
        {
            var publishExchangeDeclareStrategy = new PublishExchangeDeclareStrategyStub();
            var scheduler = CreateScheduler( publishExchangeDeclareStrategy );
            var invitation = new PartyInvitation
                {
                    Text = "Please come to my party",
                    Date = new DateTime( 2011, 5, 24 )
                };
            var delay = TimeSpan.FromSeconds( 3 );

            var publish = scheduler.ScheduleAsync( invitation, delay );
            publish.Wait( 1000 );

            AssertTargetExchangeConfigured( publishExchangeDeclareStrategy );
        }

        [Test]
        public void ScheduleAsync_should_prepare_the_scheduling_exchange_and_queue()
        {
            var exchangeNamingConvention = new Dictionary<Type, string>
                {
                    {typeof( PartyInvitation ), "Bob"}
                };
            var publishExchangeDeclareStrategy = new PublishExchangeDeclareStrategyStub(exchangeNamingConvention);
            var advancedBus = CreateAdvancedBus();
            var scheduler = CreateScheduler( publishExchangeDeclareStrategy, advancedBus );
            var invitation = new PartyInvitation
            {
                Text = "Please come to my party",
                Date = new DateTime(2011, 5, 24)
            };
            var delay = TimeSpan.FromSeconds(3);

            var publish = scheduler.ScheduleAsync(invitation, delay);
            publish.Wait( 1000 );

            AssertSchedulerExchangeAndQueueConfigured( advancedBus );
        }

        [Test]
        public void ScheduleAsync_should_publish_the_message_to_the_scheduling_exchange()
        {
            var advancedBus = CreateAdvancedBus();
            var scheduler = CreateScheduler( advancedBus: advancedBus );
            var invitation = new PartyInvitation
            {
                Text = "Please come to my party",
                Date = new DateTime(2011, 5, 24)
            };
            var delay = TimeSpan.FromSeconds(3);

            var publish = scheduler.ScheduleAsync(invitation, delay);
            publish.Wait( 1000 );

            Assert.That( advancedBus.DeclaredExchange.MessagesPublishedAsync[ "#" ], Is.EqualTo( 1 ) );
        }

        private static void AssertTargetExchangeConfigured( PublishExchangeDeclareStrategyStub publishExchangeDeclareStrategy )
        {
            Assert.That( publishExchangeDeclareStrategy.DeclaredExchange.Type, Is.EqualTo( ExchangeType.Topic ) );
            Assert.That( publishExchangeDeclareStrategy.DeclaredExchange.MessageType, Is.EqualTo( typeof( PartyInvitation ) ) );
        }

        private static void AssertSchedulerExchangeAndQueueConfigured( PartialAdvancedBusStub advancedBus )
        {
            Assert.That( advancedBus.DeclaredExchange.Name, Is.EqualTo( "Bob_00_00_03" ) );
            Assert.That( advancedBus.DeclaredExchange.Type, Is.EqualTo( ExchangeType.Topic ) );
            Assert.That( advancedBus.DeclaredQueue.Name, Is.StringEnding( "_00_00_03" ) );
            Assert.That( advancedBus.DeclaredQueue.Ttl, Is.EqualTo( 3000 ) );
            Assert.That( advancedBus.DeclaredQueue.DeadLetterExchange, Is.EqualTo( "Bob" ) );
            Assert.That( advancedBus.DeclaredQueue.BoundTo, Is.EqualTo( advancedBus.DeclaredExchange ) );
            Assert.That( advancedBus.DeclaredQueue.RoutingKey, Is.EqualTo( "#" ) );
        }

        private DeadLetterExchangeScheduler CreateScheduler(IPublishExchangeDeclareStrategy publishExchangeDeclareStrategy = null, IAdvancedBus advancedBus = null)
        {
            var conventions = new Conventions( new TypeNameSerializer() );
            var connectionConfiguration = new ConnectionConfiguration();

            advancedBus = advancedBus ?? CreateAdvancedBus();
            publishExchangeDeclareStrategy = publishExchangeDeclareStrategy ?? new PublishExchangeDeclareStrategyStub();
            return new DeadLetterExchangeScheduler( conventions, connectionConfiguration, advancedBus, publishExchangeDeclareStrategy );
        }

        private static PartialAdvancedBusStub CreateAdvancedBus()
        {
            var advancedBus = MockRepository.GenerateStub<PartialAdvancedBusStub>();
            advancedBus.Stub( b => b.ExchangeDeclare( null, null ) )
                       .IgnoreArguments()
                       .CallOriginalMethod( OriginalCallOptions.NoExpectation );
            advancedBus.Stub( b => b.QueueDeclare( null ) )
                       .IgnoreArguments()
                       .CallOriginalMethod( OriginalCallOptions.NoExpectation );
            advancedBus.Stub( b => b.Bind( null, (IQueue) null, null ) )
                       .IgnoreArguments()
                       .CallOriginalMethod( OriginalCallOptions.NoExpectation );
            advancedBus.Stub( b => b.Publish<PartyInvitation>( null, null, false, false, null ) )
                       .IgnoreArguments()
                       .CallOriginalMethod( OriginalCallOptions.NoExpectation );
            advancedBus.Stub( b => b.PublishAsync<PartyInvitation>( null, null, false, false, null ) )
                       .IgnoreArguments()
                       .CallOriginalMethod( OriginalCallOptions.NoExpectation );
            return advancedBus;
        }

        public class PublishExchangeDeclareStrategyStub : IPublishExchangeDeclareStrategy
        {
            private readonly Dictionary<Type, string> exchangeNamingConvention;

            public PublishExchangeDeclareStrategyStub( Dictionary<Type, string> exchangeNamingConvention = null )
            {
                this.exchangeNamingConvention = exchangeNamingConvention ?? new Dictionary<Type, string>();
            }

            public ExchangeStub DeclaredExchange { get; private set; }

            public IExchange DeclareExchange( IAdvancedBus advancedBus, string exchangeName, string exchangeType )
            {
                var exchange = new ExchangeStub {Name = exchangeName, Type = exchangeType};
                DeclaredExchange = exchange;
                return exchange;
            }

            public IExchange DeclareExchange( IAdvancedBus advancedBus, Type messageType, string exchangeType )
            {
                string exchangeName;
                if( !exchangeNamingConvention.TryGetValue( messageType, out exchangeName ) )
                    exchangeName = Guid.NewGuid().ToString();

                var exchange = new ExchangeStub { Name = exchangeName, Type = exchangeType, MessageType = messageType };
                DeclaredExchange = exchange;
                return exchange;
            }
        }

        public abstract class PartialAdvancedBusStub : AdvancedBusStub
        {
            public ExchangeStub DeclaredExchange { get; private set; }
            public QueueStub DeclaredQueue { get; private set; }

            public override IExchange ExchangeDeclare(string name, string type, bool passive = false, bool durable = true, bool autoDelete = false, bool @internal = false, string alternateExchange = null)
            {
                var exchange = new ExchangeStub {Name = name, Type = type};
                DeclaredExchange = exchange;
                return exchange;
            }

            public override IQueue QueueDeclare(string name, bool passive = false, bool durable = true, bool exclusive = false, bool autoDelete = false, int perQueueTtl = 2147483647, int expires = 2147483647, string deadLetterExchange = null)
            {
                var queue = new QueueStub
                    {
                        Name = name,
                        IsExclusive = exclusive,
                        Ttl = perQueueTtl,
                        DeadLetterExchange = deadLetterExchange
                    };
                DeclaredQueue = queue;
                return queue;
            }

            public override IBinding Bind(IExchange exchange, IQueue queue, string routingKey)
            {
                var stubExchange = exchange as ExchangeStub;
                var stubQueue = queue as QueueStub;
                if( stubExchange != null && stubQueue != null )
                {
                    stubQueue.BoundTo = stubExchange;
                    stubQueue.RoutingKey = routingKey;
                }
                return MockRepository.GenerateStub<IBinding>();
            }

            public override void Publish<T>(IExchange exchange, string routingKey, bool mandatory, bool immediate, IMessage<T> message)
            {
                var stubExchange = exchange as ExchangeStub;
                if( stubExchange == null )
                    return;

                stubExchange.MessagesPublished = stubExchange.MessagesPublished ?? new Dictionary<string, int>();
                RecordMessagePublished( routingKey, stubExchange.MessagesPublished);
            }

            public override Task PublishAsync<T>(IExchange exchange, string routingKey, bool mandatory, bool immediate, IMessage<T> message)
            {
                var tcs = new TaskCompletionSource<object>();
                tcs.SetResult( null );
                var stubExchange = exchange as ExchangeStub;
                if (stubExchange == null)
                    return tcs.Task;

                stubExchange.MessagesPublishedAsync = stubExchange.MessagesPublishedAsync ?? new Dictionary<string, int>();
                RecordMessagePublished( routingKey, stubExchange.MessagesPublishedAsync );
                return tcs.Task;
            }

            private static void RecordMessagePublished( string routingKey, IDictionary<string, int> messagesPublished )
            {
                if( !messagesPublished.ContainsKey( routingKey ) )
                    messagesPublished.Add( routingKey, 0 );

                messagesPublished[ routingKey ] += 1;
            }
        }

        public class ExchangeStub : IExchange
        {
            public string Name { get; set; }
            public string Type { get; set; }
            public Type MessageType { get; set; }
            public Dictionary<string,int> MessagesPublished { get; set; }
            public Dictionary<string,int> MessagesPublishedAsync { get; set; }
        }

        public class QueueStub : IQueue
        {
            public string Name { get; set; }
            public bool IsExclusive { get; set; }
            public int Ttl { get; set; }
            public string DeadLetterExchange { get; set; }
            public ExchangeStub BoundTo { get; set; }
            public string RoutingKey { get; set; }
        }

        public abstract class AdvancedBusStub : IAdvancedBus
        {
            public abstract void Dispose();
            public abstract IDisposable Consume<T>( IQueue queue, Action<IMessage<T>, MessageReceivedInfo> onMessage ) where T : class;
            public abstract IDisposable Consume<T>( IQueue queue, Action<IMessage<T>, MessageReceivedInfo> onMessage, Action<IConsumerConfiguration> configure ) where T : class;
            public abstract IDisposable Consume<T>( IQueue queue, Func<IMessage<T>, MessageReceivedInfo, Task> onMessage ) where T : class;
            public abstract IDisposable Consume<T>( IQueue queue, Func<IMessage<T>, MessageReceivedInfo, Task> onMessage, Action<IConsumerConfiguration> configure ) where T : class;
            public abstract IDisposable Consume( IQueue queue, Action<IHandlerRegistration> addHandlers );
            public abstract IDisposable Consume( IQueue queue, Action<IHandlerRegistration> addHandlers, Action<IConsumerConfiguration> configure );
            public abstract IDisposable Consume( IQueue queue, Func<byte[], MessageProperties, MessageReceivedInfo, Task> onMessage );
            public abstract IDisposable Consume( IQueue queue, Func<byte[], MessageProperties, MessageReceivedInfo, Task> onMessage, Action<IConsumerConfiguration> configure );
            public abstract void Publish( IExchange exchange, string routingKey, bool mandatory, bool immediate, MessageProperties messageProperties, byte[] body );
            public abstract void Publish<T>( IExchange exchange, string routingKey, bool mandatory, bool immediate, IMessage<T> message ) where T : class;
            public abstract Task PublishAsync( IExchange exchange, string routingKey, bool mandatory, bool immediate, MessageProperties messageProperties, byte[] body );
            public abstract Task PublishAsync<T>( IExchange exchange, string routingKey, bool mandatory, bool immediate, IMessage<T> message ) where T : class;
            public abstract IQueue QueueDeclare( string name, bool passive = false, bool durable = true, bool exclusive = false, bool autoDelete = false, int perQueueTtl = 2147483647, int expires = 2147483647, string deadLetterExchange = null );
            public abstract IQueue QueueDeclare();
            public abstract void QueueDelete( IQueue queue, bool ifUnused = false, bool ifEmpty = false );
            public abstract void QueuePurge( IQueue queue );
            public abstract IExchange ExchangeDeclare( string name, string type, bool passive = false, bool durable = true, bool autoDelete = false, bool @internal = false, string alternateExchange = null );
            public abstract void ExchangeDelete( IExchange exchange, bool ifUnused = false );
            public abstract IBinding Bind( IExchange exchange, IQueue queue, string routingKey );
            public abstract IBinding Bind( IExchange source, IExchange destination, string routingKey );
            public abstract void BindingDelete( IBinding binding );
            public abstract bool IsConnected { get; }
            public abstract event Action Connected;
            public abstract event Action Disconnected;
            public abstract event Action<byte[], MessageProperties, MessageReturnedInfo> MessageReturned;
            public abstract IContainer Container { get; }
        }
    }
}