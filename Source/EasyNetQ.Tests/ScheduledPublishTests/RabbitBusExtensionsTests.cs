// ReSharper disable InconsistentNaming

using System;
using EasyNetQ.ScheduledPublish;
using EasyNetQ.Tests.Integration;
using NUnit.Framework;
using Rhino.Mocks;

namespace EasyNetQ.Tests.ScheduledPublishTests
{
    [TestFixture]
    public class RabbitBusExtensionsTests
    {
        [Test]
        public void FuturePublish_should_delegate_to_dead_letter_exchange_scheduler()
        {
            var scheduler = MockRepository.GenerateStub<IDeadLetterExchangeScheduler>();
            var bus = CreateMockBus( scheduler );

            var invitation = new PartyInvitation
                {
                    Text = "Please come to my party",
                    Date = new DateTime( 2011, 5, 24 )
                };
            var delay = TimeSpan.FromSeconds( 3 );

            bus.FuturePublish( delay, invitation);

            scheduler.AssertWasCalled( s => s.Schedule( invitation, delay ));
        }

        [Test]
        public void FuturePublishAsync_should_delegate_to_dead_letter_exchange_scheduler()
        {
            var scheduler = MockRepository.GenerateStub<IDeadLetterExchangeScheduler>();
            var bus = CreateMockBus(scheduler);

            var invitation = new PartyInvitation
            {
                Text = "Please come to my party",
                Date = new DateTime(2011, 5, 24)
            };
            var delay = TimeSpan.FromSeconds(3);

            bus.FuturePublishAsync( delay, invitation );

            scheduler.AssertWasCalled( s => s.ScheduleAsync( invitation, delay ) );
        }

        private static IBus CreateMockBus( IDeadLetterExchangeScheduler scheduler )
        {
            var container = MockRepository.GenerateStub<IContainer>();
            container.Stub( c => c.Resolve<IDeadLetterExchangeScheduler>() ).Return( scheduler );

            var advancedBus = MockRepository.GenerateStub<IAdvancedBus>();
            advancedBus.Stub( b => b.Container ).Return( container );

            var bus = MockRepository.GenerateStub<IBus>();
            bus.Stub( b => b.Advanced ).Return( advancedBus );

            return bus;
        }
    }
}