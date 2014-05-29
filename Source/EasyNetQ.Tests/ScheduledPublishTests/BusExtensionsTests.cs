// ReSharper disable InconsistentNaming
using System;
using EasyNetQ.ScheduledPublish;
using EasyNetQ.Tests.Integration;
using NUnit.Framework;
using Rhino.Mocks;

namespace EasyNetQ.Tests.ScheduledPublishTests
{
    [TestFixture]
    public class BusExtensionsTests
    {
        [Test]
        public void FuturePublish_should_delegate_to_external_scheduler()
        {
            var scheduler = MockRepository.GenerateStub<IExternalScheduler>();
            var bus = CreateMockBus( scheduler );

            var invitation = new PartyInvitation
                {
                    Text = "Please come to my party",
                    Date = new DateTime( 2011, 5, 24 )
                };
            var publishDate = DateTime.UtcNow.AddSeconds( 3 );

            bus.FuturePublish( publishDate, invitation);

            scheduler.AssertWasCalled( s => s.Schedule( invitation, publishDate ) );
        }

        [Test]
        public void FuturePublish_with_cancellation_token_should_delegate_to_external_scheduler()
        {
            var scheduler = MockRepository.GenerateStub<IExternalScheduler>();
            var bus = CreateMockBus( scheduler );

            var invitation = new PartyInvitation
                {
                    Text = "Please come to my party",
                    Date = new DateTime( 2011, 5, 24 )
                };
            var publishDate = DateTime.UtcNow.AddSeconds( 3 );
            var cancellationToken = Guid.NewGuid().ToString();

            bus.FuturePublish( publishDate, cancellationToken, invitation );

            scheduler.AssertWasCalled( s => s.Schedule( invitation, publishDate, cancellationToken ) );
        }

        [Test]
        public void CancelFuturePublish_should_delegate_to_external_scheduler()
        {
            var scheduler = MockRepository.GenerateStub<IExternalScheduler>();
            var bus = CreateMockBus(scheduler);

            var cancellationToken = Guid.NewGuid().ToString();

            bus.CancelFuturePublish( cancellationToken );

            scheduler.AssertWasCalled( s => s.Unschedule( cancellationToken ) );
        }

        [Test]
        public void FuturePublishAsync_should_delegate_to_external_scheduler()
        {
            var scheduler = MockRepository.GenerateStub<IExternalScheduler>();
            var bus = CreateMockBus(scheduler);

            var invitation = new PartyInvitation
                {
                    Text = "Please come to my party",
                    Date = new DateTime(2011, 5, 24)
                };
            var publishDate = DateTime.UtcNow.AddSeconds(3);

            bus.FuturePublishAsync( publishDate, invitation );

            scheduler.AssertWasCalled( s => s.ScheduleAsync( invitation, publishDate ) );
        }

        [Test]
        public void FuturePublishAsync_with_cancellation_token_should_delegate_to_external_scheduler()
        {
            var scheduler = MockRepository.GenerateStub<IExternalScheduler>();
            var bus = CreateMockBus(scheduler);

            var invitation = new PartyInvitation
                {
                    Text = "Please come to my party",
                    Date = new DateTime(2011, 5, 24)
                };
            var publishDate = DateTime.UtcNow.AddSeconds(3);
            var cancellationToken = Guid.NewGuid().ToString();

            bus.FuturePublishAsync( publishDate, cancellationToken, invitation );

            scheduler.AssertWasCalled( s => s.ScheduleAsync( invitation, publishDate, cancellationToken ) );
        }


        [Test]
        public void CancelFuturePublishAsync_should_delegate_to_external_scheduler()
        {
            var scheduler = MockRepository.GenerateStub<IExternalScheduler>();
            var bus = CreateMockBus(scheduler);

            var cancellationToken = Guid.NewGuid().ToString();

            bus.CancelFuturePublishAsync(cancellationToken);

            scheduler.AssertWasCalled(s => s.UnscheduleAsync(cancellationToken));
        }

        private static IBus CreateMockBus( IExternalScheduler scheduler )
        {
            var container = MockRepository.GenerateStub<IContainer>();
            container.Stub( c => c.Resolve<IExternalScheduler>() ).Return( scheduler );

            var advancedBus = MockRepository.GenerateStub<IAdvancedBus>();
            advancedBus.Stub( b => b.Container ).Return( container );

            var bus = MockRepository.GenerateStub<IBus>();
            bus.Stub( b => b.Advanced ).Return( advancedBus );

            return bus;
        }
    }
}