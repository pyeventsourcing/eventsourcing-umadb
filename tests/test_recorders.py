# -*- coding: utf-8 -*-
from timeit import timeit
from typing import cast
from uuid import uuid4

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    IntegrityError,
    StoredEvent,
)
from eventsourcing.tests.persistence import (
    AggregateRecorderTestCase,
    ApplicationRecorderTestCase,
)
from umadb import AppendCondition, Client, Event, Query, QueryItem

from eventsourcing_umadb.recorders import (
    UmaDBAggregateRecorder,
    UmaDBApplicationRecorder,
)

DEFAULT_LOCAL_UMADB_URI = "http://127.0.0.1:50051"


class TestUmaDBAggregateRecorder(AggregateRecorderTestCase):
    def setUp(self) -> None:
        self.umadb = Client(DEFAULT_LOCAL_UMADB_URI)

    def create_recorder(self) -> AggregateRecorder:
        return UmaDBAggregateRecorder(umadb=self.umadb)

    def test_insert_and_select(self) -> None:
        super(TestUmaDBAggregateRecorder, self).test_insert_and_select()

    def test_performance(self) -> None:
        super().test_performance()

        # # Construct the recorder.
        # recorder = self.create_recorder()
        #
        # def insert() -> None:
        #     originator_id = uuid4()
        #
        #     stored_event = StoredEvent(
        #         originator_id=originator_id,
        #         originator_version=self.INITIAL_VERSION,
        #         topic="topic1",
        #         state=b"state1",
        #     )
        #     recorder.insert_events([stored_event])
        #
        # number = 1000
        # while True:
        #     duration = timeit(insert, number=number)
        #     print(
        #         self,
        #         f"\n{1000000 * duration / number:.1f} μs per insert, "
        #         f"{number / duration:.0f} inserts per second",
        #     )

    # def test_performance_direct(self) -> None:
    #     # Construct the recorder.
    #     recorder = cast(UmaDBAggregateRecorder, self.create_recorder())
    #
    #     tags = ["something1"*4, "something2"*4]
    #     def insert() -> None:
    #         recorder.umadb.append(
    #             events=[
    #                 Event(event_type="Nothing", data=b"data", tags=tags),
    #             ],
    #             condition=AppendCondition(
    #                 fail_if_events_match=Query(
    #                     items=[
    #                         QueryItem(
    #                             tags=["somethingelse1"*4, "somethingelse2"*4]
    #                         )
    #                     ]
    #                 )
    #             ),
    #         )
    #
    #     number = 1000
    #     while True:
    #         duration = timeit(insert, number=number)
    #         print(
    #             self,
    #             f"\n{1000000 * duration / number:.1f} μs per insert, "
    #             f"{number / duration:.0f} inserts per second",
    #         )


class TestUmaDBApplicationRecorder(ApplicationRecorderTestCase):
    INITIAL_VERSION = 0

    def setUp(self) -> None:
        self.umadb = Client(DEFAULT_LOCAL_UMADB_URI)

    def create_recorder(self) -> ApplicationRecorder:
        return UmaDBApplicationRecorder(umadb=self.umadb)

    def test_insert_select(self) -> None:
        # TODO: The common test case doesn't work because it assumes there are no events.
        recorder = self.create_recorder()
        start_notification_id = recorder.max_notification_id()

        # Call the super class test....
        self.super_test_insert_select(start_notification_id=start_notification_id)

    def super_test_insert_select(
        self, start_notification_id: int | None = None
    ) -> None:
        from eventsourcing.tests.persistence import convert_notification_originator_ids

        start = start_notification_id or 0

        # Construct the recorder.
        recorder = self.create_recorder()

        # Check notifications methods work when there aren't any.
        self.assertEqual(
            len(recorder.select_notifications(start=start + 1, limit=3)), 0
        )
        self.assertEqual(
            len(
                recorder.select_notifications(
                    start=start_notification_id, limit=3, topics=["topic1"]
                )
            ),
            0,
        )

        self.assertEqual(recorder.max_notification_id(), start_notification_id)

        # Write two stored events.
        originator_id1 = uuid4()
        originator_id2 = uuid4()

        stored_event1 = StoredEvent(
            originator_id=originator_id1,
            originator_version=self.INITIAL_VERSION,
            topic="topic1",
            state=b"state1",
        )
        stored_event2 = StoredEvent(
            originator_id=originator_id1,
            originator_version=self.INITIAL_VERSION + 1,
            topic="topic2",
            state=b"state2",
        )

        notification_ids = recorder.insert_events([stored_event1, stored_event2])
        self.assertEqual(notification_ids, [start + 1, start + 2])

        # Store a third event.
        stored_event3 = StoredEvent(
            originator_id=originator_id2,
            originator_version=self.INITIAL_VERSION,
            topic="topic3",
            state=b"state3",
        )
        notification_ids = recorder.insert_events([stored_event3])
        self.assertEqual(notification_ids, [start + 3])

        stored_events1 = recorder.select_events(originator_id1)
        stored_events2 = recorder.select_events(originator_id2)

        # Check we got what was written.
        self.assertEqual(len(stored_events1), 2)
        self.assertEqual(len(stored_events2), 1)

        # Check get record conflict error if attempt to store it again.
        with self.assertRaises(IntegrityError):
            recorder.insert_events([stored_event3])

        notifications = recorder.select_notifications(start=start + 1, limit=10)
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 3)
        self.assertEqual(notifications[0].id, start + 1)
        self.assertEqual(notifications[0].originator_id, originator_id1)
        self.assertEqual(notifications[0].topic, "topic1")
        self.assertEqual(notifications[0].state, b"state1")
        self.assertEqual(notifications[1].id, start + 2)
        self.assertEqual(notifications[1].originator_id, originator_id1)
        self.assertEqual(notifications[1].topic, "topic2")
        self.assertEqual(notifications[1].state, b"state2")
        self.assertEqual(notifications[2].id, start + 3)
        self.assertEqual(notifications[2].originator_id, originator_id2)
        self.assertEqual(notifications[2].topic, "topic3")
        self.assertEqual(notifications[2].state, b"state3")

        notifications = recorder.select_notifications(start=start + 1, limit=10)
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 3)
        self.assertEqual(notifications[0].id, start + 1)
        self.assertEqual(notifications[0].originator_id, originator_id1)
        self.assertEqual(notifications[0].topic, "topic1")
        self.assertEqual(notifications[0].state, b"state1")
        self.assertEqual(notifications[1].id, start + 2)
        self.assertEqual(notifications[1].originator_id, originator_id1)
        self.assertEqual(notifications[1].topic, "topic2")
        self.assertEqual(notifications[1].state, b"state2")
        self.assertEqual(notifications[2].id, start + 3)
        self.assertEqual(notifications[2].originator_id, originator_id2)
        self.assertEqual(notifications[2].topic, "topic3")
        self.assertEqual(notifications[2].state, b"state3")

        notifications = recorder.select_notifications(
            start=start + 1, stop=start + 2, limit=10
        )
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 2)
        self.assertEqual(notifications[0].id, start + 1)
        self.assertEqual(notifications[0].originator_id, originator_id1)
        self.assertEqual(notifications[0].topic, "topic1")
        self.assertEqual(notifications[0].state, b"state1")
        self.assertEqual(notifications[1].id, start + 2)
        self.assertEqual(notifications[1].originator_id, originator_id1)
        self.assertEqual(notifications[1].topic, "topic2")
        self.assertEqual(notifications[1].state, b"state2")

        notifications = recorder.select_notifications(
            start=start + 1, limit=10, inclusive_of_start=False
        )
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 2)
        self.assertEqual(notifications[0].id, start + 2)
        self.assertEqual(notifications[0].originator_id, originator_id1)
        self.assertEqual(notifications[0].topic, "topic2")
        self.assertEqual(notifications[0].state, b"state2")
        self.assertEqual(notifications[1].id, start + 3)
        self.assertEqual(notifications[1].originator_id, originator_id2)
        self.assertEqual(notifications[1].topic, "topic3")
        self.assertEqual(notifications[1].state, b"state3")

        notifications = recorder.select_notifications(
            start=start + 2, limit=10, inclusive_of_start=False
        )
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0].id, start + 3)
        self.assertEqual(notifications[0].originator_id, originator_id2)
        self.assertEqual(notifications[0].topic, "topic3")
        self.assertEqual(notifications[0].state, b"state3")

        notifications = recorder.select_notifications(
            start=start + 1, limit=10, topics=["topic1", "topic2", "topic3"]
        )
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 3)
        self.assertEqual(notifications[0].id, start + 1)
        self.assertEqual(notifications[0].originator_id, originator_id1)
        self.assertEqual(notifications[0].topic, "topic1")
        self.assertEqual(notifications[0].state, b"state1")
        self.assertEqual(notifications[1].id, start + 2)
        self.assertEqual(notifications[1].originator_id, originator_id1)
        self.assertEqual(notifications[1].topic, "topic2")
        self.assertEqual(notifications[1].state, b"state2")
        self.assertEqual(notifications[2].id, start + 3)
        self.assertEqual(notifications[2].originator_id, originator_id2)
        self.assertEqual(notifications[2].topic, "topic3")
        self.assertEqual(notifications[2].state, b"state3")

        notifications = recorder.select_notifications(start + 1, 10, topics=["topic1"])
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0].id, start + 1)
        self.assertEqual(notifications[0].originator_id, originator_id1)
        self.assertEqual(notifications[0].topic, "topic1")
        self.assertEqual(notifications[0].state, b"state1")

        notifications = recorder.select_notifications(start + 1, 3, topics=["topic2"])
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0].id, start + 2)
        self.assertEqual(notifications[0].originator_id, originator_id1)
        self.assertEqual(notifications[0].topic, "topic2")
        self.assertEqual(notifications[0].state, b"state2")

        notifications = recorder.select_notifications(start + 1, 3, topics=["topic3"])
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0].id, start + 3)
        self.assertEqual(notifications[0].originator_id, originator_id2)
        self.assertEqual(notifications[0].topic, "topic3")
        self.assertEqual(notifications[0].state, b"state3")

        notifications = recorder.select_notifications(
            start + 1, 3, topics=["topic1", "topic3"]
        )
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 2)
        self.assertEqual(notifications[0].id, start + 1)
        self.assertEqual(notifications[0].originator_id, originator_id1)
        self.assertEqual(notifications[0].topic, "topic1")
        self.assertEqual(notifications[0].state, b"state1")
        self.assertEqual(notifications[1].id, start + 3)
        self.assertEqual(notifications[1].topic, "topic3")
        self.assertEqual(notifications[1].state, b"state3")

        self.assertEqual(recorder.max_notification_id(), start + 3)

        # Check limit is working
        notifications = recorder.select_notifications(start + 1, 1)
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0].id, start + 1)

        notifications = recorder.select_notifications(start + 2, 1)
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0].id, start + 2)

        notifications = recorder.select_notifications(
            start + 1, 1, inclusive_of_start=False
        )
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0].id, start + 2)

        notifications = recorder.select_notifications(start + 2, 2)
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 2)
        self.assertEqual(notifications[0].id, start + 2)
        self.assertEqual(notifications[1].id, start + 3)

        notifications = recorder.select_notifications(start + 3, 1)
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0].id, start + 3)

        notifications = recorder.select_notifications(
            start + 3, 1, inclusive_of_start=False
        )
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 0)

        notifications = recorder.select_notifications(start=start + 2, limit=10, stop=2)
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0].id, start + 2)

        notifications = recorder.select_notifications(
            start=start + 1, limit=10, stop=start + 2
        )
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 2, len(notifications))
        self.assertEqual(notifications[0].id, start + 1)
        self.assertEqual(notifications[1].id, start + 2)

        notifications = recorder.select_notifications(
            start=start + 1, limit=10, stop=start + 2, inclusive_of_start=False
        )
        notifications = convert_notification_originator_ids(notifications)
        self.assertEqual(len(notifications), 1, len(notifications))
        self.assertEqual(notifications[0].id, start + 2)

    def test_concurrent_no_conflicts(self) -> None:
        super().test_concurrent_no_conflicts()

    def test_concurrent_throughput(self) -> None:
        super().test_concurrent_throughput()


del AggregateRecorderTestCase
del ApplicationRecorderTestCase
