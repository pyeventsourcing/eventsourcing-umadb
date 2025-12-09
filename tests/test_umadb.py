import datetime
import os
import unittest
from unittest import skipIf
from uuid import uuid4

from umadb import AppendCondition, Client, Event, Query, QueryItem


class TestUmaDBClient(unittest.TestCase):
    def _generate_tagged_event(self, tag: str) -> Event:
        return Event(
            uuid=str(uuid4()),
            event_type="OrderCreated",
            data=b"12345",
            tags=[tag],
        )

    def _generate_tag(self) -> str:
        return "foo" + str(uuid4()) + ":" + "bar"

    # @skipIf("TEST_BENCHMARK_NUM_ITERS" not in os.environ, "Don't mess up the tags")
    def test_benchmark_dcb_append(self) -> None:
        # Just for comparison with Axon Server.
        client = Client("http://127.0.0.1:50051")

        print()
        num_iters = int(os.environ.get("TEST_BENCHMARK_NUM_ITERS", 3))
        for i in range(num_iters):
            start = datetime.datetime.now()
            num_per_iter = 1000
            for j in range(num_per_iter):
                tag1 = self._generate_tag()
                client.append(
                    events=[self._generate_tagged_event(tag1)],
                    condition=AppendCondition(
                        fail_if_events_match=Query(
                            items=[
                                QueryItem(
                                    tags=[tag1],
                                    types=["OrderCreated"],
                                )
                            ]
                        ),
                        after=0,
                    ),
                )
            duration = datetime.datetime.now() - start
            rate = num_per_iter / duration.total_seconds()
            print(f"After {(i + 1) * num_per_iter:} events, rate: {rate:.0f} events/s")
