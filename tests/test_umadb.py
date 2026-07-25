import datetime
import os
import threading
import unittest
from unittest import skipIf
from uuid import uuid4

import time
from umadb import AppendCondition, Client, Event, Query, QueryItem, cancel_all_stream_responses


class TestUmaDbClient(unittest.TestCase):
    def _generate_tagged_event(self, tag: str) -> Event:
        return Event(
            uuid=uuid4(),
            event_type="OrderCreated",
            data=b"12345",
            tags=[tag],
        )

    def _generate_tag(self) -> str:
        return "foo" + str(uuid4()) + ":" + "bar"

    def test_cancel_all_stream_responses_after_one_second(self) -> None:
        client = Client("http://127.0.0.1:50051")
        subscription = client.subscribe(query=Query([QueryItem(tags=[str(uuid4())])]))

        def cancel_after_one_second():
            time.sleep(1)
            cancel_all_stream_responses()

        thread = threading.Thread(target=cancel_after_one_second)
        thread.start()

        with self.assertRaises(KeyboardInterrupt):
            for x in subscription:
                pass


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
