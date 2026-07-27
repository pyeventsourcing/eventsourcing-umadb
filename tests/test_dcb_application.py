import threading
import time
from typing import Any, Dict
from unittest import TestCase

from eventsourcing.domain import TaggedEvent, TEnvelope, event
from eventsourcing.persistence import Tracking
from eventsourcing.popo import POPOTrackingRecorder
from eventsourcing.projection import Projection, ProjectionRunner
from eventsourcing.pydantic import DcbApplication, Decision, EnduringObject
from eventsourcing.utils import get_topic
from umadb import CancelledByUserError


class TrainingSchool(DcbApplication):
    context_name = "dog_school"

    def register(self, name: str) -> str:
        dog = Dog(name=name)
        self.repository.save(dog)
        return dog.id

    def add_trick(self, dog_id: str, trick: str) -> None:
        dog = self.repository.get(dog_id, Dog)
        dog.add_trick(trick)
        self.repository.save(dog)

    def get_dog(self, dog_id: str) -> Dict[str, Any]:
        dog = self.repository.get(dog_id, Dog)
        return {"name": dog.name, "tricks": tuple(dog.tricks)}


class Dog(EnduringObject):
    class Registered(Decision):
        dog_id: str
        name: str

    class TrickAdded(Decision):
        dog_id: str
        trick: str

    @event(Registered)
    def __init__(self, *, name: str) -> None:
        self.name = name
        self.tricks: list[str] = []

    @event(TrickAdded)
    def add_trick(self, trick: str) -> None:
        self.tricks.append(trick)


class TestDcbApplication(TestCase):
    dog_school_env = {
        "DOG_SCHOOL_PERSISTENCE_MODULE": "eventsourcing_umadb",
        "DOG_SCHOOL_UMADB_URI": "http://127.0.0.1:50051",
    }

    def test_app(self) -> None:
        app = TrainingSchool(env=self.dog_school_env)
        # Register dog.
        dog_id = app.register("Fido")

        # Add tricks.
        app.add_trick(dog_id, "roll over")
        app.add_trick(dog_id, "play dead")

        # Get details.
        dog = app.get_dog(dog_id)
        assert dog["name"] == "Fido"
        assert dog["tricks"] == ("roll over", "play dead")

        subscription1 = app.application_subscription(topics=[get_topic(Dog.TrickAdded)])
        envelope, _ = next(subscription1)
        self.assertIsInstance(envelope, TaggedEvent)

        subscription2 = app.application_subscription(topics=[get_topic(Dog.TrickAdded)])
        subscription2.stop()
        with self.assertRaises(StopIteration):
            next(subscription2)

        with app:
            subscription3 = app.application_subscription(
                topics=[get_topic(Dog.TrickAdded)]
            )
        with self.assertRaises(CancelledByUserError):
            next(subscription3)

    def test_projection_runner_works_with_umadb_dcb_subscriptions(self) -> None:
        projection_is_running = threading.Event()

        class MyView(POPOTrackingRecorder):
            pass

        class MyProjection(Projection[MyView, TaggedEvent[Decision]]):
            name = "projection"
            topics = [get_topic(Dog.TrickAdded)]

            def process_event(self, envelope: TEnvelope, tracking: Tracking) -> None:
                projection_is_running.set()
                # Just return to other threads so this test isn't delayed.
                time.sleep(0.01)

        runner = ProjectionRunner(
            application_class=TrainingSchool,
            projection_class=MyProjection,
            view_class=MyView,
            env=self.dog_school_env,
        )

        # This shouldn't hang (on exiting from the runner context manager).
        with runner:
            self.assertTrue(projection_is_running.wait(timeout=1))

        # This shouldn't hang either (the runner should have been interrupted).
        runner.run_forever()
