from typing import Any, Dict, cast
from unittest import TestCase

from eventsourcing.dcb.application import DCBApplication
from eventsourcing.dcb.domain import EnduringObject
from eventsourcing.dcb.msgpack import Decision, InitialDecision, MessagePackMapper
from eventsourcing.domain import event
from eventsourcing.utils import get_topic


class TrainingSchool(DCBApplication):
    env = {"MAPPER_TOPIC": get_topic(MessagePackMapper)}

    def register(self, name: str) -> str:
        dog = Dog(name=name)
        self.repository.save(dog)
        return dog.id

    def add_trick(self, dog_id: str, trick: str) -> None:
        dog = cast(Dog, self.repository.get(dog_id))
        dog.add_trick(trick)
        self.repository.save(dog)

    def get_dog(self, dog_id: str) -> Dict[str, Any]:
        dog = cast(Dog, self.repository.get(dog_id))
        return {"name": dog.name, "tricks": tuple(dog.tricks)}


class Dog(EnduringObject[Decision, str]):
    class Registered(InitialDecision):
        dog_id: str
        name: str

    @event(Registered)
    def __init__(self, *, name: str) -> None:
        self.name = name
        self.tricks: list[str] = []

    class TrickAdded(Decision):
        trick: str

    @event(TrickAdded)
    def add_trick(self, trick: str) -> None:
        self.tricks.append(trick)


class TestDCBApplication(TestCase):
    def test(self) -> None:
        app = TrainingSchool(
            env={
                "PERSISTENCE_MODULE": "eventsourcing_umadb",
                "UMADB_URI": "http://127.0.0.1:50051",
            }
        )
        # Register dog.
        dog_id = app.register("Fido")

        # Add tricks.
        app.add_trick(dog_id, "roll over")
        app.add_trick(dog_id, "play dead")

        # Get details.
        dog = app.get_dog(dog_id)
        assert dog["name"] == "Fido"
        assert dog["tricks"] == ("roll over", "play dead")
