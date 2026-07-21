from typing import Any, Dict, cast
from unittest import TestCase
from uuid import uuid4

from eventsourcing.dcb.application import DCBApplication
from eventsourcing.domain import EnduringObject, event
from eventsourcing.pydantic.application import PydanticDCBApplication
from eventsourcing.pydantic.immutable import PydanticDecision
from eventsourcing.pydantic.mutable import PydanticEnduringObject
from eventsourcing.utils import get_topic


class TrainingSchool(PydanticDCBApplication):

    def register(self, name: str) -> str:
        dog = Dog(dog_id=str(uuid4()), name=name)
        self.repository.save(dog)
        return dog.id

    def add_trick(self, dog_id: str, trick: str) -> None:
        dog = self.repository.get(dog_id, Dog)
        dog.add_trick(trick)
        self.repository.save(dog)

    def get_dog(self, dog_id: str) -> Dict[str, Any]:
        dog = self.repository.get(dog_id, Dog)
        return {"name": dog.name, "tricks": tuple(dog.tricks)}


class Dog(PydanticEnduringObject):
    class Registered(PydanticDecision):
        dog_id: str
        name: str

    @event(Registered)
    def __init__(self, *, dog_id: str, name: str) -> None:
        self.id = dog_id
        self.name = name
        self.tricks: list[str] = []

    class TrickAdded(PydanticDecision):
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
