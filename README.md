# Event Sourcing with UmaDB

This package supports using the Python
[eventsourcing](https://eventsourcing.readthedocs.io/en/stable/topics/dcb.html) library
with [UmaDB](https://umadb.io/).

## Installation

Use pip to install the stable distribution from the Python Package Index.

    $ pip install eventsourcing-umadb

Please note, it is recommended to install Python packages into a Python virtual environment.

## Getting started

Use the `eventsourcing.dcb` package to define a DCB application. Read the [docs](https://eventsourcing.readthedocs.io/en/stable/topics/dcb.html) for more information.

```python
from typing import Any, Dict, cast

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

    class TrickAdded(Decision):
        trick: str

    @event(Registered)
    def __init__(self, *, name: str) -> None:
        self.name = name
        self.tricks: list[str] = []

    @event(TrickAdded)
    def add_trick(self, trick: str) -> None:
        self.tricks.append(trick)

```

Configure the application to use UmaDB. Set environment variable
`PERSISTENCE_MODULE` to `'eventsourcing_umadb'`, and set
`UMADB_URI` to your UmaDB URI.

```python
app = TrainingSchool(
    env={
        "PERSISTENCE_MODULE": "eventsourcing_umadb",
        "UMADB_URI": "http://127.0.0.1:50051",
    }
)
```

The application's methods may be then called, from tests and
user interfaces.

```python
# Register dog.
dog_id = app.register("Fido")

# Add tricks.
app.add_trick(dog_id, "roll over")
app.add_trick(dog_id, "play dead")

# Get details.
dog = app.get_dog(dog_id)
assert dog["name"] == "Fido"
assert dog["tricks"] == ("roll over", "play dead")
```

For more information, please refer to the Python
[eventsourcing](https://eventsourcing.readthedocs.io/en/stable/topics/dcb.html) library
and the [UmaDB](https://umadb.io) project.

## Developers

Clone the GitHub repo and the use the following `make` commands.

Install Poetry.

    $ make install-poetry

Install packages.

    $ make install

Start UmaDB.

    $ make start-umadb

Run tests.

    $ make test

Stop UmaDB.

    $ make stop-umadb

Check the formatting of the code.

    $ make lint

Reformat the code.

    $ make fmt

Tests belong in `./tests`.

Edit package dependencies in `pyproject.toml`. Update installed packages (and the
`poetry.lock` file) using the following command.

    $ make update
