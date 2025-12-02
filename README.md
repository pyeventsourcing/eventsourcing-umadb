# Event Sourcing with UmaDB

This package supports using the Python
[eventsourcing](https://github.com/pyeventsourcing/eventsourcing) library
with [UmaDB](https://umadb.io).

## Installation

Use pip to install the stable distribution from the Python Package Index.

    $ pip install eventsourcing-umadb

Please note, it is recommended to install Python packages into a Python virtual environment.

## Getting started

```python
from typing import Any, Dict
from uuid import UUID

from eventsourcing.application import Application
from eventsourcing.domain import Aggregate, event


class TrainingSchool(Application):
    def register(self, name: str) -> UUID:
        dog = Dog(name)
        self.save(dog)
        return dog.id

    def add_trick(self, dog_id: UUID, trick: str) -> None:
        dog = self.repository.get(dog_id)
        dog.add_trick(trick)
        self.save(dog)

    def get_dog(self, dog_id) -> Dict[str, Any]:
        dog = self.repository.get(dog_id)
        return {'name': dog.name, 'tricks': tuple(dog.tricks)}


class Dog(Aggregate):
    @event('Registered')
    def __init__(self, name: str) -> None:
        self.name = name
        self.tricks = []

    @event('TrickAdded')
    def add_trick(self, trick: str) -> None:
        self.tricks.append(trick)
```

Configure the application to use UmaDB. Set environment variable
`PERSISTENCE_MODULE` to `'eventsourcing_umadb'`, and set
`UMADB_URI` to your UmaDB URI.

```python
school = TrainingSchool(env={
    "PERSISTENCE_MODULE": "eventsourcing_umadb",
    "UMADB_URI": "http://127.0.0.1:50051",
})
```

The application's methods may be then called, from tests and
user interfaces.

```python
# Register dog.
dog_id = school.register('Fido')

# Add tricks.
school.add_trick(dog_id, 'roll over')
school.add_trick(dog_id, 'play dead')

# Get details.
dog = school.get_dog(dog_id)
assert dog["name"] == 'Fido'
assert dog["tricks"] == ('roll over', 'play dead')
```

For more information, please refer to the Python
[eventsourcing](https://github.com/johnbywater/eventsourcing) library
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

Tests belong in `./tests`. Code-under-test belongs in `./Client`.

Edit package dependencies in `pyproject.toml`. Update installed packages (and the
`poetry.lock` file) using the following command.

    $ make update
