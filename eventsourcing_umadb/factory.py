# -*- coding: utf-8 -*-
from typing import Any

from eventsourcing.dcb.api import DCBRecorder
from eventsourcing.dcb.persistence import DCBInfrastructureFactory
from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    BaseInfrastructureFactory,
    InfrastructureFactory,
    ProcessRecorder,
    TrackingRecorder,
)
from eventsourcing.utils import Environment
from umadb import Client

from eventsourcing_umadb.recorders import (
    UmaDBAggregateRecorder,
    UmaDBApplicationRecorder,
    UmaDBDCBRecorder,
)


class BaseUmaDBFactory(BaseInfrastructureFactory[TrackingRecorder]):
    UMADB_URI = "UMADB_URI"

    def __init__(self, env: Environment):
        super().__init__(env)
        uri = self.env.get(self.UMADB_URI)
        if uri is None:
            raise EnvironmentError(
                f"'{self.UMADB_URI}' not found "
                "in environment with keys: "
                f"'{', '.join(self.env.create_keys(self.UMADB_URI))}'"
            )
        self.umadb = Client(url=uri)

    def __del__(self) -> None:
        if hasattr(self, "umadb"):
            del self.umadb


class Factory(BaseUmaDBFactory, InfrastructureFactory):
    """
    Infrastructure factory for UmaDB infrastructure.
    """

    def aggregate_recorder(self, purpose: str = "events") -> AggregateRecorder:
        return UmaDBAggregateRecorder(
            umadb=self.umadb, for_snapshotting=bool(purpose == "snapshots")
        )

    def application_recorder(self) -> ApplicationRecorder:
        return UmaDBApplicationRecorder(self.umadb)

    def process_recorder(self) -> ProcessRecorder:
        raise NotImplementedError()

    def tracking_recorder(
        self, tracking_record_class: type[TrackingRecorder] | None = None
    ) -> TrackingRecorder:
        raise NotImplementedError()


class DCBFactory(BaseUmaDBFactory, DCBInfrastructureFactory[TrackingRecorder]):
    def dcb_recorder(self) -> DCBRecorder:
        return UmaDBDCBRecorder(self.umadb)
