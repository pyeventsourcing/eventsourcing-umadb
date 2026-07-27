# -*- coding: utf-8 -*-
from types import TracebackType
from typing import Self

from eventsourcing.dcb.api import DcbRecorder
from eventsourcing.dcb.persistence import DcbInfrastructureFactory
from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    BaseInfrastructureFactory,
    InfrastructureFactory,
    ProcessRecorder,
    TrackingRecorder,
)
from eventsourcing.utils import Environment, resolve_topic
from umadb import Client

from eventsourcing_umadb.recorders import (
    UmaDbAggregateRecorder,
    UmaDbApplicationRecorder,
    UmaDbDcbRecorder,
)


class BaseUmaDbFactory(BaseInfrastructureFactory[TrackingRecorder]):
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

    def close(self) -> None:
        self.umadb.close()
        super().close()

    def __del__(self) -> None:
        if hasattr(self, "umadb"):
            del self.umadb


class Factory(BaseUmaDbFactory, InfrastructureFactory[TrackingRecorder]):
    """
    Infrastructure factory for UmaDB infrastructure.
    """

    def aggregate_recorder(self, purpose: str = "events") -> AggregateRecorder:
        return UmaDbAggregateRecorder(
            umadb=self.umadb, for_snapshotting=bool(purpose == "snapshots")
        )

    def application_recorder(self) -> ApplicationRecorder:
        application_recorder_topic = self.env.get(self.APPLICATION_RECORDER_TOPIC)
        if application_recorder_topic:
            application_recorder_class: type[UmaDbApplicationRecorder] = resolve_topic(
                application_recorder_topic
            )
            assert issubclass(application_recorder_class, UmaDbApplicationRecorder)
        else:
            application_recorder_class = UmaDbApplicationRecorder

        return application_recorder_class(self.umadb)

    def process_recorder(self) -> ProcessRecorder:
        raise NotImplementedError()

    def tracking_recorder(
        self, tracking_record_class: type[TrackingRecorder] | None = None
    ) -> TrackingRecorder:
        # TODO: We can implement this now that UmaDB supports tracking records.
        raise NotImplementedError()


class DcbFactory(BaseUmaDbFactory, DcbInfrastructureFactory[TrackingRecorder]):
    def dcb_recorder(self) -> DcbRecorder:
        return UmaDbDcbRecorder(self.umadb)
