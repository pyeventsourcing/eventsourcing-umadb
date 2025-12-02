# -*- coding: utf-8 -*-
import os
from typing import Type
from unittest import skip

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    InfrastructureFactory,
    ProcessRecorder,
    TrackingRecorder,
)
from eventsourcing.tests.persistence import InfrastructureFactoryTestCase
from eventsourcing.utils import Environment

from eventsourcing_umadb.factory import Factory
from eventsourcing_umadb.recorders import (
    UmaDBAggregateRecorder,
    UmaDBApplicationRecorder,
)

DEFAULT_LOCAL_UMADB_URI = "http://127.0.0.1:50051"


class TestFactory(InfrastructureFactoryTestCase[Factory]):
    def test_create_process_recorder(self) -> None:
        self.skipTest("UmaDB doesn't support tracking records")

    def expected_factory_class(self) -> Type[Factory]:
        return Factory

    def expected_aggregate_recorder_class(self) -> Type[AggregateRecorder]:
        return UmaDBAggregateRecorder

    def expected_application_recorder_class(self) -> Type[ApplicationRecorder]:
        return UmaDBApplicationRecorder

    def expected_process_recorder_class(self) -> Type[ProcessRecorder]:
        raise NotImplementedError()

    def expected_tracking_recorder_class(self) -> Type[TrackingRecorder]:
        raise NotImplementedError()

    def tracking_recorder_subclass(self) -> type[TrackingRecorder]:
        raise NotImplementedError()

    @skip("UmaDB doesn't support tracking records")
    def test_create_tracking_recorder(self) -> None:
        pass

    def setUp(self) -> None:
        self.env = Environment("TestCase")
        self.env[InfrastructureFactory.PERSISTENCE_MODULE] = Factory.__module__
        self.env[Factory.UMADB_URI] = DEFAULT_LOCAL_UMADB_URI
        super().setUp()

    def tearDown(self) -> None:
        if Factory.UMADB_URI in os.environ:
            del os.environ[Factory.UMADB_URI]
        super().tearDown()


del InfrastructureFactoryTestCase
