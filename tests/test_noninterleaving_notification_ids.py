# -*- coding: utf-8 -*-
from unittest import skip

from eventsourcing.persistence import ApplicationRecorder
from eventsourcing.tests.persistence import NonInterleavingNotificationIDsBaseCase
from umadb import Client

from eventsourcing_umadb.recorders import UmaDbApplicationRecorder

DEFAULT_LOCAL_UMADB_URI = "http://127.0.0.1:50051"


# @skip("This is still a bit flakey - not sure why")
class TestNonInterleaving(NonInterleavingNotificationIDsBaseCase):
    insert_num = 1000

    def setUp(self) -> None:
        super().setUp()
        self.umadb = Client(DEFAULT_LOCAL_UMADB_URI)

    def create_recorder(self) -> ApplicationRecorder:
        return UmaDbApplicationRecorder(umadb=self.umadb)


del NonInterleavingNotificationIDsBaseCase
