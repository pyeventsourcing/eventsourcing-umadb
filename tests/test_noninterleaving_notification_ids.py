# -*- coding: utf-8 -*-
from eventsourcing.persistence import ApplicationRecorder
from eventsourcing.tests.persistence import NonInterleavingNotificationIDsBaseCase

from eventsourcing_umadb.recorders import UmaDbApplicationRecorder
from tests.test_recorders import WithUmaDb


# @skip("This is still a bit flakey - not sure why")
class TestNonInterleaving(WithUmaDb, NonInterleavingNotificationIDsBaseCase):
    insert_num = 1000

    def create_recorder(self) -> ApplicationRecorder:
        return UmaDbApplicationRecorder(umadb=self.umadb)


del NonInterleavingNotificationIDsBaseCase
