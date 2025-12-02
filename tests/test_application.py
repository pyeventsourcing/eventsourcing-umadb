# -*- coding: utf-8 -*-
import os
from decimal import Decimal
from uuid import uuid4

from eventsourcing.domain import Aggregate
from eventsourcing.tests.application import ExampleApplicationTestCase
from eventsourcing.utils import get_topic

DEFAULT_LOCAL_UMADB_URI = "http://127.0.0.1:50051"


class TestApplicationWithUmaDB(ExampleApplicationTestCase):
    expected_factory_topic = "eventsourcing_umadb.factory:Factory"

    def setUp(self) -> None:
        self.original_initial_version = Aggregate.INITIAL_VERSION
        Aggregate.INITIAL_VERSION = 0
        super().setUp()
        os.environ["PERSISTENCE_MODULE"] = "eventsourcing_umadb"
        os.environ["UMADB_URI"] = DEFAULT_LOCAL_UMADB_URI

    def tearDown(self) -> None:
        Aggregate.INITIAL_VERSION = self.original_initial_version
        del os.environ["PERSISTENCE_MODULE"]
        del os.environ["UMADB_URI"]
        super().tearDown()

    def test_example_application(self) -> None:
        self.super_test_example_application()

    def super_test_example_application(self) -> None:
        from eventsourcing.tests.application import BankAccounts
        from eventsourcing.tests.domain import BankAccount

        # app = BankAccounts(env={"IS_SNAPSHOTTING_ENABLED": "y"})
        BankAccounts.is_snapshotting_enabled = False
        app = BankAccounts()

        self.assertEqual(get_topic(type(app.factory)), self.expected_factory_topic)

        # Check AccountNotFound exception.
        with self.assertRaises(BankAccounts.AccountNotFoundError):
            app.get_account(uuid4())

        # Open an account.
        account_id = app.open_account(
            full_name="Alice",
            email_address="alice@example.com",
        )

        # Check balance.
        self.assertEqual(
            app.get_balance(account_id),
            Decimal("0.00"),
        )

        # Credit the account.
        app.credit_account(account_id, Decimal("10.00"))

        # Check balance.
        self.assertEqual(
            app.get_balance(account_id),
            Decimal("10.00"),
        )

        app.credit_account(account_id, Decimal("25.00"))
        app.credit_account(account_id, Decimal("30.00"))

        # Check balance.
        self.assertEqual(
            app.get_balance(account_id),
            Decimal("65.00"),
        )

        # TODO: Maybe reinstate this somehow?
        # section = app.notification_log["1,10"]
        # self.assertEqual(len(section.items), 4)

        # # Take snapshot (specify version).
        # app.take_snapshot(account_id, version=Aggregate.INITIAL_VERSION + 1)
        #
        # assert app.snapshots is not None  # for mypy
        # snapshots = list(app.snapshots.get(account_id))
        # self.assertEqual(len(snapshots), 1)
        # self.assertEqual(snapshots[0].originator_version, Aggregate.INITIAL_VERSION + 1)

        # from_snapshot1: BankAccount = app.repository.get(
        #     account_id, version=Aggregate.INITIAL_VERSION + 2
        # )
        # self.assertIsInstance(from_snapshot1, BankAccount)
        # self.assertEqual(from_snapshot1.version, Aggregate.INITIAL_VERSION + 2)
        # self.assertEqual(from_snapshot1.balance, Decimal("35.00"))
        #
        # # Take snapshot (don't specify version).
        # app.take_snapshot(account_id)
        # assert app.snapshots is not None  # for mypy
        # snapshots = list(app.snapshots.get(account_id))
        # self.assertEqual(len(snapshots), 2)
        # self.assertEqual(snapshots[0].originator_version, Aggregate.INITIAL_VERSION + 1)
        # self.assertEqual(snapshots[1].originator_version, Aggregate.INITIAL_VERSION + 3)
        #
        # from_snapshot2: BankAccount = app.repository.get(account_id)
        # self.assertIsInstance(from_snapshot2, BankAccount)
        # self.assertEqual(from_snapshot2.version, Aggregate.INITIAL_VERSION + 3)
        # self.assertEqual(from_snapshot2.balance, Decimal("65.00"))


del ExampleApplicationTestCase
