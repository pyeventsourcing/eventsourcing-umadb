# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Iterator, List, Optional, Sequence, cast
from uuid import UUID, uuid4

import umadb
from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    IntegrityError,
    Notification,
    StoredEvent,
    Subscription,
)
from umadb import AppendCondition, Client, Event, Query, QueryItem, SequencedEvent


class UmaDBAggregateRecorder(AggregateRecorder):
    def __init__(
        self,
        umadb: Client,
        for_snapshotting: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if for_snapshotting:
            raise NotImplementedError("Snapshotting is not supported")
        super(UmaDBAggregateRecorder, self).__init__(*args, **kwargs)
        self.umadb = umadb
        self.for_snapshotting = for_snapshotting

    def insert_events(
        self, stored_events: Sequence[StoredEvent], **kwargs: Any
    ) -> Optional[Sequence[int]]:
        self._insert_events(stored_events, **kwargs)
        return None

    def _insert_events(
        self, stored_events: Sequence[StoredEvent], **kwargs: Any
    ) -> Optional[Sequence[int]]:
        # print("Inserting events")
        # for stored_event in stored_events:
        #     print(" - {}, {}".format(stored_event.originator_id, stored_event.originator_version))
        umadb_events: List[Event] = []
        if len(stored_events) == 0:
            return None
        originator_ids_and_versions: dict[UUID | str, int] = dict()
        for stored_event in stored_events:
            if stored_event.originator_id in originator_ids_and_versions:
                last_version = originator_ids_and_versions[stored_event.originator_id]
                if stored_event.originator_version != last_version + 1:
                    raise IntegrityError()
                originator_ids_and_versions[stored_event.originator_id] = (
                    stored_event.originator_version
                )
            else:
                originator_ids_and_versions[stored_event.originator_id] = (
                    stored_event.originator_version
                )
            originator_id_tag = self._tag_originator_id(stored_event.originator_id)
            originator_version_tag = self._tag_originator_version(
                stored_event.originator_id, stored_event.originator_version
            )
            umadb_event = Event(
                event_type=stored_event.topic,
                data=stored_event.state,
                tags=[originator_id_tag, originator_version_tag],
                uuid=str(uuid4()),
            )
            umadb_events.append(umadb_event)
        try:
            query_items = [
                QueryItem(
                    tags=umadb_event.tags,
                )
                for umadb_event in umadb_events
            ]
            # print("Query items:", query_items)
            sequence_number = self.umadb.append(
                events=umadb_events,
                condition=AppendCondition(
                    fail_if_events_match=Query(
                        items=query_items,
                    ),
                ),
            )
            # print("Sequence number:", sequence_number)
        except umadb.IntegrityError as e:
            raise IntegrityError(e) from e
        else:
            return list(
                range(sequence_number - len(stored_events) + 1, sequence_number + 1)
            )

    def _tag_originator_id(self, originator_id: UUID | str) -> str:
        return f"originator:{originator_id}"

    def _tag_originator_version(
        self, originator_id: UUID | str, originator_version: int
    ) -> str:
        return f"originator-{originator_id}-version:{originator_version}"

    def select_events(
        self,
        originator_id: UUID | str,
        gt: Optional[int] = None,
        lte: Optional[int] = None,
        desc: bool = False,
        limit: Optional[int] = None,
    ) -> List[StoredEvent]:
        if self.for_snapshotting and desc and limit == 1:
            return []
        umadb_events = self.umadb.read(
            query=Query(
                items=[QueryItem(tags=[self._tag_originator_id(originator_id)])]
            ),
            backwards=desc,
        )

        stored_events: List[StoredEvent] = []
        for ue in umadb_events:
            extracted_originator_id = self._extract_originator_id(ue)
            extracted_originator_version = self._extract_originator_version(ue)
            if len(stored_events) == limit:
                break
            if gt is not None:
                if extracted_originator_version <= gt:
                    if desc:
                        break
                    else:
                        continue
            if lte is not None:
                if extracted_originator_version > lte:
                    if not desc:
                        break
                    else:
                        continue
            stored_events.append(
                StoredEvent(
                    originator_id=extracted_originator_id,
                    originator_version=extracted_originator_version,
                    topic=ue.event.event_type,
                    state=ue.event.data,
                )
            )
        return stored_events

    def _extract_originator_version(self, ue: SequencedEvent) -> int:
        return int(ue.event.tags[1].split(":")[1])

    def _extract_originator_id(self, ue: SequencedEvent) -> UUID:
        return UUID(ue.event.tags[0].split(":")[1])


class UmaDBApplicationRecorder(UmaDBAggregateRecorder, ApplicationRecorder):
    def max_notification_id(self) -> int | None:
        return self.umadb.head()

    def insert_events(
        self, stored_events: Sequence[StoredEvent], **kwargs: Any
    ) -> Optional[Sequence[int]]:
        return self._insert_events(stored_events, **kwargs)

    def select_notifications(
        self,
        start: int | None,
        limit: int,
        stop: int | None = None,
        topics: Sequence[str] = (),
        *,
        inclusive_of_start: bool = True,
    ) -> Sequence[Notification]:
        if not inclusive_of_start and start is not None:
            start += 1
        ues = self.umadb.read(
            start=start,
            limit=limit,
            query=Query(items=[QueryItem(types=topics)]),
        )
        notifications: List[Notification] = []
        count = 0
        for ue in ues:
            notification = Notification(
                id=ue.position,
                originator_id=self._extract_originator_id(ue),
                originator_version=self._extract_originator_version(ue),
                topic=ue.event.event_type,
                state=ue.event.data,
            )
            notifications.append(notification)
            count += 1

            if stop is not None and stop <= ue.position:
                break

        return notifications

    def subscribe(
        self, gt: int | None = None, topics: Sequence[str] = ()
    ) -> Subscription[ApplicationRecorder]:
        raise NotImplementedError()
