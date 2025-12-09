# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, List, Optional, Sequence, cast
from uuid import UUID, uuid4

import umadb
from eventsourcing.dcb.api import (
    DCBAppendCondition,
    DCBEvent,
    DCBQuery,
    DCBReadResponse,
    DCBRecorder,
    DCBSequencedEvent,
    DCBSubscription,
)
from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    IntegrityError,
    Notification,
    StoredEvent,
    Subscription,
)


class UmaDBAggregateRecorder(AggregateRecorder):
    def __init__(
        self,
        umadb: umadb.Client,
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
        umadb_events: List[umadb.Event] = []
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
            umadb_event = umadb.Event(
                event_type=stored_event.topic,
                data=stored_event.state,
                tags=[originator_id_tag, originator_version_tag],
                uuid=str(uuid4()),
            )
            umadb_events.append(umadb_event)
        try:
            query_items = [
                umadb.QueryItem(
                    tags=umadb_event.tags,
                )
                for umadb_event in umadb_events
            ]
            # print("Query items:", query_items)
            sequence_number = self.umadb.append(
                events=umadb_events,
                condition=umadb.AppendCondition(
                    fail_if_events_match=umadb.Query(
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
            query=umadb.Query(
                items=[umadb.QueryItem(tags=[self._tag_originator_id(originator_id)])]
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

    def _extract_originator_version(self, ue: umadb.SequencedEvent) -> int:
        return int(ue.event.tags[1].split(":")[1])

    def _extract_originator_id(self, ue: umadb.SequencedEvent) -> UUID:
        try:
            return UUID(ue.event.tags[0].split(":")[1])
        except IndexError as e:
            msg = f"Couldn't extract originator ID from: {ue.event.tags[0]}"
            raise ValueError(msg) from e


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
            query=umadb.Query(items=[umadb.QueryItem(types=topics)]),
        )
        notifications: List[Notification] = []
        count = 0
        for ue in ues:
            notification = self.construct_notification(ue)
            notifications.append(notification)
            count += 1

            if stop is not None and stop <= ue.position:
                break

        return notifications

    def construct_notification(self, ue: umadb.SequencedEvent) -> Notification:
        return Notification(
            id=ue.position,
            originator_id=self._extract_originator_id(ue),
            originator_version=self._extract_originator_version(ue),
            topic=ue.event.event_type,
            state=ue.event.data,
        )

    def subscribe(
        self, gt: int | None = None, topics: Sequence[str] = ()
    ) -> Subscription[UmaDBApplicationRecorder]:
        return UmaDBSubscription(
            recorder=self,
            gt=gt,
            topics=topics,
        )


class UmaDBSubscription(Subscription[UmaDBApplicationRecorder]):
    def __init__(
        self,
        recorder: UmaDBApplicationRecorder,
        gt: int | None = None,
        topics: Sequence[str] = (),
    ) -> None:
        super().__init__(recorder=recorder, gt=gt, topics=topics)
        self._read_response = recorder.umadb.read(
            query=umadb.Query(items=[umadb.QueryItem(types=topics)]),
            start=gt + 1 if isinstance(gt, int) else None,
            subscribe=True,
        )

    def __next__(self) -> Notification:
        if self._has_been_stopped:
            raise StopIteration
        return self._recorder.construct_notification(next(self._read_response))


class UmaDBDCBRecorder(DCBRecorder):
    def __init__(self, umadb: umadb.Client):
        self.umadb = umadb

    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        try:
            return self.umadb.append(
                events=[
                    umadb.Event(
                        event_type=e.type,
                        data=e.data,
                        tags=e.tags,
                        uuid=str(uuid4()),
                    )
                    for e in events
                ],
                condition=(
                    umadb.AppendCondition(
                        fail_if_events_match=umadb.Query(
                            items=[
                                umadb.QueryItem(
                                    types=qi.types,
                                    tags=qi.tags,
                                )
                                for qi in condition.fail_if_events_match.items
                            ],
                        ),
                        after=condition.after,
                    )
                    if condition
                    else None
                ),
            )
        except umadb.IntegrityError as exc:
            raise IntegrityError(exc)

    def read(
        self,
        query: DCBQuery | None = None,
        *,
        after: int | None = None,
        limit: int | None = None,
    ) -> DCBReadResponse:
        r = self.umadb.read(
            (
                umadb.Query(
                    items=[
                        umadb.QueryItem(
                            types=qi.types,
                            tags=qi.tags,
                        )
                        for qi in query.items
                    ],
                )
                if query
                else None
            ),
            start=after + 1 if after else None,
            limit=limit,
        )
        return UmaDBDCBReadResponse(r)

    def subscribe(
        self,
        query: DCBQuery | None = None,
        *,
        after: int | None = None,
    ) -> DCBSubscription:
        return UmaDBDCBSubscription(
            recorder=self,
            query=query,
            after=after,
        )


class UmaDBDCBReadResponse(DCBReadResponse):
    def __init__(self, read_response: umadb.ReadResponse) -> None:
        self.read_response = read_response

    @property
    def head(self) -> int | None:
        return self.read_response.head()

    def __next__(self) -> DCBSequencedEvent:
        sequenced_event = next(self.read_response)
        return DCBSequencedEvent(
            position=sequenced_event.position,
            event=DCBEvent(
                type=sequenced_event.event.event_type,
                data=sequenced_event.event.data,
                tags=sequenced_event.event.tags,
            ),
        )


class UmaDBDCBSubscription(DCBSubscription):
    def __init__(
        self,
        recorder: DCBRecorder,
        query: DCBQuery | None = None,
        after: int | None = None,
    ) -> None:
        super().__init__(
            recorder=recorder,
            query=query,
            after=after,
        )
        self._read_response = UmaDBDCBReadResponse(
            cast(UmaDBDCBRecorder, self._recorder).umadb.read(
                (
                    umadb.Query(
                        items=[
                            umadb.QueryItem(
                                types=qi.types,
                                tags=qi.tags,
                            )
                            for qi in query.items
                        ],
                    )
                    if query
                    else None
                ),
                start=after + 1 if after else None,
                subscribe=True,
            )
        )

    def __next__(self) -> DCBSequencedEvent:
        return next(self._read_response)
