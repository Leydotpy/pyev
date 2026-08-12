from __future__ import annotations

from dataclasses import dataclass

import pytest

from pymq.event import EventRegistry, event, get_event_metadata
from pymq.exceptions import (
    DuplicateRegistrationError,
    EventRegistrationError,
    MessageValidationError,
)


def test_event_declaration_and_exact_reconstruction() -> None:
    registry = EventRegistry()

    @event("tests.user.created", version=1, registry=registry)
    @dataclass(frozen=True, slots=True)
    class UserCreated:
        user_id: int
        name: str

    metadata = get_event_metadata(UserCreated)
    assert metadata.name == "tests.user.created"
    assert metadata.version == 1
    assert registry.resolve(metadata.name, metadata.version) is UserCreated
    assert registry.reconstruct(
        "tests.user.created", 1, {"user_id": 7, "name": "Ada"}
    ) == UserCreated(7, "Ada")


def test_upcasting_is_explicit_and_can_chain() -> None:
    registry = EventRegistry()

    @event("tests.account.opened", version=1, registry=registry)
    @dataclass(frozen=True)
    class OpenedV1:
        owner: str

    @event("tests.account.opened", version=2, registry=registry)
    @dataclass(frozen=True)
    class OpenedV2:
        owner: str
        currency: str

    @event("tests.account.opened", version=3, registry=registry)
    @dataclass(frozen=True)
    class OpenedV3:
        owner: str
        currency: str
        active: bool

    registry.register_upcaster(
        "tests.account.opened",
        1,
        2,
        lambda payload: {**payload, "currency": "USD"},
    )
    registry.register_upcaster(
        "tests.account.opened",
        2,
        3,
        lambda payload: {**payload, "active": True},
    )

    assert registry.reconstruct("tests.account.opened", 1, {"owner": "A"}) == OpenedV1("A")
    assert registry.reconstruct(
        "tests.account.opened", 1, {"owner": "A"}, target_version=3
    ) == OpenedV3("A", "USD", True)


def test_registry_rejects_duplicate_schema_owners() -> None:
    registry = EventRegistry()

    @event("tests.duplicate", registry=registry)
    @dataclass
    class First:
        value: int

    del First

    with pytest.raises(DuplicateRegistrationError):

        @event("tests.duplicate", registry=registry)
        @dataclass
        class Second:
            value: int


def test_event_identity_validation() -> None:
    with pytest.raises(EventRegistrationError):
        event("not valid", version=1)
    with pytest.raises(EventRegistrationError):
        event("valid.name", version=0)


def test_undecorated_subclass_does_not_inherit_event_identity() -> None:
    registry = EventRegistry()

    @event("tests.base", registry=registry)
    @dataclass
    class BaseEvent:
        value: int

    class UndeclaredSubclass(BaseEvent):
        pass

    with pytest.raises(MessageValidationError):
        get_event_metadata(UndeclaredSubclass)
