from __future__ import annotations

import pytest

from pyev.capabilities import Capability, CapabilitySet, CapabilitySpec
from pyev.exceptions import UnsupportedCapabilityError


def test_capability_set_supports_attributes_and_immutable_updates() -> None:
    capabilities = CapabilitySet(
        {
            Capability.PUBLISH_SUBSCRIBE: None,
            Capability.PARTITION_ORDERING: {"scope": "partition", "max": 8},
        }
    )

    assert capabilities.supports(Capability.PUBLISH_SUBSCRIBE)
    assert capabilities.supports(Capability.PARTITION_ORDERING, scope="partition")
    assert not capabilities.supports(Capability.PARTITION_ORDERING, scope="global")
    assert capabilities.attribute(Capability.PARTITION_ORDERING, "max") == 8

    extended = capabilities.with_capability(Capability.BATCH_PUBLISHING, max_batch=100)
    assert Capability.BATCH_PUBLISHING not in capabilities
    assert extended.supports(Capability.BATCH_PUBLISHING, max_batch=100)


def test_capability_set_requires_missing_semantics_explicitly() -> None:
    capabilities = CapabilitySet.of(
        CapabilitySpec(Capability.MESSAGE_ORDERING, {"scope": "partition"})
    )
    with pytest.raises(UnsupportedCapabilityError) as captured:
        capabilities.require(Capability.EXACTLY_ONCE, operation="publish")
    assert captured.value.operation == "publish"


def test_capability_diagnostic_mapping_is_detached() -> None:
    capabilities = CapabilitySet(
        {Capability.QUEUE_DEPTH: {"bounds": {"maximum": 100}, "units": ["messages"]}}
    )
    diagnostics = capabilities.to_dict()
    diagnostics[Capability.QUEUE_DEPTH.value]["bounds"] = {}
    assert capabilities.attribute(Capability.QUEUE_DEPTH, "bounds") == {"maximum": 100}


def test_attribute_requirements_are_normalized_before_comparison() -> None:
    capabilities = CapabilitySet({Capability.MESSAGE_ORDERING: {"scopes": ["partition", "key"]}})
    assert capabilities.supports(Capability.MESSAGE_ORDERING, scopes=["partition", "key"])
