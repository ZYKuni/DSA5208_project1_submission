"""Canonical read/write/session configurations for the experiment matrix.

The required matrix contains C1-C4. C5-C6 are optional diagnostic
configurations based on MongoDB's partial causal-consistency guarantee table.
Experiments should use :func:`session_scope` so that only configurations which
explicitly request a causal session receive one. PyMongo may still use implicit
sessions in C1-C3; those are not explicit causal sessions.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Iterator, Literal, Mapping

from pymongo import MongoClient, ReadPreference
from pymongo.client_session import ClientSession
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

ReadConcernLevel = Literal["local", "majority"]
ReadPreferenceName = Literal["primary", "secondaryPreferred"]
WriteConcernValue = int | Literal["majority"]

_READ_PREFERENCES = {
    "primary": ReadPreference.PRIMARY,
    "secondaryPreferred": ReadPreference.SECONDARY_PREFERRED,
}


@dataclass(frozen=True, slots=True)
class ConsistencyConfig:
    """Serializable settings for one consistency experiment configuration."""

    config_id: str
    name: str
    read_concern_level: ReadConcernLevel
    write_concern_w: WriteConcernValue
    read_preference_name: ReadPreferenceName
    causal_session: bool
    purpose: str

    @property
    def read_concern(self) -> ReadConcern:
        """Return the PyMongo read-concern object for this configuration."""

        return ReadConcern(self.read_concern_level)

    @property
    def write_concern(self) -> WriteConcern:
        """Return the PyMongo write-concern object for this configuration."""

        return WriteConcern(w=self.write_concern_w)

    @property
    def read_preference(self) -> Any:
        """Return the PyMongo read-preference object for this configuration."""

        return _READ_PREFERENCES[self.read_preference_name]

    def collection_options(self) -> dict[str, Any]:
        """Return keyword arguments accepted by ``Collection.with_options``."""

        return {
            "read_concern": self.read_concern,
            "write_concern": self.write_concern,
            "read_preference": self.read_preference,
        }

    def to_log_record(self) -> dict[str, object]:
        """Return stable scalar values suitable for CSV or JSONL metadata."""

        return {
            "config_id": self.config_id,
            "config_name": self.name,
            "read_concern": self.read_concern_level,
            "write_concern": self.write_concern_w,
            "read_preference": self.read_preference_name,
            "causal_session": self.causal_session,
        }


REQUIRED_CONFIGS: Mapping[str, ConsistencyConfig] = MappingProxyType(
    {
        "C1": ConsistencyConfig(
            config_id="C1",
            name="weak-secondary",
            read_concern_level="local",
            write_concern_w=1,
            read_preference_name="secondaryPreferred",
            causal_session=False,
            purpose="Expose stale secondary reads under the weakest explicit settings.",
        ),
        "C2": ConsistencyConfig(
            config_id="C2",
            name="primary-local",
            read_concern_level="local",
            write_concern_w=1,
            read_preference_name="primary",
            causal_session=False,
            purpose="Isolate the effect of routing all reads to the current primary.",
        ),
        "C3": ConsistencyConfig(
            config_id="C3",
            name="majority-no-session",
            read_concern_level="majority",
            write_concern_w="majority",
            read_preference_name="secondaryPreferred",
            causal_session=False,
            purpose="Measure durability and visibility without client causal ordering.",
        ),
        "C4": ConsistencyConfig(
            config_id="C4",
            name="majority-causal-session",
            read_concern_level="majority",
            write_concern_w="majority",
            read_preference_name="secondaryPreferred",
            causal_session=True,
            purpose="Provide all four durable causal-consistency guarantees.",
        ),
    }
)

# Default experiment matrix used by runners unless optional diagnostics are
# explicitly requested.
CONFIGS: Mapping[str, ConsistencyConfig] = REQUIRED_CONFIGS


OPTIONAL_CONFIGS: Mapping[str, ConsistencyConfig] = MappingProxyType(
    {
        "C5": ConsistencyConfig(
            config_id="C5",
            name="causal-majority-read-w1",
            read_concern_level="majority",
            write_concern_w=1,
            read_preference_name="secondaryPreferred",
            causal_session=True,
            purpose=(
                "Exercise MongoDB's partial monotonic-read and "
                "writes-follow-reads guarantees."
            ),
        ),
        "C6": ConsistencyConfig(
            config_id="C6",
            name="causal-local-read-majority-write",
            read_concern_level="local",
            write_concern_w="majority",
            read_preference_name="secondaryPreferred",
            causal_session=True,
            purpose="Exercise MongoDB's partial monotonic-write guarantee.",
        ),
    }
)


ALL_CONFIGS: Mapping[str, ConsistencyConfig] = MappingProxyType(
    {**REQUIRED_CONFIGS, **OPTIONAL_CONFIGS}
)


def get_config(config_id: str) -> ConsistencyConfig:
    """Return a configuration by ID, accepting case-insensitive input."""

    normalized_id = config_id.strip().upper()
    try:
        return ALL_CONFIGS[normalized_id]
    except KeyError as error:
        valid_ids = ", ".join(ALL_CONFIGS)
        message = f"Unknown configuration {config_id!r}; choose one of {valid_ids}."
        raise ValueError(message) from error


def iter_configs(*, include_optional: bool = False) -> tuple[ConsistencyConfig, ...]:
    """Return the deterministic experiment order for the requested matrix."""

    configs = ALL_CONFIGS if include_optional else REQUIRED_CONFIGS
    return tuple(configs.values())


@contextmanager
def session_scope(
    client: MongoClient[Any], config: ConsistencyConfig
) -> Iterator[ClientSession | None]:
    """Yield one causal session for C4-C6, otherwise yield ``None``.

    Every causally related operation must receive the yielded ``session``
    argument. Do not open a new session between operations in the same trial.
    """

    if not config.causal_session:
        yield None
        return

    with client.start_session(causal_consistency=True) as session:
        yield session
