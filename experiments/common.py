"""Shared operation logging; both legacy and trial-schema interfaces are supported."""
from __future__ import annotations

import contextvars
import json
from pathlib import Path
from typing import Any, Callable

from bson import json_util
from pymongo import MongoClient, monitoring
from pymongo.errors import PyMongoError

import time
from datetime import datetime, timezone

from pymongo.errors import (
    ConnectionFailure,
    NotPrimaryError,
    WriteConcernError,
)
from pymongo.monitoring import CommandListener


def utc_now():
    """UTC timestamp in the schema's required Z format."""
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


class NodeRecorder(CommandListener):
    """Record command destinations for sequential experiments."""

    def __init__(self):
        self.events = []

    def started(self, event):
        if event.command_name not in ("insert", "find", "update"):
            return

        host, port = event.connection_id
        self.events.append({
            "command": event.command_name,
            "node": f"{host}:{port}",
            "request_id": event.request_id,
        })

    def succeeded(self, event):
        pass

    def failed(self, event):
        pass


def classify_error(error):
    """Separate timeouts, availability failures, and other errors."""
    if getattr(error, "timeout", False):
        return "timeout"

    if isinstance(
        error,
        (ConnectionFailure, NotPrimaryError, WriteConcernError),
    ):
        return "unavailable"

    return "error"


def config_snapshot(config):
    """Convert the existing configuration into schema v1 format."""
    return {
        "id": config.config_id,
        "read_concern": config.read_concern_level,
        "write_concern": config.write_concern_w,
        "read_preference": config.read_preference_name,
        "causal_session": config.causal_session,
        "retry_reads": False,
        "retry_writes": False,
    }


def record_operation(
    operations,
    recorder,
    *,
    stage,
    operation,
    document_id,
    action,
    requested_version=None,
    client_seq=None,
):
    """Execute one operation and retain its trace, including on failure.

    action must be a callable with no arguments.
    Reads return a document or None; writes return a PyMongo result.
    Exceptions are recorded and then re-raised for the trial runner.
    """
    recorder.events.clear()

    entry = {
        "sequence": len(operations) + 1,
        "stage": stage,
        "operation": operation,
        "status": "success",
        "node": None,
        "document_id": document_id,
        "version": None,
        "client_seq": client_seq,
        "started_at": utc_now(),
        "completed_at": None,
        "latency_ms": 0.0,
        "error": None,
        "command_events": [],
    }

    started = time.perf_counter()

    try:
        result = action()

        if operation in ("read", "observe"):
            if result is not None:
                version = result["version"]
                if type(version) is not int or version < 0:
                    raise ValueError("Expected a nonnegative integer version")
                entry["version"] = version
        else:
            entry["version"] = requested_version

        return result

    except Exception as error:
        entry["status"] = classify_error(error)
        entry["error"] = {
            "type": type(error).__name__,
            "message": str(error) or type(error).__name__,
            "stage": stage,
        }
        raise

    finally:
        entry["latency_ms"] = round(
            (time.perf_counter() - started) * 1000,
            3,
        )
        entry["completed_at"] = utc_now()
        entry["command_events"] = [
            dict(event) for event in recorder.events
        ]

        nodes = {event["node"] for event in entry["command_events"]}
        if len(nodes) == 1:
            entry["node"] = next(iter(nodes))

        operations.append(entry)

DEFAULT_URI = 'mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0'
_attempts = contextvars.ContextVar('operation_attempts', default=None)


def json_safe(value: Any):
    """Preserve BSON timestamps and binary session IDs as Extended JSON."""
    return json.loads(json_util.dumps(value))


def node_name(address):
    return f'{address[0]}:{address[1]}' if address else None


class JsonlWriter:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.file = path.open('x', encoding='utf-8')

    def write(self, row):
        self.file.write(json.dumps(json_safe(row), ensure_ascii=False) + '\n')
        self.file.flush()

    def close(self):
        self.file.close()


class CommandRecorder(monitoring.CommandListener):
    """Capture real destinations/attempts without adding reads to timed work."""
    def started(self, event):
        attempts = _attempts.get()
        if attempts is None:
            return
        cmd = event.command
        attempts.append({
            'attempt': len(attempts) + 1, 'request_id': event.request_id,
            'driver_operation_id': event.operation_id, 'command_name': event.command_name,
            'target_node': node_name(event.connection_id), 'started_at': utc_now(),
            'wire_session_id': cmd.get('lsid'), 'txn_number': cmd.get('txnNumber'),
            'wire_write_concern': cmd.get('writeConcern'),
            'wire_read_concern': cmd.get('readConcern'),
            'wire_read_preference': cmd.get('$readPreference'), 'status': 'started',
        })

    def _finish(self, event, status, error=None):
        attempts = _attempts.get()
        if attempts is None:
            return
        for attempt in reversed(attempts):
            if attempt['request_id'] == event.request_id and attempt['status'] == 'started':
                attempt.update(status=status, ended_at=utc_now(),
                               latency_ms=event.duration_micros / 1000, error=error)
                if status == 'success':
                    attempt['operation_time'] = event.reply.get('operationTime')
                    attempt['cluster_time'] = event.reply.get('$clusterTime')
                    attempt['write_concern_error'] = event.reply.get('writeConcernError')
                    attempt['write_errors'] = event.reply.get('writeErrors')
                break

    def succeeded(self, event):
        self._finish(event, 'success')

    def failed(self, event):
        self._finish(event, 'failure', event.failure)


def make_client(uri=DEFAULT_URI, *, timeout_ms=10000, retry_writes=True, direct=False):
    return MongoClient(
        uri, appname='dsa5208-experiments', event_listeners=[CommandRecorder()],
        timeoutMS=timeout_ms, serverSelectionTimeoutMS=timeout_ms,
        connectTimeoutMS=min(timeout_ms, 5000), retryWrites=retry_writes,
        retryReads=False, directConnection=direct,
    )


def known_primary(client):
    # Cached SDAM snapshot: hello here would add a database operation to the trial.
    return node_name(next((d.address for d in
        client.topology_description.server_descriptions().values()
        if d.server_type_name == 'RSPrimary'), None))


def execute_operation(client, writer, metadata, operation, action: Callable, *,
                      phase='workload', session=None, written_version=None,
                      effective_options=None, returned_document=False):
    attempts = []
    token = _attempts.set(attempts)
    row = {
        **metadata, 'record_type': 'operation', 'phase': phase, 'operation': operation,
        'operation_id': f"{metadata['trial_id']}:{operation}",
        'session_id': session.session_id if session else None,
        'explicit_causal_session': bool(session and session.options.causal_consistency),
        'written_version': written_version, 'returned_version': None,
        'started_at': utc_now(), 'primary_at_start': known_primary(client),
        'effective_options': effective_options, 'is_violation': None,
        'candidate_violation': False, 'classification': 'pending_trial_assessment',
        'error': None, 'error_type': None, 'error_code': None, 'error_details': None,
        'write_effect': 'not_applicable',
    }
    row['monotonic_started_ns'] = time.monotonic_ns()
    start_ns = time.perf_counter_ns()
    try:
        result = action()
        row['status'] = 'success'
        if returned_document:
            row['returned_document'] = result
            row['returned_version'] = result.get('version') if result else None
        if written_version is not None:
            row['write_effect'] = ('no_match' if returned_document and result is None
                                   else 'acknowledged')
    except PyMongoError as error:
        row.update(status='timeout' if error.timeout else 'failure', error=str(error),
                   error_type=type(error).__name__, error_code=getattr(error, 'code', None),
                   error_details=getattr(error, 'details', None))
        if written_version is not None:
            row['write_effect'] = 'unknown'
    finally:
        row['latency_ms'] = (time.perf_counter_ns() - start_ns) / 1_000_000
        row['ended_at'] = utc_now()
        row['monotonic_ended_ns'] = time.monotonic_ns()
        row['primary_at_end'] = known_primary(client)
        row['attempts'] = attempts
        row['attempt_count'] = len(attempts)
        row['retry_count'] = max(0, len(attempts) - 1)
        row['target_node'] = attempts[-1]['target_node'] if attempts else None
        _attempts.reset(token)
    writer.write(row)
    return row


def cluster_snapshot(client):
    hello = client.admin.command('hello')
    status = client.admin.command('replSetGetStatus')
    config = client.admin.command('replSetGetConfig')['config']
    return json_safe({
        'captured_at': utc_now(), 'hello': hello, 'status': status,
        'replica_set_config': config,
        'mongodb_version': client.admin.command('buildInfo')['version'],
        'discovered_nodes': sorted(node_name(n) for n in client.nodes),
    })


def require_healthy(snapshot):
    members = snapshot['status']['members']
    if (len(members) != 3 or sum(m['stateStr'] == 'PRIMARY' for m in members) != 1
            or sum(m['stateStr'] == 'SECONDARY' for m in members) != 2
            or not all(m['health'] == 1 for m in members)):
        raise RuntimeError('Expected three healthy nodes: one PRIMARY and two SECONDARY')
