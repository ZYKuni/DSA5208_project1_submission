"""Fault evidence, controller handshake and conservative rollback classification."""
from __future__ import annotations
import json
import os
import threading
import time
import uuid
from pathlib import Path

import pymongo
from bson import decode_all
from pymongo import ReadPreference
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern

from experiments.common import JsonlWriter, json_safe, utc_now, cluster_snapshot, require_healthy
from experiments.test_monotonic_writes import classify_trial

NODES = ('mongo1','mongo2','mongo3')


class Events:
    def __init__(self, path):
        self.writer = JsonlWriter(path)
        self.lock = threading.Lock()

    def emit(self, event, **fields):
        with self.lock:
            self.writer.write({'event':event,'time':utc_now(), 'monotonic_ns':time.monotonic_ns(), **fields})

    def close(self):
        self.writer.close()


class Controller:
    def __init__(self, events):
        self.path = Path(os.environ['FAULT_CONTROL'])
        self.events = events

    def request(self, action, node, **fields):
        request_id = uuid.uuid4().hex
        request = {'action':action,'node':node,**fields}
        target = self.path / (request_id + '.request.json')
        temp = target.with_suffix('.tmp')
        self.events.emit('controller_request', request_id=request_id, **request)
        temp.write_text(json.dumps(request))
        temp.replace(target)
        reply = self.path / (request_id + '.response.json')
        deadline = time.monotonic() + 150
        while not reply.exists():
            if time.monotonic() >= deadline:
                raise TimeoutError('Host fault controller did not respond')
            time.sleep(0.02)
        result = json.loads(reply.read_text())
        self.events.emit('controller_response', request_id=request_id, **result)
        if not result['ok']:
            raise RuntimeError(result['error'])
        return result


def observer(node, timeout_ms=700):
    # Per-node client-only network remains reachable during replication isolation.
    return pymongo.MongoClient(f'mongodb://observe-{node}:27017/', directConnection=True,
        timeoutMS=timeout_ms, serverSelectionTimeoutMS=timeout_ms, connectTimeoutMS=timeout_ms,
        retryWrites=False, retryReads=False, appname='dsa5208-fault-observer')


def wait_healthy(client, seconds=90):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            with pymongo.timeout(2):
                status = client.admin.command('replSetGetStatus')
            require_healthy({'status':status})
            return cluster_snapshot(client)
        except (PyMongoError, RuntimeError) as error:
            last = str(error)
            time.sleep(0.2)
    raise RuntimeError(f'Fault lab did not recover: {last}')


def evidence_on_node(node, database, trial_id):
    with observer(node, 5000) as client:
        collection = client[database]['mw_documents'].with_options(
            read_preference=ReadPreference.SECONDARY_PREFERRED, read_concern=ReadConcern('local'))
        oplog = client.local['oplog.rs'].with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)
        return json_safe({'node':node,'captured_at':utc_now(),
            'hello':client.admin.command('hello'), 'rbid':client.admin.command('replSetGetRBID')['rbid'],
            'document':collection.find_one({'_id':trial_id}),
            'oplog':list(oplog.find({'ns':database+'.mw_documents', '$or':[
                {'o._id':trial_id}, {'o2._id':trial_id}]}).sort('$natural', -1).limit(6))})


class ElectionObserver:
    def __init__(self, old_primary, events, trial_id):
        self.old_primary = old_primary
        self.events = events
        self.trial_id = trial_id
        self.result = None
        self.stopped = threading.Event()
        self.started_ns = time.perf_counter_ns()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def _run(self):
        clients = {node:observer(node, 300) for node in NODES if node != self.old_primary}
        try:
            while not self.stopped.is_set():
                for node, client in clients.items():
                    try:
                        hello = client.admin.command('hello')
                        if hello.get('isWritablePrimary'):
                            self.result = {'node':node,'observed_at':utc_now(),
                                'observed_delay_ms':(time.perf_counter_ns()-self.started_ns)/1e6,
                                'election_id':json_safe(hello.get('electionId'))}
                            self.events.emit('new_primary_observed', trial_id=self.trial_id, **self.result)
                            return
                    except PyMongoError:
                        pass
                self.stopped.wait(0.1)
        finally:
            for client in clients.values():
                client.close()

    def finish(self, wait_seconds=0):
        if wait_seconds:
            self.thread.join(timeout=wait_seconds)
        self.stopped.set()
        self.thread.join(timeout=2)
        return self.result


def history_has(document, operation_id):
    return bool(document and any(h.get('operation_id') == operation_id for h in document.get('history', [])))


def read_rollback_evidence(root, collected, trial_id, operation_id):
    matches = []
    for name in collected.get('rollback_files', []):
        for document in decode_all((root / name).read_bytes()):
            if document.get('_id') == trial_id:
                matches.append({'file':name,'document':json_safe(document),
                                'contains_w1':history_has(document, operation_id)})
    log = (root / collected['log_path']).read_text(errors='replace')
    relevant = [line for line in log.splitlines() if 'rollback' in line.lower()]
    completed = any(any(marker in line.lower() for marker in (
        'rollback complete', 'rollback finished', 'rollback successful')) for line in relevant)
    return {'matching_rollback_documents':matches, 'rollback_completed_log':completed,
            'rollback_log_lines':relevant}


def assess_fault(w1, w2, audit, before_crash, after_recovery, rollback_evidence):
    base, reason = classify_trial(w1, w2, audit)
    operation_id = w1['operation_id'] if w1 else None
    acknowledged = bool(w1 and w1['status']=='success' and w1.get('returned_document') is not None)
    survived = (history_has(after_recovery.get('document'), operation_id)
                if acknowledged and after_recovery else None)
    proof = rollback_evidence or {}
    confirmed = bool(acknowledged and before_crash and after_recovery
        and history_has(before_crash.get('document'), operation_id)
        and not survived and proof.get('rollback_completed_log')
        and any(d['contains_w1'] for d in proof.get('matching_rollback_documents', [])))
    # RBID can also increase after an unclean shutdown. It is supplementary only.
    if confirmed:
        return {'classification':'rollback_observed',
                'reason':'Acknowledged W1 preserved in rollback BSON, rollback completion logged, W1 absent after recovery',
                'rollback':'confirmed','write_1_survived':False,
                'mw_candidate_before_rollback_review':base=='candidate_violation',
                'candidate_violation':False,'is_violation':None}
    return {'classification':base, 'reason':reason,
            'rollback':'not_observed_for_trial' if survived else 'not_confirmed',
            'write_1_survived':survived,
            'mw_candidate_before_rollback_review':base=='candidate_violation',
            'candidate_violation':base=='candidate_violation',
            'is_violation':False if base=='no_violation' else None}


def documents_converged(samples, expected_document):
    """Require direct self-reported roles and the new branch's actual document."""
    if len(samples) != 3 or any(s.get('error') for s in samples):
        return False
    roles = [s['role'] for s in samples]
    return (roles.count('PRIMARY') == 1 and roles.count('SECONDARY') == 2
            and all(s['document'] == expected_document for s in samples))


def wait_document_convergence(client, database, trial_id, expected_document, seconds=90):
    deadline = time.monotonic() + seconds
    stable_since = None
    samples = []
    clients = {node:observer(node, 700) for node in NODES}
    try:
        while time.monotonic() < deadline:
            samples = []
            for node, direct in clients.items():
                try:
                    hello = direct.admin.command('hello')
                    role = ('PRIMARY' if hello.get('isWritablePrimary') else
                            'SECONDARY' if hello.get('secondary') else 'TRANSITION')
                    doc = direct[database]['mw_documents'].with_options(
                        read_preference=ReadPreference.SECONDARY_PREFERRED,
                        read_concern=ReadConcern('local')).find_one({'_id':trial_id})
                    samples.append({'node':node,'role':role,'document':json_safe(doc)})
                except PyMongoError as error:
                    samples.append({'node':node,'error':str(error)})
            if documents_converged(samples, json_safe(expected_document)):
                if stable_since is None:
                    stable_since = time.monotonic()
                if time.monotonic() - stable_since >= 1:
                    return {'converged_at':utc_now(),'stable_seconds':1,'nodes':samples}
            else:
                stable_since = None
            time.sleep(.1)
    finally:
        for direct in clients.values():
            direct.close()
    raise RuntimeError(f'Trial document did not converge on all direct nodes: {json_safe(samples)}')
