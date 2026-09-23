"""Member A: sequential modifying writes with atomic predecessor evidence."""
from pymongo import ReadPreference, ReturnDocument
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from experiments.common import execute_operation, utc_now
from experiments.configs import session_scope


def history_entry(operation_id, seq):
    return {'client_seq': seq, 'version': seq, 'depends_on': seq - 1,
            'operation_id': operation_id, 'writer': 'client-A'}


def classify_trial(w1, w2, audit):
    """Conservative classification: rollback requires external evidence."""
    if w1 is None or w2 is None:
        return 'inconclusive', 'Both sequential workload writes were not completed'
    if any(w['status'] != 'success' for w in (w1, w2)):
        return 'inconclusive', 'Write failure/timeout; write effect may be unknown'
    before1, before2 = w1.get('returned_document'), w2.get('returned_document')
    if before1 is None or before2 is None:
        return 'inconclusive', 'A write did not match the trial document (no-op excluded)'
    if (before1.get('version') != 0 or before1.get('client_seq') != 0
            or before1.get('history') != []):
        return 'invalid_trial', 'W1 did not start from the registered initial state'
    expected1 = history_entry(w1['operation_id'], 1)
    if (before2.get('version') != 1 or before2.get('client_seq') != 1
            or before2.get('history') != [expected1]):
        return 'candidate_violation', 'W2 preimage lacks W1; investigate rollback before an MW claim'
    if audit is None or audit['status'] != 'success':
        return 'inconclusive', 'Predecessor evidence is ordered but final audit was unavailable'
    final = audit.get('returned_document')
    expected2 = history_entry(w2['operation_id'], 2)
    if (not final or final.get('version') != 2 or final.get('client_seq') != 2
            or final.get('history') != [expected1, expected2]):
        return 'candidate_violation', 'Final Primary audit differs from ordered history; review trace/rollback'
    return 'no_violation', 'W2 preimage contains W1 and final Primary history is [1,2]'


def run_trial(client, writer, config, *, experiment_id, trial_number,
              database, timeout_ms, retry_writes):
    trial_id = f'{experiment_id}-mw-{config.config_id}-normal-trial-{trial_number:04d}'
    meta = {**config.to_log_record(), 'schema_version': 1,
            'experiment_id': experiment_id, 'trial_id': trial_id,
            'experiment': 'mw', 'scenario': 'normal', 'client_id': 'client-A',
            'document_id': trial_id, 'retry_writes': retry_writes,
            'retry_reads': False, 'timeout_ms': timeout_ms}
    base = client[database]['mw_documents']
    options = config.collection_options()
    options['write_concern'] = WriteConcern(w=config.write_concern_w, wtimeout=timeout_ms)
    workload = base.with_options(**options)
    # Setup is explicitly stronger, outside the causal workload/timing.
    setup = base.with_options(write_concern=WriteConcern(w=3, wtimeout=timeout_ms))
    initial = {'_id': trial_id, 'experiment_id': experiment_id, 'version': 0,
               'value': 'initial', 'writer': 'client-A', 'client_seq': 0,
               'depends_on': None, 'history': []}
    init = execute_operation(client, writer, meta, 'initialize',
                             lambda: setup.insert_one(initial), phase='setup',
                             written_version=0, effective_options={'write_concern': 3})
    w1 = w2 = observed = audit = None
    if init['status'] == 'success':
        with session_scope(client, config) as session:
            for seq in (1, 2):
                name = f'write_{seq}'
                entry = history_entry(f'{trial_id}:{name}', seq)
                update = {'$set': {'version': seq, 'client_seq': seq,
                                   'depends_on': seq - 1, 'value': f'value-{seq}'},
                          '$push': {'history': entry}}
                row = execute_operation(
                    client, writer, meta, name,
                    lambda update=update: workload.find_one_and_update(
                        {'_id': trial_id}, update, upsert=False,
                        return_document=ReturnDocument.BEFORE, session=session),
                    session=session, written_version=seq, returned_document=True,
                    effective_options={'write_concern': config.write_concern_w,
                                       'read_preference': 'primary (write command)',
                                       'preimage': True})
                if seq == 1:
                    w1 = row
                else:
                    w2 = row
                if row['status'] != 'success' or row['returned_document'] is None:
                    break
            if w2 and w2['status'] == 'success':
                observed = execute_operation(
                    client, writer, meta, 'configured_read',
                    lambda: workload.find_one({'_id': trial_id}, session=session),
                    phase='observation', session=session, returned_document=True,
                    effective_options={'read_concern': config.read_concern_level,
                                       'read_preference': config.read_preference_name})
        # Independent audit cannot advance the workload's session or influence W2.
        verifier = base.with_options(read_preference=ReadPreference.PRIMARY,
                                     read_concern=ReadConcern('local'))
        audit = execute_operation(
            client, writer, meta, 'primary_audit',
            lambda: verifier.find_one({'_id': trial_id}), phase='audit',
            returned_document=True,
            effective_options={'read_concern': 'local', 'read_preference': 'primary'})
    classification, reason = classify_trial(w1, w2, audit)
    observation_stale = None
    if observed and observed['status'] == 'success':
        doc = observed.get('returned_document')
        observation_stale = doc is None or doc.get('version', -1) < 2
    result = {**meta, 'record_type': 'trial_result', 'ended_at': utc_now(),
              'classification': classification, 'reason': reason,
              'is_violation': False if classification == 'no_violation' else None,
              'candidate_violation': classification == 'candidate_violation',
              'rollback': 'not_assessed', 'setup_status': init['status'],
              'write_1_status': w1['status'] if w1 else 'not_run',
              'write_2_status': w2['status'] if w2 else 'not_run',
              'observation_status': observed['status'] if observed else 'not_run',
              'observation_stale': observation_stale,
              'audit_status': audit['status'] if audit else 'not_run',
              'evidence_operation_ids': [r['operation_id'] for r in (w1, w2, audit) if r]}
    writer.write(result)
    return result
