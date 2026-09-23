"""Check tagged secondary routing before MR fault experiments."""

import os
from uuid import uuid4

from pymongo import MongoClient
from pymongo.read_preferences import Secondary
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

# Support both `python -m experiments...` and historical direct scripts.
import sys
from pathlib import Path
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.common import NodeRecorder


recorder = NodeRecorder()

with MongoClient(
    os.environ["MONGODB_URI"],
    serverSelectionTimeoutMS=5000,
    timeoutMS=10000,
    retryReads=False,
    retryWrites=False,
    event_listeners=[recorder],
) as client:
    hello = client.admin.command("hello")
    print("Primary:", hello.get("primary"))

    if hello.get("primary") != "mongo1:27017":
        raise RuntimeError(
            "This preflight expects mongo1 as primary. "
            "Inspect current roles before continuing."
        )

    collection = client["zhou_jiahao_mr_preflight"]["routing"]
    document_id = f"routing-{uuid4().hex}"

    # Establish the document on all three nodes before checking routing.
    collection.with_options(
        write_concern=WriteConcern(w=3, wtimeout=5000)
    ).insert_one({"_id": document_id, "version": 1})

    # One client and one session span both reads.
    with client.start_session(causal_consistency=True) as session:
        for target in ("mongo2", "mongo3"):
            target_collection = collection.with_options(
                read_preference=Secondary(
                    tag_sets=[{"target": target}]
                ),
                read_concern=ReadConcern("majority"),
            )

            recorder.events.clear()
            document = target_collection.find_one(
                {"_id": document_id},
                session=session,
                max_time_ms=5000,
            )

            destinations = [
                event["node"]
                for event in recorder.events
                if event["command"] == "find"
            ]

            print(f"Target: {target}")
            print(f"Actual nodes: {destinations}")
            print(f"Document: {document}")

            assert document is not None
            assert document["version"] == 1
            assert destinations == [f"{target}:27017"]

    print("PASS: tagged reads reached both intended secondaries")
