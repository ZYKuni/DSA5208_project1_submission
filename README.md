# End-to-End Reproduction Guide

This package reproduces the MongoDB client-centric consistency experiments reported in `report.pdf`. It covers read-your-writes (RYW), monotonic reads (MR), monotonic writes (MW), and writes-follow-reads (WFR) under normal operation, Secondary loss, loss of a majority, Primary crash, and replication-network partition.

Project members:

- Zhao Yikun: deployment, fault orchestration, MW, and Secondary-loss experiments
- Zhou Jiahao: RYW and MR experiments
- Chen Zheke: WFR, unified analysis, and report integration

The guide has two tracks:

1. **Quick verification** checks the deployment and runs a small trial for each model.
2. **Full reproduction** repeats the report's principal sample sizes and regenerates the unified tables and figures.

Run all commands from the directory containing this README. Use fresh experiment IDs. Fault experiments must run serially.

## 1. What is in this package

```text
submission/
├── report.pdf
├── README.md
├── docker-compose.yml
├── docker-compose.mr.yml
├── docker-compose.client.yml
├── config/
│   └── init-replica-set.js
├── docker/
│   ├── fault-compose.yml
│   └── runner.Dockerfile
├── requirements-experiments.txt
├── requirements.txt
├── report/
│   └── result-schema-v1.json
├── experiments/
├── scripts/
├── tests/
├── analysis/
└── results/
    ├── archive/
    └── figures/
```

Generated runs are written beneath `results/raw/`, `results/pilot/`, `results/fault-control/`, `results/summary/`, and `results/figures/`. Existing output is never overwritten.

## 2. Supported environment

The recorded experiment environment used MongoDB 8.0.12, Python 3.12.14 in the runner, and PyMongo 4.17.0. The main deployment used Docker Engine 28.3.3 and Docker Compose 2.39.2 on Apple Silicon. The same workflow should run on recent macOS, Linux, or WSL2 with Docker Compose v2.

Required software:

- Docker Desktop or Docker Engine with `docker compose`
- Git
- Python 3.12 on the host (the executable may be supplied through `PYTHON312`)
- At least 5 GB of free disk space for a full run

Native Windows PowerShell is not supported by the host fault controller because it uses POSIX file locking. Use WSL2 instead.

Confirm the tools before continuing:

```sh
docker version
docker compose version
git --version
docker info
```

`docker info` must succeed. Start Docker Desktop if it does not.

Locate Python 3.12 and verify its version before creating an environment. The
first branch handles the usual command name. The second is the path used by the
recorded macOS/Miniforge host; replace it with an absolute path on another
machine if necessary.

```sh
if [ -n "${PYTHON312:-}" ]; then
  :
elif command -v python3.12 >/dev/null 2>&1; then
  PYTHON312=$(command -v python3.12)
elif [ -x "$HOME/miniforge3/bin/python3.12" ]; then
  PYTHON312="$HOME/miniforge3/bin/python3.12"
else
  echo "Python 3.12 was not found. Install it or export PYTHON312=/absolute/path/to/python3.12."
  exit 1
fi

"$PYTHON312" -c 'import sys; assert sys.version_info[:2] == (3, 12), sys.version'
"$PYTHON312" --version
```

An already exported `PYTHON312` takes precedence over automatic detection.

## 3. Prepare a clean local copy

Copy or extract this directory into a path without modifying its files. The experiment manifests record a Git commit, so initialize a local repository if the package was supplied as a ZIP:

```sh
git init
git add .
git -c user.name="Reproduction User" \
    -c user.email="reproduction@example.invalid" \
    commit -m "Reproduction snapshot"
```

If the directory is already a clean Git checkout, keep its existing history and skip those commands.

Create the required host environment and activate it. The validators for MR and
WFR import the experiment modules, so they require the same PyMongo dependency
as the experiment code. Run the remaining host-side Python commands in this
shell with the environment active; activate it again after opening a new shell.

```sh
"$PYTHON312" -m venv --clear .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python --version
```

`--clear` replaces an older environment in the same directory, including the
Python 3.9 environment that would otherwise remain incompatible with the pinned
analysis dependencies. The final version check must report Python 3.12.x.

The supplied `.gitignore` excludes `.venv`, Python caches, and newly generated
experiment output. Before a formal fault batch, commit any intentional source
or documentation changes and confirm that the recorded checkout is clean:

```sh
test -z "$(git status --porcelain)" || {
  git status --short
  echo "Commit or remove the listed changes before a formal run."
  exit 1
}
```

Do not use `git add -f` to commit `.venv` or generated run directories merely
to satisfy this check.

Build the pinned experiment runner:

```sh
docker compose build runner
```

Validate both Compose definitions without starting a database:

```sh
docker compose config >/dev/null
docker compose -p dsa5208-mw-fault -f docker/fault-compose.yml config >/dev/null
```

Run the software tests inside the runner:

```sh
docker compose run --rm --no-deps runner \
  python -m unittest discover -s tests -v
```

The package discovers 119 tests. In the compact submission, 115 self-contained tests pass and four archived-fixture regression tests are skipped because the large historical fixture bundle is not duplicated. The experiment validators remain included and run against every newly generated batch.

## 4. Start and verify the normal replica set

Start MongoDB and wait for one Primary and two Secondaries:

```sh
./scripts/start-cluster.sh
./scripts/cluster-status.sh --wait
```

The expected topology is:

```text
rs0
├── 1 PRIMARY
└── 2 SECONDARY members
```

Check the runner's replica-set connection:

```sh
./scripts/run-experiment.sh --check-only
```

Do not continue if the cluster has zero or two Primaries, fewer than two Secondaries, or an unhealthy member. Inspect it with:

```sh
docker compose ps
docker compose logs mongo1 mongo2 mongo3
```

## 5. Quick verification

This track exercises every model and each fault mechanism with small samples. It verifies the workflow; its results must not be presented as the formal sample.

### 5.1 Normal MW

```sh
./scripts/run-experiment.sh \
  --experiment mw --scenario normal \
  --configs C1 C4 --trials 2 \
  --experiment-id quick-mw-normal-001

python scripts/validate-run.py results/raw/quick-mw-normal-001
```

### 5.2 Normal RYW and MR

The normal RYW and MR runners validate each row against the included JSON Schema. Pass the current snapshot commit into the container:

```sh
SOURCE_COMMIT=$(git rev-parse HEAD)

docker compose run --rm --no-deps \
  -e GIT_COMMIT="$SOURCE_COMMIT" runner \
  python -m experiments.test_ryw --config C1 --trials 2 \
    --run-id quick-ryw-normal-001

docker compose run --rm --no-deps \
  -e GIT_COMMIT="$SOURCE_COMMIT" runner \
  python -m experiments.test_monotonic_reads --config C4 --trials 2 \
    --run-id quick-mr-normal-001
```

These commands write JSON Lines files with the specified run IDs under
`results/pilot/ryw/normal/debug/` and `results/pilot/mr/normal/debug/`.

### 5.3 WFR normal operation

The WFR normal smoke uses the isolated fault-lab deployment even though it injects no fault:

```sh
python scripts/run-fault-experiment.py \
  --workload wfr --scenario normal \
  --experiment-id quick-wfr-normal-001 \
  --configs C1 C4 --trials 1 \
  --sample-stage pilot \
  --wfr-protocol wfr-committed-dependency-v2 \
  --plan-id quick-verification \
  --no-retry-writes

python scripts/validate-wfr-run.py results/raw/quick-wfr-normal-001
```

### 5.4 One Secondary stopped

```sh
python scripts/run-fault-experiment.py \
  --workload ryw --scenario secondary-stop \
  --experiment-id quick-ryw-s2-001 \
  --configs C1 C4 --trials 2 \
  --sample-stage pilot --timeout-ms 5000 --no-retry-writes

python scripts/validate-secondary-run.py results/raw/quick-ryw-s2-001
```

### 5.5 Primary crash

```sh
python scripts/run-fault-experiment.py \
  --workload mr --scenario primary-crash \
  --experiment-id quick-mr-s4-001 \
  --configs C1 C4 --trials 2 \
  --sample-stage pilot --timeout-ms 10000 --no-retry-writes

python scripts/validate-mr-fault-run.py results/raw/quick-mr-s4-001
```

### 5.6 Replication partition

```sh
python scripts/run-fault-experiment.py \
  --workload wfr --scenario replication-partition \
  --experiment-id quick-wfr-s5-001 \
  --configs C1 C4 --trials 2 \
  --sample-stage pilot \
  --wfr-protocol wfr-committed-dependency-v2 \
  --plan-id quick-verification \
  --no-retry-writes

python scripts/validate-wfr-run.py results/raw/quick-wfr-s5-001
```

After every fault command, the controller prints that all isolated-lab nodes were restarted and reconnected. The validator must exit with status 0. If recovery fails, stop and follow Section 10 before running another fault.

## 6. Full reproduction of the reported experiment scope

The full sequence can take several hours. Keep the machine connected to power, prevent sleep, and do not run two fault commands at the same time. Fixed attempts are not replaced when they time out or become inconclusive.

### 6.1 Normal-operation matrix

Run MW with 100 trials per configuration:

```sh
./scripts/run-experiment.sh \
  --experiment mw --scenario normal \
  --configs C1 C2 C3 C4 --trials 100 \
  --experiment-id reproduce-mw-normal-001

python scripts/validate-run.py results/raw/reproduce-mw-normal-001
python scripts/archive-experiment.py reproduce-mw-normal-001
```

Run RYW and MR with 100 trials per configuration:

```sh
SOURCE_COMMIT=$(git rev-parse HEAD)

for CONFIG in C1 C2 C3 C4; do
  docker compose run --rm --no-deps \
    -e GIT_COMMIT="$SOURCE_COMMIT" runner \
    python -m experiments.test_ryw --config "$CONFIG" --trials 100 \
      --run-id reproduce-ryw-normal-001

  docker compose run --rm --no-deps \
    -e GIT_COMMIT="$SOURCE_COMMIT" runner \
    python -m experiments.test_monotonic_reads --config "$CONFIG" --trials 100 \
      --run-id reproduce-mr-normal-001
done
```

These runners produce Schema v1 pilot evidence. Their fixed run IDs allow the
analysis command in Section 7 to select exactly this reproduction cohort while
preserving its true `pilot` stage. They are not relabelled as legacy or formal
evidence. If either fixed ID already exists, choose a new ID for both the runner
and its matching `--select-source` pattern in Section 7; output is never
overwritten.

### 6.2 MW and RYW Secondary-loss matrix

The fixed Secondary matrix contains C1/C4, 20 pilot and 100 formal attempts per cell, for S2, S3 immediate, and S3 settled. Run the pilot first because the formal launcher validates it before continuing:

```sh
python scripts/run-secondary-suite.py \
  --prefix reproduce-secondary \
  --stage pilot \
  --workloads mw ryw \
  --scenarios s2 s3 s3-settled

python scripts/run-secondary-suite.py \
  --prefix reproduce-secondary-formal \
  --stage formal \
  --pilot-prefix reproduce-secondary \
  --workloads mw ryw \
  --scenarios s2 s3 s3-settled
```

The launcher validates every batch and copies verified evidence into `results/archive/`. Expected totals are 240 pilot and 1,200 formal attempts. S3 may legitimately produce timeout-heavy or zero-evaluable groups after loss of a majority.

### 6.3 MR S2 and S4

Run C1/C4 pilot batches:

```sh
python scripts/run-fault-experiment.py \
  --workload mr --scenario secondary-stop \
  --experiment-id reproduce-mr-s2-pilot \
  --configs C1 C4 --trials 20 \
  --sample-stage pilot --timeout-ms 5000 --no-retry-writes

python scripts/run-fault-experiment.py \
  --workload mr --scenario primary-crash \
  --experiment-id reproduce-mr-s4-pilot \
  --configs C1 C4 --trials 20 \
  --sample-stage pilot --timeout-ms 10000 --no-retry-writes

python scripts/validate-mr-fault-run.py results/raw/reproduce-mr-s2-pilot
python scripts/validate-mr-fault-run.py results/raw/reproduce-mr-s4-pilot
```

Then run 100 formal attempts per configuration:

```sh
python scripts/run-fault-experiment.py \
  --workload mr --scenario secondary-stop \
  --experiment-id reproduce-mr-s2-formal \
  --configs C1 C4 --trials 100 \
  --sample-stage formal --timeout-ms 5000 --no-retry-writes

python scripts/run-fault-experiment.py \
  --workload mr --scenario primary-crash \
  --experiment-id reproduce-mr-s4-formal \
  --configs C1 C4 --trials 100 \
  --sample-stage formal --timeout-ms 10000 --no-retry-writes

python scripts/validate-mr-fault-run.py results/raw/reproduce-mr-s2-formal
python scripts/validate-mr-fault-run.py results/raw/reproduce-mr-s4-formal
```

Expected total: 80 pilot and 400 formal attempts. The formal validator requires complete fault-order, source-hash, recovery, and convergence evidence.

### 6.4 Historical MW and RYW S4/S5 diagnostics

These batches are smaller than the principal fixed matrices and must remain separate.

```sh
python scripts/run-fault-experiment.py \
  --workload mw --scenario primary-crash \
  --experiment-id reproduce-mw-s4-diagnostic \
  --configs C1 C2 C3 C4 --trials 20

python scripts/validate-fault-run.py \
  results/raw/reproduce-mw-s4-diagnostic --archive

python scripts/run-fault-experiment.py \
  --workload mw --scenario replication-partition \
  --experiment-id reproduce-mw-s5-diagnostic \
  --configs C1 C4 --trials 5 --no-retry-writes

python scripts/validate-fault-run.py \
  results/raw/reproduce-mw-s5-diagnostic --archive
```

Run one RYW diagnostic attempt per configuration and scenario:

```sh
python scripts/run-fault-experiment.py \
  --workload ryw --scenario primary-crash \
  --experiment-id reproduce-ryw-s4-diagnostic \
  --configs C1 C2 C3 C4 --trials 1

python scripts/run-fault-experiment.py \
  --workload ryw --scenario replication-partition \
  --experiment-id reproduce-ryw-s5-diagnostic \
  --configs C1 C2 C3 C4 --trials 1 --no-retry-writes

python scripts/validate-ryw-fault-run.py results/raw/reproduce-ryw-s4-diagnostic
python scripts/validate-ryw-fault-run.py results/raw/reproduce-ryw-s5-diagnostic
```

Rollback is reported separately from MW ordering. A timeout or failed predecessor write is not a consistency violation.

### 6.5 MR partition diagnostic

MR partition uses a separate three-node project with independent volumes and tagged observation networks.
The commands below create a fresh, dedicated MR diagnostic deployment. The
volume removal applies only to the `dsa5208-mr` Compose project; copy any prior
diagnostic evidence out first if this is not a new reproduction.

```sh
docker compose -f docker-compose.mr.yml down --volumes
docker compose -f docker-compose.mr.yml up -d

until docker compose -f docker-compose.mr.yml exec -T mongo1 \
  mongosh --quiet --eval 'quit(db.adminCommand({ping:1}).ok ? 0 : 2)'; do
  sleep 1
done

docker compose -f docker-compose.mr.yml exec -T mongo1 \
  mongosh --quiet --eval '
    try { rs.status(); }
    catch (error) {
      rs.initiate({_id:"rs0",members:[
        {_id:0,host:"mongo1:27017",priority:2,tags:{target:"mongo1"}},
        {_id:1,host:"mongo2:27017",priority:1,tags:{target:"mongo2"}},
        {_id:2,host:"mongo3:27017",priority:1,tags:{target:"mongo3"}}
      ]});
    }
  '

docker compose -f docker-compose.mr.yml exec -T client \
  sh -lc 'python -m venv /opt/venv && \
          /opt/venv/bin/pip install -r requirements-experiments.txt'
```

Wait until `mongo1` is Primary, `mongo2` and `mongo3` are Secondaries, and all
three `target` tags are present. The probe enforces these preconditions because
its first and second reads are deliberately routed to different Secondaries.

```sh
until docker compose -f docker-compose.mr.yml exec -T mongo1 \
  mongosh --quiet --eval '
    const status = rs.status();
    const config = rs.conf();
    status.members.forEach(m => print(m.name, m.stateStr, m.health));
    printjson(config.members.map(m => ({host:m.host, priority:m.priority, tags:m.tags})));
    if (db.hello().primary !== "mongo1:27017" ||
        status.members.filter(m => m.stateStr === "SECONDARY").length !== 2 ||
        config.members.some(m => !m.tags || m.tags.target !== m.host.split(":")[0])) {
      quit(2);
    }
  '; do
  sleep 1
done
```

Run five fixed diagnostics for each comparison configuration:

```sh
for N in 1 2 3 4 5; do python scripts/run_mr_partition.py --config C1; done
for N in 1 2 3 4 5; do python scripts/run_mr_partition.py --config C4; done
```

The controller restores the network and verifies document convergence after every trial. C1 may expose a 2-to-1 regression; C4 may time out. Either outcome is environment-dependent, so reproduce the protocol and classification rather than demanding identical counts.

Stop this dedicated project without deleting its volumes:

```sh
docker compose -f docker-compose.mr.yml down
```

### 6.6 Fixed WFR v2 fault matrix

Run 20 pilot attempts per C1/C4 cell:

```sh
for SCENARIO in secondary-stop primary-crash replication-partition; do
  python scripts/run-fault-experiment.py \
    --workload wfr --scenario "$SCENARIO" \
    --experiment-id "reproduce-wfr-${SCENARIO}-pilot" \
    --configs C1 C4 --trials 20 \
    --sample-stage pilot \
    --wfr-protocol wfr-committed-dependency-v2 \
    --plan-id reproduce-wfr-v2 \
    --no-retry-writes

  python scripts/validate-wfr-run.py \
    "results/raw/reproduce-wfr-${SCENARIO}-pilot"
done
```

Run 100 formal attempts per cell:

```sh
for SCENARIO in secondary-stop primary-crash replication-partition; do
  python scripts/run-fault-experiment.py \
    --workload wfr --scenario "$SCENARIO" \
    --experiment-id "reproduce-wfr-${SCENARIO}-formal" \
    --configs C1 C4 --trials 100 \
    --sample-stage formal \
    --wfr-protocol wfr-committed-dependency-v2 \
    --plan-id reproduce-wfr-v2 \
    --no-retry-writes

  python scripts/validate-wfr-run.py \
    "results/raw/reproduce-wfr-${SCENARIO}-formal"
done
```

Expected total: 120 pilot and 600 formal attempts. The report's formal batch had 596 evaluable attempts, four inconclusive attempts, and complete recovery. A new machine may produce different timeout counts; never rerun selectively to replace an inconclusive attempt.

### 6.7 Optional historical WFR diagnostic

The earlier protocol tests an uncommitted old-Primary dependency and is not pooled with the fixed v2 matrix:

```sh
python scripts/run-fault-experiment.py \
  --workload wfr --scenario normal \
  --experiment-id reproduce-wfr-v1-normal \
  --configs C1 C4 --trials 3 \
  --wfr-protocol wfr-protocol-1 --no-retry-writes

python scripts/run-fault-experiment.py \
  --workload wfr --scenario primary-crash \
  --experiment-id reproduce-wfr-v1-crash \
  --configs C1 C4 --trials 1 \
  --wfr-protocol wfr-protocol-1 --no-retry-writes

python scripts/run-fault-experiment.py \
  --workload wfr --scenario replication-partition \
  --experiment-id reproduce-wfr-v1-partition \
  --configs C1 C4 --trials 1 \
  --wfr-protocol wfr-protocol-1 --no-retry-writes
```

Validate the v1 directories with the historical-protocol verifier. The v2-only
`validate-wfr-run.py` intentionally rejects these runs.

```sh
python scripts/verify-wfr-pilot.py \
  results/raw/reproduce-wfr-v1-normal \
  results/raw/reproduce-wfr-v1-crash \
  results/raw/reproduce-wfr-v1-partition
```

Candidate rollback evidence remains a candidate unless its trial-specific
evidence supports the classification.

## 7. Regenerate unified statistics and figures

Section 3 is required. If this is a new shell, reactivate its environment:

```sh
. .venv/bin/activate
python --version
```

Choose output directories that do not already exist:

```sh
python -m analysis.unified \
  --select-source 'results/pilot/ryw/normal/debug/*-reproduce-ryw-normal-001.jsonl' \
  --select-source 'results/pilot/mr/normal/debug/*-reproduce-mr-normal-001.jsonl' \
  --output results/summary/reproduced-final

python -m analysis.plot_results \
  --input results/summary/reproduced-final \
  --output results/figures/reproduced-final
```

Each `--select-source` value is a repository-relative glob for an explicitly
named reproduction cohort. The analyzer fails if a pattern matches no evidence,
records the selection reason in `catalog.json`, and retains the source's actual
pilot/formal stage. This prevents unrelated files in a debug directory from
silently entering the figures.

The summary directory contains:

- `catalog.json`: source files, hashes, formats, stages, and selection decisions
- `trials.json`: normalized trial records
- `summary.json`: grouped denominators, classifications, latency, and recovery
- `README.md`: human-readable selected-cohort table and provenance

The figure directory contains PNG and SVG versions of violation, availability, latency, and selected timeline charts. Legacy, pilot, diagnostic, and formal cohorts remain separate.

## 8. How to interpret a successful reproduction

A reproduction is successful when:

1. the cluster reaches exactly one Primary and two Secondaries before a batch;
2. every completed batch passes its matching validator;
3. manifests and source archives pass hash checks;
4. each fault trial has explicit recovery evidence, or the batch stops on recovery failure;
5. the unified analyzer completes without modifying source evidence; and
6. the classification rules are preserved.

Exact violation and timeout counts may differ because elections, scheduling, and replication lag are nondeterministic. The expected qualitative checkpoints are:

- weak Secondary-preferring RYW can return an older version;
- C4 should wait, fail, or time out rather than successfully return a causally older value when its prerequisites hold;
- loss of two Secondaries can make majority writes unavailable;
- sequential modifying writes should not be labeled as MW violations merely because an audit read is stale;
- rollback, candidate, invalid, inconclusive, timeout, and confirmed violation remain separate; and
- cluster recovery is reported independently from operation consistency.

## 9. Evidence locations

For a run named `RUN_ID`, inspect:

```text
results/raw/RUN_ID/manifest.json
results/raw/RUN_ID/operations.jsonl
results/raw/RUN_ID/events.jsonl
results/raw/RUN_ID/evidence/
results/raw/RUN_ID/summary.json
results/fault-control/RUN_ID/
```

Some protocols also produce `trials.csv`, `operations.csv`, node audits, rollback BSON, or source archives. Do not edit raw evidence. Regenerate summaries in a new directory.

## 10. Failure recovery and troubleshooting

### Docker is unavailable

Start Docker Desktop and rerun `docker info`.

### Replica set has no Primary

```sh
docker compose ps
docker compose logs mongo1 mongo2 mongo3
./scripts/cluster-status.sh --wait
```

Do not start an experiment until the health gate passes.

### A fault run was interrupted

Restore all isolated-lab members and replication links:

```sh
docker compose -p dsa5208-mw-fault -f docker/fault-compose.yml \
  start mongo1 mongo2 mongo3

for NODE in mongo1 mongo2 mongo3; do
  CONTAINER=$(docker compose -p dsa5208-mw-fault \
    -f docker/fault-compose.yml ps -a -q "$NODE")
  docker network connect --alias "$NODE" \
    dsa5208-mw-fault_replication "$CONTAINER" 2>/dev/null || true
done
```

Then inspect the interrupted run. Do not delete it or reuse its experiment ID.

### A validator fails

Stop further sampling. The usual causes are an interrupted run, missing controller response, source-hash mismatch, failed recovery, or edited evidence. Preserve the directory and read the first validator error before retrying with a new ID.

### Output directory already exists

This is intentional overwrite protection. Select a new experiment ID or a new summary/figure directory.

### Formal run is rejected

Formal fault runs require at least 100 trials per configuration. WFR formal runs also require the committed-dependency protocol and a plan ID. Secondary formal batches require matching validated pilot evidence.

## 11. Safe shutdown

Stop the normal cluster and the isolated fault project while preserving their volumes:

```sh
./scripts/stop-cluster.sh
docker compose -p dsa5208-mw-fault -f docker/fault-compose.yml down
docker compose -f docker-compose.mr.yml down
```

Do not add `--volumes` unless permanent deletion of database state is explicitly intended.

## 12. Reproduction checklist

- [ ] Tool versions recorded
- [ ] Runner image built
- [ ] 115 self-contained tests passed and four archived-fixture tests were reported as skipped
- [ ] Normal replica set reached one Primary and two Secondaries
- [ ] Quick MW, RYW, MR, and WFR checks completed
- [ ] S2/S3 MW and RYW batches validated
- [ ] S2/S4 MR batches validated
- [ ] MW/RYW diagnostic batches retained separately
- [ ] MR partition diagnostic recovered after every attempt
- [ ] WFR pilot and formal batches validated
- [ ] Unified summary regenerated in a new directory
- [ ] Figures regenerated in a new directory
- [ ] Raw evidence remained unchanged
- [ ] All fault-lab nodes and networks were restored

For the interpretation, exact recorded sample counts, and limitations, see `report.pdf`.
