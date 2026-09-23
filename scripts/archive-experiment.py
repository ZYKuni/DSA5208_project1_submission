"""Verify checksums then archive a run for review/version control."""
import argparse
import hashlib
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('experiment_id')
    args = parser.parse_args()
    if Path(args.experiment_id).name != args.experiment_id or args.experiment_id in ('.', '..'):
        parser.error('Pass an experiment ID, not a path')
    source = ROOT / 'results/raw' / args.experiment_id
    manifest = json.loads((source / 'manifest.json').read_text())
    for name, expected in manifest['artifact_sha256'].items():
        if Path(name).name != name:
            raise ValueError('Invalid artifact filename')
        actual = hashlib.sha256((source / name).read_bytes()).hexdigest()
        if actual != expected:
            raise ValueError(f'Checksum mismatch: {name}')
    destination = ROOT / 'results/archive' / args.experiment_id
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, destination)  # Refuse to overwrite an existing archive.
    print(destination)


if __name__ == '__main__':
    main()
