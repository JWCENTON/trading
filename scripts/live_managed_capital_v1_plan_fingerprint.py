#!/usr/bin/env python3
"""Compute and verify the canonical fingerprint of a baseline plan artifact."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from common.live_managed_capital import plan_artifact_fingerprint


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--verify-embedded", action="store_true")
    args = parser.parse_args()
    artifact = json.loads(args.artifact.read_text(encoding="utf-8"))
    computed = plan_artifact_fingerprint(artifact)
    if args.verify_embedded and artifact.get("artifact_fingerprint") != computed:
        raise ValueError("LIVE_BASELINE_FINGERPRINT_MISMATCH")
    print(computed)


if __name__ == "__main__":
    main()
