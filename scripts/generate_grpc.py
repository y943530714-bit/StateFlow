"""Regenerate the checked-in Python gRPC bindings."""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
PROTO = ROOT / "proto" / "stateflow.proto"
OUTPUT = ROOT / "stateflow" / "rpc" / "generated"
GENERATED = ("stateflow_pb2.py", "stateflow_pb2_grpc.py")


def generate(*, check: bool = False) -> int:
    with tempfile.TemporaryDirectory(prefix="stateflow-grpc-") as directory:
        target = Path(directory)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                "-I",
                str(PROTO.parent),
                f"--python_out={target}",
                f"--grpc_python_out={target}",
                str(PROTO),
            ],
            check=True,
        )
        for name in GENERATED:
            content = (target / name).read_text(encoding="utf-8")
            if name.endswith("_grpc.py"):
                content = content.replace(
                    "import stateflow_pb2 as stateflow__pb2",
                    "from . import stateflow_pb2 as stateflow__pb2",
                )
            destination = OUTPUT / name
            if check:
                if not destination.exists() or destination.read_text(
                    encoding="utf-8"
                ) != content:
                    print(f"generated binding is stale: {destination.relative_to(ROOT)}")
                    return 1
            else:
                destination.write_text(content, encoding="utf-8")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    return generate(check=args.check)


if __name__ == "__main__":
    raise SystemExit(main())
