"""Command line entry points for generate, run, and the local dashboard."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from hc_platform.config import load_settings, project_root
from hc_platform.generator.generate import generate_dataset
from hc_platform.pipeline.runner import run_pipeline


def _cmd_generate(args: argparse.Namespace) -> None:
    manifest = generate_dataset(args.output, scale=args.scale, seed=args.seed)
    print(f"Wrote synthetic batches to {Path(args.output).resolve()}")
    print(f"Manifest: {manifest}")


def _cmd_run(args: argparse.Namespace) -> None:
    settings = load_settings(args.env)
    run_pipeline(settings, fail_after=args.fail_after, force=args.force, stop_spark=True)


def _cmd_dashboard(args: argparse.Namespace) -> None:
    root = project_root()
    app = root / "dashboard" / "app.py"
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app),
        "--server.headless",
        "true",
        "--browser.gatherUsageStats",
        "false",
    ]
    if args.port:
        command.extend(["--server.port", str(args.port)])
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(root), str(root / "src"), env.get("PYTHONPATH", "")) if part
    )
    raise SystemExit(subprocess.call(command, env=env))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hc-platform")
    sub = parser.add_subparsers(dest="command", required=True)

    generate = sub.add_parser("generate", help="Write synthetic landing batches")
    generate.add_argument("--scale", default="small", choices=["tiny", "small", "medium"])
    generate.add_argument("--output", default="data/sample")
    generate.add_argument("--seed", type=int, default=42)
    generate.set_defaults(func=_cmd_generate)

    run = sub.add_parser("run", help="Run the medallion pipeline")
    run.add_argument("--env", default="dev", choices=["dev", "test", "prod"])
    run.add_argument("--fail-after", choices=["silver", "gold"], default=None)
    run.add_argument("--force", action="store_true", help="Ignore the batch checkpoint and replay")
    run.set_defaults(func=_cmd_run)

    dashboard = sub.add_parser("dashboard", help="Launch the local analytics dashboard")
    dashboard.add_argument("--port", type=int, default=8501)
    dashboard.set_defaults(func=_cmd_dashboard)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
