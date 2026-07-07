#!/usr/bin/env python3
"""Create or verify the context-ledger package drift manifest."""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path
from typing import Dict, Iterable

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_ROOT = SCRIPT_DIR.parent
MANIFEST = SKILL_ROOT / "SHA256SUMS.txt"
EXCLUDED_NAMES = {"SHA256SUMS.txt"}


class ManifestError(RuntimeError):
    pass


def iter_files() -> Iterable[Path]:
    root = SKILL_ROOT.resolve()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_symlink():
            raise ManifestError(f"symlinks are not permitted in the package: {path.relative_to(root)}")
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if relative.name in EXCLUDED_NAMES:
            continue
        if "__pycache__" in relative.parts or relative.suffix in {".pyc", ".pyo"}:
            continue
        resolved = path.resolve()
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise ManifestError(f"path escapes skill root: {relative}") from exc
        yield path


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def expected_entries() -> Dict[str, str]:
    return {
        "./" + path.relative_to(SKILL_ROOT).as_posix(): digest(path)
        for path in iter_files()
    }


def parse_manifest() -> Dict[str, str]:
    if not MANIFEST.is_file():
        raise ManifestError(f"manifest is missing: {MANIFEST}")
    entries: Dict[str, str] = {}
    for number, line in enumerate(MANIFEST.read_text(encoding="utf-8").splitlines(), start=1):
        if not line:
            raise ManifestError(f"blank manifest line {number}")
        if "  " not in line:
            raise ManifestError(f"malformed manifest line {number}")
        value, relative = line.split("  ", 1)
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ManifestError(f"invalid SHA-256 on manifest line {number}")
        if not relative.startswith("./"):
            raise ManifestError(f"manifest path must start with './' on line {number}: {relative}")
        body = relative[2:]
        rel_path = Path(body)
        if not body or rel_path.is_absolute() or ".." in rel_path.parts or body != rel_path.as_posix():
            raise ManifestError(f"noncanonical manifest path on line {number}: {relative}")
        if relative in entries:
            raise ManifestError(f"duplicate manifest path: {relative}")
        entries[relative] = value
    return entries


def render(entries: Dict[str, str]) -> str:
    return "".join(f"{value}  {relative}\n" for relative, value in sorted(entries.items()))


def check() -> None:
    actual = parse_manifest()
    expected = expected_entries()
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    changed = sorted(path for path in set(actual) & set(expected) if actual[path] != expected[path])
    errors = []
    if missing:
        errors.append("missing entries: " + ", ".join(missing))
    if extra:
        errors.append("unexpected entries: " + ", ".join(extra))
    if changed:
        errors.append("changed files: " + ", ".join(changed))
    if errors:
        raise ManifestError("; ".join(errors))
    print(f"RESULT: PASS\nEntries: {len(expected)}")


def write() -> None:
    entries = expected_entries()
    temp = MANIFEST.with_name(f".{MANIFEST.name}.{os.getpid()}.tmp")
    temp.write_text(render(entries), encoding="utf-8", newline="\n")
    os.replace(temp, MANIFEST)
    print(f"Wrote {MANIFEST} with {len(entries)} entries")


def main() -> int:
    parser = argparse.ArgumentParser(description="write or verify SHA256SUMS.txt")
    parser.add_argument("--check", action="store_true", help="verify rather than rewrite")
    args = parser.parse_args()
    try:
        check() if args.check else write()
        return 0
    except ManifestError as exc:
        print(f"manifest: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
