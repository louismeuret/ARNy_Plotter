#!/usr/bin/env python3
"""
run_tests.py — Test runner for Plot_RNA6

Usage:
    python tests/run_tests.py [OPTIONS] [SUITE...]

Suites:
    unit          Fast unit tests (no MD files required)
    integration   Integration tests (Flask routes + real MD files)
    installation  Package presence, scripts, project structure checks
    workers       Celery task tests in eager mode (real MD files)
    e2e           End-to-end pipeline tests (real MD files + barnaba stack)
    all           Run every suite (default)

Examples:
    python tests/run_tests.py                          # run everything
    python tests/run_tests.py unit                     # only unit tests
    python tests/run_tests.py unit integration         # unit + integration
    python tests/run_tests.py installation             # check install health
    python tests/run_tests.py -v e2e                  # e2e with verbose output
    python tests/run_tests.py --fast                   # unit only, fastest feedback
    python tests/run_tests.py --no-e2e                # skip slow e2e tests
    python tests/run_tests.py workers                  # celery worker tests only
    python tests/run_tests.py --cov                   # run with coverage report

Options:
    -v / --verbose      Verbose pytest output
    -x / --failfast     Stop after first failure
    --fast              Alias for 'unit' suite only
    --no-e2e            Skip end-to-end tests (run unit + integration + celery)
    --cov               Generate HTML coverage report (requires pytest-cov)
    -k EXPR             Only run tests matching expression (passed to pytest)
    -h / --help         Show this help message
"""

import argparse
import subprocess
import sys
import os
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TESTS_DIR = os.path.join(PROJECT_ROOT, "tests")

# Colour codes (disabled on Windows or non-TTY)
_USE_COLOR = sys.stdout.isatty() and os.name != "nt"

def _c(code, text):
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text

GREEN  = lambda t: _c("32;1", t)
RED    = lambda t: _c("31;1", t)
YELLOW = lambda t: _c("33;1", t)
BLUE   = lambda t: _c("34;1", t)
BOLD   = lambda t: _c("1", t)

SUITE_PATHS = {
    "unit":         os.path.join(TESTS_DIR, "unit"),
    "integration":  os.path.join(TESTS_DIR, "integration"),
    "installation": os.path.join(TESTS_DIR, "installation"),
    "workers":      os.path.join(TESTS_DIR, "workers"),
    "e2e":          os.path.join(TESTS_DIR, "e2e"),
}

SUITE_DESCRIPTIONS = {
    "unit":         "Fast unit tests — no external files required",
    "integration":  "Flask route integration tests — uses example MD files",
    "installation": "Installation checks — packages, scripts, project structure",
    "workers":      "Celery task tests (eager mode) — uses example MD files",
    "e2e":          "End-to-end pipeline tests — barnaba + MD files required",
}


def _print_banner(suites):
    print()
    print(BOLD("=" * 60))
    print(BOLD("  Plot_RNA6 Test Runner"))
    print(BOLD("=" * 60))
    print(f"  Project root : {PROJECT_ROOT}")
    print(f"  Suites       : {', '.join(suites)}")
    print(BOLD("=" * 60))
    print()


def _build_pytest_cmd(paths, args):
    cmd = [sys.executable, "-m", "pytest"]
    cmd += paths

    if args.verbose:
        cmd.append("-v")
    else:
        cmd.append("-q")

    if args.failfast:
        cmd.append("-x")

    if args.cov:
        cmd += [
            "--cov=src",
            "--cov-report=html:tests/coverage_html",
            "--cov-report=term-missing",
        ]

    if args.k:
        cmd += ["-k", args.k]

    # Always show short test summary
    cmd += ["-r", "fEsxX"]

    return cmd


def _run_suite(suite_name, path, args):
    """Run a single test suite. Returns (returncode, duration_seconds)."""
    print(BLUE(f"\n{'─' * 60}"))
    print(BLUE(f"  Suite: {suite_name.upper()}"))
    print(f"  {SUITE_DESCRIPTIONS[suite_name]}")
    print(BLUE(f"{'─' * 60}"))

    if not os.path.isdir(path):
        print(YELLOW(f"  [SKIP] Directory not found: {path}"))
        return 0, 0.0

    cmd = _build_pytest_cmd([path], args)
    print(f"  Running: {' '.join(cmd)}\n")

    start = time.time()
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    duration = time.time() - start

    return result.returncode, duration


def _print_summary(results):
    print()
    print(BOLD("=" * 60))
    print(BOLD("  Summary"))
    print(BOLD("=" * 60))

    all_passed = True
    for suite, (rc, duration) in results.items():
        if rc == 0:
            status = GREEN("PASSED")
        elif rc == 5:
            status = YELLOW("NO TESTS")
        else:
            status = RED("FAILED")
            all_passed = False
        print(f"  {suite:<14}  {status}   ({duration:.1f}s)")

    print(BOLD("=" * 60))
    if all_passed:
        print(GREEN("  All suites passed!"))
    else:
        print(RED("  Some suites failed."))
    print(BOLD("=" * 60))
    print()

    return all_passed


def main():
    parser = argparse.ArgumentParser(
        description="Plot_RNA6 test runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "suites",
        nargs="*",
        default=["all"],
        choices=list(SUITE_PATHS.keys()) + ["all"],
        metavar="SUITE",
        help=f"Test suites to run: {', '.join(SUITE_PATHS)} or all (default: all)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-x", "--failfast", action="store_true", help="Stop after first failure")
    parser.add_argument("--fast", action="store_true", help="Run unit tests only (fastest)")
    parser.add_argument("--no-e2e", action="store_true", help="Skip e2e tests")
    parser.add_argument("--cov", action="store_true", help="Generate coverage report")
    parser.add_argument("-k", metavar="EXPR", help="Run tests matching expression")
    args = parser.parse_args()

    # Resolve suites
    if args.fast:
        selected = ["unit"]
    elif "all" in args.suites:
        selected = list(SUITE_PATHS.keys())
    else:
        selected = args.suites

    if args.no_e2e and "e2e" in selected:
        selected.remove("e2e")

    _print_banner(selected)

    results = {}
    for suite in selected:
        rc, dur = _run_suite(suite, SUITE_PATHS[suite], args)
        results[suite] = (rc, dur)
        if args.failfast and rc not in (0, 5):
            print(RED(f"\n  Stopping after failure in suite: {suite}"))
            break

    all_passed = _print_summary(results)

    if args.cov and "coverage_html" in str(results):
        cov_path = os.path.join(TESTS_DIR, "coverage_html", "index.html")
        if os.path.isfile(cov_path):
            print(f"  Coverage report: {cov_path}")

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
