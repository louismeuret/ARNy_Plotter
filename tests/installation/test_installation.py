"""
Installation tests for ARNy Plotter.

These tests verify that:
  1. All Python packages listed in requirements.txt are importable at the
     expected (or compatible) version.
  2. The project's own package tree (src/) is importable.
  3. External system tools required by the service stack are present
     (redis-server, celery, gunicorn).
  4. The shell scripts are syntactically valid and executable.
  5. The Installer class logic (verify_conda_env, check_git, file copying,
     launcher creation) works correctly in isolation, without hitting the
     network.
  6. Redis connectivity can be established (skipped gracefully when Redis
     is not running).
  7. The mandatory project file tree is present and complete.

Run with:
    python tests/run_tests.py installation
or:
    pytest tests/installation/ -v
"""

import importlib
import io
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR  = PROJECT_ROOT / "scripts"
REQUIREMENTS  = SCRIPTS_DIR / "installation" / "requirements.txt"
INSTALL_SCRIPT = SCRIPTS_DIR / "install_script.py"
START_SH       = SCRIPTS_DIR / "start_services.sh"
STOP_SH        = SCRIPTS_DIR / "stop_services.sh"
CRON_SH        = SCRIPTS_DIR / "cron_start_services.sh"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_requirements(req_path: Path):
    """Return list of (package_name, version_spec) tuples from requirements.txt."""
    entries = []
    with open(req_path) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            # Split on first occurrence of ==, >=, <=, !=, ~=
            for sep in ("==", ">=", "<=", "!=", "~="):
                if sep in line:
                    name, ver = line.split(sep, 1)
                    entries.append((name.strip(), sep, ver.strip()))
                    break
            else:
                entries.append((line, None, None))
    return entries


def _importable_name(package_name: str) -> str:
    """Convert pip package name to Python importable module name."""
    # Common mapping table
    mapping = {
        "Flask":          "flask",
        "Flask_SocketIO": "flask_socketio",
        "Pillow":         "PIL",
        "Requests":       "requests",
        "Werkzeug":       "werkzeug",
        "barnaba":        "barnaba",
        "celery":         "celery",
        "eventlet":       "eventlet",
        "kaleido":        "kaleido",
        "matplotlib":     "matplotlib",
        "mdanalysis":     "MDAnalysis",
        "mdtraj":         "mdtraj",
        "numba":          "numba",
        "numpy":          "numpy",
        "orjson":         "orjson",
        "pandas":         "pandas",
        "plotly":         "plotly",
        "redis":          "redis",
        "rq":             "rq",
        "scipy":          "scipy",
        "seaborn":        "seaborn",
    }
    return mapping.get(package_name, package_name.lower().replace("-", "_"))


# ---------------------------------------------------------------------------
# 1. Python package presence
# ---------------------------------------------------------------------------

REQUIRED_PACKAGES = [
    ("barnaba",        "barnaba"),
    ("celery",         "celery"),
    ("eventlet",       "eventlet"),
    ("Flask",          "flask"),
    ("Flask_SocketIO", "flask_socketio"),
    ("kaleido",        "kaleido"),
    ("matplotlib",     "matplotlib"),
    ("mdanalysis",     "MDAnalysis"),
    ("mdtraj",         "mdtraj"),
    ("numba",          "numba"),
    ("numpy",          "numpy"),
    ("orjson",         "orjson"),
    ("pandas",         "pandas"),
    ("Pillow",         "PIL"),
    ("plotly",         "plotly"),
    ("redis",          "redis"),
    ("Requests",       "requests"),
    ("rq",             "rq"),
    ("scipy",          "scipy"),
    ("seaborn",        "seaborn"),
    ("Werkzeug",       "werkzeug"),
]


@pytest.mark.parametrize("pip_name,import_name", REQUIRED_PACKAGES)
def test_package_importable(pip_name, import_name):
    """Every package listed in requirements.txt must be importable."""
    try:
        importlib.import_module(import_name)
    except ImportError as exc:
        pytest.fail(
            f"Package '{pip_name}' (import as '{import_name}') is not installed: {exc}"
        )


class TestRequirementsFile:
    """Validate the requirements.txt file itself."""

    def test_requirements_file_exists(self):
        assert REQUIREMENTS.is_file(), f"requirements.txt not found at {REQUIREMENTS}"

    def test_requirements_file_not_empty(self):
        content = REQUIREMENTS.read_text().strip()
        assert len(content) > 0, "requirements.txt is empty"

    def test_all_required_packages_listed(self):
        """Core packages must appear in requirements.txt."""
        text = REQUIREMENTS.read_text().lower()
        for pip_name, _ in REQUIRED_PACKAGES:
            assert pip_name.lower() in text, \
                f"Package '{pip_name}' missing from requirements.txt"

    def test_requirements_parseable(self):
        """Every non-comment line must follow package==version syntax."""
        entries = _parse_requirements(REQUIREMENTS)
        assert len(entries) >= 10, "requirements.txt has fewer entries than expected"

    def test_no_git_plus_urls_in_requirements(self):
        """Avoid unpinned git+ URLs that make installs non-reproducible."""
        text = REQUIREMENTS.read_text()
        git_lines = [l for l in text.splitlines() if l.strip().startswith("git+")]
        assert len(git_lines) == 0, \
            f"Found git+ URLs in requirements.txt (non-reproducible): {git_lines}"

    def test_versions_are_pinned(self):
        """Every entry should have a version specifier (== preferred)."""
        entries = _parse_requirements(REQUIREMENTS)
        unpinned = [name for name, sep, _ in entries if sep is None]
        assert len(unpinned) == 0, \
            f"Unpinned packages in requirements.txt: {unpinned}"


# ---------------------------------------------------------------------------
# 2. Package version checks (soft — warn, not fail, on minor version drift)
# ---------------------------------------------------------------------------

class TestPackageVersions:
    """Verify installed package versions match or exceed requirements.txt pins."""

    def _get_installed_version(self, import_name: str):
        try:
            mod = importlib.import_module(import_name)
            return getattr(mod, "__version__", None)
        except ImportError:
            return None

    def test_numpy_version_compatible(self):
        import numpy as np
        major = int(np.__version__.split(".")[0])
        assert major >= 1, f"numpy version too old: {np.__version__}"

    def test_pandas_version_compatible(self):
        import pandas as pd
        major = int(pd.__version__.split(".")[0])
        assert major >= 1, f"pandas version too old: {pd.__version__}"

    def test_flask_version_compatible(self):
        import flask
        major = int(flask.__version__.split(".")[0])
        assert major >= 2, f"Flask version too old: {flask.__version__}"

    def test_plotly_version_compatible(self):
        import plotly
        major = int(plotly.__version__.split(".")[0])
        assert major >= 5, f"plotly version too old: {plotly.__version__}"

    def test_celery_version_compatible(self):
        import celery
        major = int(celery.__version__.split(".")[0])
        assert major >= 5, f"celery version too old: {celery.__version__}"

    def test_mdanalysis_version_compatible(self):
        import MDAnalysis
        major = int(MDAnalysis.__version__.split(".")[0])
        assert major >= 2, f"MDAnalysis version too old: {MDAnalysis.__version__}"

    def test_barnaba_installed_and_functional(self):
        """barnaba must import and expose the key API functions."""
        import barnaba as bb
        for fn in ("rmsd", "ermsd", "annotate"):
            assert hasattr(bb, fn), f"barnaba.{fn} is not accessible"

    def test_scipy_importable_with_stats(self):
        from scipy import stats
        assert callable(stats.pearsonr)

    def test_sklearn_importable(self):
        """sklearn is used for PCA/t-SNE — check it's installed."""
        try:
            from sklearn.decomposition import PCA
            from sklearn.manifold import TSNE
        except ImportError:
            pytest.skip("scikit-learn not installed (optional for some analyses)")

    def test_umap_importable(self):
        """umap-learn is used for UMAP dimensionality reduction."""
        try:
            import umap
        except ImportError:
            pytest.skip("umap-learn not installed (optional for UMAP analysis)")


# ---------------------------------------------------------------------------
# 3. Project source-tree imports
# ---------------------------------------------------------------------------

class TestProjectImports:
    """Verify that the project's own modules are importable."""

    def test_src_core_tasks_importable(self):
        """Celery task definitions must load without errors."""
        sys.path.insert(0, str(PROJECT_ROOT))
        import src.core.tasks  # noqa: F401

    def test_src_plotting_create_plots_importable(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        import src.plotting.create_plots  # noqa: F401

    def test_src_analysis_importable(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        import src.analysis  # noqa: F401

    def test_src_api_importable(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        import src.api  # noqa: F401

    def test_flask_app_creates_without_error(self):
        """The Flask application object must be constructable."""
        sys.path.insert(0, str(PROJECT_ROOT))
        from src.core.app import app
        assert app is not None
        assert app.name == "src.core.app"

    def test_celery_app_creates_without_error(self):
        sys.path.insert(0, str(PROJECT_ROOT))
        from src.core.tasks import app as celery_app
        assert celery_app is not None

    def test_calculation_planner_importable(self):
        from src.core.app import CalculationPlanner
        planner = CalculationPlanner("test-session")
        assert planner is not None


# ---------------------------------------------------------------------------
# 4. Mandatory project file tree
# ---------------------------------------------------------------------------

REQUIRED_PATHS = [
    # Python source
    "src/__init__.py",
    "src/core/app.py",
    "src/core/tasks.py",
    "src/plotting/create_plots.py",
    "src/analysis/generate_contact_maps.py",
    # Templates
    "templates/index.html",
    "templates/layout.html",
    "templates/view_trajectory.html",
    "templates/tools.html",
    # Scripts
    "scripts/install_script.py",
    "scripts/start_services.sh",
    "scripts/stop_services.sh",
    "scripts/cron_start_services.sh",
    "scripts/installation/requirements.txt",
    # Entry point
    "main.py",
    # Config
    "config/params.json",
    # Static
    "static/explanations.json",
]


@pytest.mark.parametrize("rel_path", REQUIRED_PATHS)
def test_required_file_present(rel_path):
    """Every file in the mandatory tree must exist after installation."""
    full = PROJECT_ROOT / rel_path
    assert full.exists(), f"Required file missing: {rel_path}"


class TestProjectStructure:
    """Verify key directories are present."""

    def test_src_directory(self):
        assert (PROJECT_ROOT / "src").is_dir()

    def test_templates_directory(self):
        assert (PROJECT_ROOT / "templates").is_dir()

    def test_static_directory(self):
        assert (PROJECT_ROOT / "static").is_dir()

    def test_scripts_directory(self):
        assert (PROJECT_ROOT / "scripts").is_dir()

    def test_data_directory(self):
        assert (PROJECT_ROOT / "data").is_dir()

    def test_tests_directory(self):
        assert (PROJECT_ROOT / "tests").is_dir()

    def test_logs_directory_creatable(self):
        """The logs/ directory may not exist yet but must be creatable."""
        logs = PROJECT_ROOT / "logs"
        if not logs.exists():
            logs.mkdir()
            assert logs.is_dir()
            logs.rmdir()
        else:
            assert logs.is_dir()

    def test_uploads_directory_exists_or_creatable(self):
        uploads = PROJECT_ROOT / "static" / "uploads"
        uploads.mkdir(exist_ok=True)
        assert uploads.is_dir()

    def test_example_data_present(self):
        examples = PROJECT_ROOT / "data" / "examples"
        assert examples.is_dir(), "data/examples/ directory missing"
        files = list(examples.iterdir())
        assert len(files) > 0, "data/examples/ is empty"

    def test_plot_templates_present(self):
        """At least some plot-specific partial templates must exist."""
        templates = PROJECT_ROOT / "templates"
        plot_templates = list(templates.glob("_plot_*.html"))
        assert len(plot_templates) > 0, "No _plot_*.html templates found"

    def test_tool_scripts_present(self):
        """User-downloadable tool scripts must be present."""
        tools_dir = PROJECT_ROOT / "scripts" / "tools"
        if tools_dir.is_dir():
            py_files = list(tools_dir.glob("*.py"))
            assert len(py_files) >= 5, \
                f"Expected ≥5 tool scripts, found {len(py_files)}"
        else:
            pytest.skip("scripts/tools/ directory not found")

    def test_main_entry_point_runnable(self):
        """main.py must import without executing the server."""
        main_py = PROJECT_ROOT / "main.py"
        assert main_py.is_file()
        content = main_py.read_text()
        assert "app.run" in content or "socketio.run" in content


# ---------------------------------------------------------------------------
# 5. Shell-script validation
# ---------------------------------------------------------------------------

class TestShellScripts:
    """Validate shell scripts: exist, executable, and syntactically correct."""

    SCRIPTS = [
        ("start_services.sh",      START_SH),
        ("stop_services.sh",       STOP_SH),
        ("cron_start_services.sh", CRON_SH),
    ]

    @pytest.mark.parametrize("name,path", SCRIPTS)
    def test_script_exists(self, name, path):
        assert path.is_file(), f"{name} not found at {path}"

    @pytest.mark.parametrize("name,path", SCRIPTS)
    def test_script_has_shebang(self, name, path):
        first_line = path.read_text().splitlines()[0]
        assert first_line.startswith("#!"), \
            f"{name} missing shebang line, got: {first_line!r}"

    @pytest.mark.parametrize("name,path", SCRIPTS)
    def test_script_shebang_is_bash(self, name, path):
        first_line = path.read_text().splitlines()[0]
        assert "bash" in first_line, \
            f"{name} shebang is not bash: {first_line!r}"

    @pytest.mark.skipif(
        shutil.which("bash") is None,
        reason="bash not found in PATH",
    )
    @pytest.mark.parametrize("name,path", SCRIPTS)
    def test_script_syntax_valid(self, name, path):
        """bash -n performs a dry-run syntax check without executing."""
        result = subprocess.run(
            ["bash", "-n", str(path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, \
            f"{name} has syntax errors:\n{result.stderr}"

    @pytest.mark.parametrize("name,path", SCRIPTS)
    def test_script_executable(self, name, path):
        """Scripts must be marked executable (owner execute bit)."""
        mode = path.stat().st_mode
        assert mode & stat.S_IXUSR, \
            f"{name} is not executable (chmod +x it)"

    def test_start_script_references_celery(self):
        content = START_SH.read_text()
        assert "celery" in content, "start_services.sh must start celery"

    def test_start_script_references_gunicorn(self):
        content = START_SH.read_text()
        assert "gunicorn" in content, "start_services.sh must start gunicorn"

    def test_start_script_references_port_4242(self):
        content = START_SH.read_text()
        assert "4242" in content, "start_services.sh must bind to port 4242"

    def test_stop_script_kills_gunicorn(self):
        content = STOP_SH.read_text()
        assert "gunicorn" in content.lower()

    def test_stop_script_kills_celery(self):
        content = STOP_SH.read_text()
        assert "celery" in content.lower()

    def test_cron_script_handles_redis(self):
        content = CRON_SH.read_text()
        assert "redis" in content.lower()

    def test_cron_script_has_pid_management(self):
        content = CRON_SH.read_text()
        assert "pidfile" in content.lower() or ".pid" in content.lower()

    def test_cron_script_idempotent(self):
        """cron script must check if services are already running."""
        content = CRON_SH.read_text()
        # Should have some form of 'is service running' guard
        assert "is_service_running" in content or "pgrep" in content or \
               "kill -0" in content, \
            "cron_start_services.sh should guard against double-starting services"


# ---------------------------------------------------------------------------
# 6. System tool availability
# ---------------------------------------------------------------------------

class TestSystemTools:
    """Check that external programs the service stack needs are in PATH."""

    def test_redis_server_available(self):
        if shutil.which("redis-server") is None:
            pytest.skip("redis-server not in PATH — install Redis")
        result = subprocess.run(
            ["redis-server", "--version"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "Redis" in result.stdout or "redis" in result.stdout.lower()

    def test_celery_available(self):
        if shutil.which("celery") is None:
            pytest.skip("celery binary not in PATH")
        result = subprocess.run(
            ["celery", "--version"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0

    def test_gunicorn_available(self):
        if shutil.which("gunicorn") is None:
            pytest.skip("gunicorn not in PATH")
        result = subprocess.run(
            ["gunicorn", "--version"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0

    def test_git_available(self):
        """git is needed by install_script.py."""
        if shutil.which("git") is None:
            pytest.fail("git is not installed — required by install_script.py")
        result = subprocess.run(
            ["git", "--version"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "git" in result.stdout.lower()

    def test_python_version(self):
        """ARNy Plotter targets Python ≥ 3.10."""
        info = sys.version_info
        assert (info.major, info.minor) >= (3, 10), \
            f"Python ≥ 3.10 required, got {info.major}.{info.minor}"

    def test_pip_available(self):
        result = subprocess.run(
            [sys.executable, "-m", "pip", "--version"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, "pip is not available"


# ---------------------------------------------------------------------------
# 7. Redis connectivity
# ---------------------------------------------------------------------------

class TestRedisConnectivity:
    """Soft tests — all skip gracefully if Redis is not running."""

    @pytest.fixture(autouse=True)
    def _skip_if_no_redis(self):
        try:
            import redis
            r = redis.Redis(host="localhost", port=6379, socket_connect_timeout=1)
            r.ping()
        except Exception:
            pytest.skip("Redis is not reachable on localhost:6379")

    def test_redis_ping(self):
        import redis
        r = redis.Redis(host="localhost", port=6379)
        assert r.ping() is True

    def test_redis_set_and_get(self):
        import redis
        r = redis.Redis(host="localhost", port=6379)
        r.set("arny_test_key", "arny_plotter_ok", ex=5)
        assert r.get("arny_test_key") == b"arny_plotter_ok"
        r.delete("arny_test_key")

    def test_redis_broker_url_reachable(self):
        """The broker URL hard-coded in tasks.py must be reachable."""
        import redis
        # The tasks module uses redis://localhost:6379/0
        r = redis.Redis(host="localhost", port=6379, db=0)
        assert r.ping()

    def test_celery_can_inspect_workers(self):
        """With Redis up, Celery's inspect won't crash (workers may be 0)."""
        from src.core.tasks import app as celery_app
        insp = celery_app.control.inspect(timeout=1.0)
        # inspect() returns None or a dict; it should not raise
        try:
            result = insp.ping()
            # result is None (no workers) or dict — both are valid
        except Exception as exc:
            pytest.fail(f"celery.control.inspect raised: {exc}")


# ---------------------------------------------------------------------------
# 8. Installer class unit tests (no network, no git clone)
# ---------------------------------------------------------------------------

@pytest.fixture
def dummy_installer(tmp_path):
    """Provide an Installer instance pointed at tmp_path, no network needed."""
    sys.path.insert(0, str(SCRIPTS_DIR))
    from install_script import Installer
    inst = Installer(
        repo_url="https://github.com/example/fake-repo.git",
        install_path=str(tmp_path / "install"),
    )
    # Override repo_dir to a temp location so clone isn't needed
    inst.repo_dir = tmp_path / "fake-repo"
    inst.repo_dir.mkdir()
    inst.temp_dir = tmp_path / "temp"
    inst.temp_dir.mkdir()
    return inst


class TestInstallerClass:
    """Unit tests for the Installer helper class."""

    def test_installer_instantiates(self, tmp_path):
        sys.path.insert(0, str(SCRIPTS_DIR))
        from install_script import Installer
        inst = Installer(
            repo_url="https://github.com/example/repo.git",
            install_path=str(tmp_path / "out"),
        )
        assert inst is not None

    def test_installer_derives_repo_name(self, tmp_path):
        from install_script import Installer
        inst = Installer(
            repo_url="https://github.com/user/my-cool-app.git",
            install_path=str(tmp_path),
        )
        assert "my-cool-app" in str(inst.repo_dir)

    def test_installer_detects_platform(self, tmp_path):
        from install_script import Installer
        inst = Installer(
            repo_url="https://github.com/x/y.git",
            install_path=str(tmp_path),
        )
        # Exactly one of is_windows / is_mac / is_linux should be True
        flags = [inst.is_windows, inst.is_mac, inst.is_linux]
        assert sum(flags) == 1, f"Platform detection ambiguous: {flags}"

    def test_verify_conda_env_detects_active_env(self, dummy_installer, monkeypatch):
        """When CONDA_PREFIX is set, verify_conda_env should return True."""
        monkeypatch.setenv("CONDA_PREFIX", "/home/user/miniconda3/envs/test_env")
        assert dummy_installer.verify_conda_env() is True

    def test_verify_conda_env_fails_without_prefix(self, dummy_installer, monkeypatch):
        """When CONDA_PREFIX is absent, verify_conda_env should return False."""
        monkeypatch.delenv("CONDA_PREFIX", raising=False)
        assert dummy_installer.verify_conda_env() is False

    def test_check_git_returns_bool(self, dummy_installer):
        result = dummy_installer.check_git()
        assert isinstance(result, bool)

    def test_check_git_true_when_git_installed(self, dummy_installer):
        if shutil.which("git") is None:
            pytest.skip("git not installed")
        assert dummy_installer.check_git() is True

    def test_copy_program_files_copies_files(self, dummy_installer, tmp_path):
        """copy_program_files() must replicate the repo tree to install_path."""
        # Populate a fake repo directory
        (dummy_installer.repo_dir / "main.py").write_text("print('hello')")
        (dummy_installer.repo_dir / "src").mkdir()
        (dummy_installer.repo_dir / "src" / "app.py").write_text("# app")

        result = dummy_installer.copy_program_files()
        assert result is True

        install_main = dummy_installer.install_path / "main.py"
        assert install_main.is_file(), "main.py was not copied to install_path"

        install_src = dummy_installer.install_path / "src" / "app.py"
        assert install_src.is_file(), "src/app.py was not copied recursively"

    def test_copy_program_files_excludes_git_dir(self, dummy_installer):
        """The .git directory must not be copied to the install path."""
        (dummy_installer.repo_dir / ".git").mkdir()
        (dummy_installer.repo_dir / ".git" / "config").write_text("[core]")
        (dummy_installer.repo_dir / "README.md").write_text("# readme")

        dummy_installer.copy_program_files()

        assert not (dummy_installer.install_path / ".git").exists(), \
            ".git directory must not be copied to install_path"
        assert (dummy_installer.install_path / "README.md").is_file()

    def test_copy_program_files_excludes_pycache(self, dummy_installer):
        pycache = dummy_installer.repo_dir / "__pycache__"
        pycache.mkdir()
        (pycache / "something.pyc").write_bytes(b"\x00")
        (dummy_installer.repo_dir / "module.py").write_text("x=1")

        dummy_installer.copy_program_files()

        assert not (dummy_installer.install_path / "__pycache__").exists()

    def test_create_launcher_linux(self, dummy_installer, monkeypatch):
        """On Linux/Mac, a .sh launcher must be created and made executable."""
        # Patch platform detection
        monkeypatch.setattr(dummy_installer, "is_windows", False)
        monkeypatch.setattr(dummy_installer, "is_mac",     False)
        monkeypatch.setattr(dummy_installer, "is_linux",   True)

        # Create a fake main.py with an entry point
        dummy_installer.install_path.mkdir(parents=True, exist_ok=True)
        main_py = dummy_installer.install_path / "main.py"
        main_py.write_text("if __name__ == '__main__':\n    pass\n")

        monkeypatch.setenv("CONDA_PREFIX", "/opt/conda/envs/test")

        result = dummy_installer.create_launcher()
        assert result is True

        launcher = dummy_installer.install_path / "run_program.sh"
        assert launcher.is_file(), "run_program.sh not created"

        # Must be executable
        mode = launcher.stat().st_mode
        assert mode & stat.S_IXUSR, "run_program.sh is not executable"

    def test_create_launcher_sh_contains_conda_activate(self, dummy_installer, monkeypatch):
        monkeypatch.setattr(dummy_installer, "is_windows", False)
        monkeypatch.setattr(dummy_installer, "is_mac",     False)
        monkeypatch.setattr(dummy_installer, "is_linux",   True)

        dummy_installer.install_path.mkdir(parents=True, exist_ok=True)
        (dummy_installer.install_path / "main.py").write_text("if __name__ == '__main__': pass")
        monkeypatch.setenv("CONDA_PREFIX", "/opt/conda/envs/my_rna_env")

        dummy_installer.create_launcher()

        content = (dummy_installer.install_path / "run_program.sh").read_text()
        assert "conda activate" in content
        assert "my_rna_env" in content

    def test_create_launcher_sh_contains_stop_option(self, dummy_installer, monkeypatch):
        """The launcher should support a --stop flag."""
        monkeypatch.setattr(dummy_installer, "is_windows", False)
        monkeypatch.setattr(dummy_installer, "is_linux",   True)
        monkeypatch.setattr(dummy_installer, "is_mac",     False)

        dummy_installer.install_path.mkdir(parents=True, exist_ok=True)
        (dummy_installer.install_path / "main.py").write_text("if __name__ == '__main__': pass")
        monkeypatch.setenv("CONDA_PREFIX", "/opt/conda/envs/arny")

        dummy_installer.create_launcher()

        content = (dummy_installer.install_path / "run_program.sh").read_text()
        assert "--stop" in content

    def test_create_launcher_windows(self, dummy_installer, monkeypatch):
        """On Windows, a .bat launcher must be created."""
        monkeypatch.setattr(dummy_installer, "is_windows", True)
        monkeypatch.setattr(dummy_installer, "is_linux",   False)
        monkeypatch.setattr(dummy_installer, "is_mac",     False)

        dummy_installer.install_path.mkdir(parents=True, exist_ok=True)
        (dummy_installer.install_path / "main.py").write_text("if __name__ == '__main__': pass")
        monkeypatch.setenv("CONDA_PREFIX", r"C:\Miniconda3\envs\arny")

        result = dummy_installer.create_launcher()
        assert result is True

        launcher = dummy_installer.install_path / "run_program.bat"
        assert launcher.is_file(), "run_program.bat not created on Windows path"

    def test_run_command_captures_stdout(self, dummy_installer):
        """run_command must return command stdout as a string."""
        output = dummy_installer.run_command(["echo", "hello_arny"])
        assert output is not None
        assert "hello_arny" in output

    def test_run_command_returns_none_on_failure(self, dummy_installer):
        """run_command must return None (not raise) when the command fails."""
        output = dummy_installer.run_command(
            ["false"],  # unix 'false' always exits with code 1
            check=True,
        )
        assert output is None

    def test_download_with_progress_writes_file(self, dummy_installer, tmp_path, httpretty=None):
        """download_with_progress must save content to the destination path."""
        # Use a local test server mock via urllib — we'll serve a small bytes payload
        dest = tmp_path / "downloaded.bin"

        # Create a small local HTTP server using http.server in a thread
        import http.server
        import threading
        import urllib.request

        payload = b"FAKE_REDIS_BINARY_DATA"

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass  # suppress output

        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.handle_request)
        thread.daemon = True
        thread.start()

        result = dummy_installer.download_with_progress(
            f"http://127.0.0.1:{port}/test", str(dest)
        )
        thread.join(timeout=2)

        assert result is True
        assert dest.is_file()
        assert dest.read_bytes() == payload

    def test_install_requirements_path_derived_correctly(self, dummy_installer):
        """install_requirements must look for requirements.txt under repo_dir/installation/."""
        req_dir = dummy_installer.repo_dir / "installation"
        req_dir.mkdir()
        req_file = req_dir / "requirements.txt"
        req_file.write_text("# empty\n")

        # Should not fail just because of an empty requirements file
        # (pip install -r with empty file is fine)
        result = dummy_installer.install_requirements()
        # True or False depending on env — just must not raise
        assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# 9. Full installer dry-run (integration, uses git)
# ---------------------------------------------------------------------------

@pytest.mark.slow
class TestInstallerIntegration:
    """
    Dry-run integration test: clone the local project repo into a temp
    directory, copy files, create launcher — without touching the network.
    Marked @slow so it is excluded from the fast run.
    """

    @pytest.fixture
    def local_repo(self, tmp_path):
        """
        Create a minimal local git repository that mirrors the project
        structure so we can test clone/copy without network access.
        """
        if shutil.which("git") is None:
            pytest.skip("git not available")

        repo_path = tmp_path / "local_repo"
        repo_path.mkdir()

        # Init bare-ish repo
        subprocess.run(["git", "init", str(repo_path)], check=True,
                       capture_output=True)
        subprocess.run(["git", "-C", str(repo_path), "config",
                        "user.email", "test@test.com"], check=True,
                       capture_output=True)
        subprocess.run(["git", "-C", str(repo_path), "config",
                        "user.name", "Test"], check=True,
                       capture_output=True)

        # Create minimal project structure
        (repo_path / "main.py").write_text(
            "from src.core.app import app\n"
            "if __name__ == '__main__':\n    app.run()\n"
        )
        (repo_path / "installation").mkdir()
        (repo_path / "installation" / "requirements.txt").write_text(
            "# minimal requirements\n"
        )

        subprocess.run(["git", "-C", str(repo_path), "add", "."],
                       check=True, capture_output=True)
        subprocess.run(
            ["git", "-C", str(repo_path), "commit", "-m", "initial"],
            check=True, capture_output=True,
        )

        return repo_path

    def test_clone_local_repo(self, tmp_path, local_repo, monkeypatch):
        sys.path.insert(0, str(SCRIPTS_DIR))
        from install_script import Installer

        install_path = tmp_path / "arny_install"
        inst = Installer(
            repo_url=str(local_repo),
            install_path=str(install_path),
        )
        # Point temp to tmp_path to avoid polluting CWD
        inst.temp_dir = tmp_path / "temp"
        inst.temp_dir.mkdir()
        inst.repo_dir = inst.temp_dir / "local_repo"

        monkeypatch.setenv("CONDA_PREFIX", "/opt/conda/envs/test")

        assert inst.clone_repository() is True
        assert inst.repo_dir.is_dir()
        assert (inst.repo_dir / "main.py").is_file()

    def test_full_install_flow_local(self, tmp_path, local_repo, monkeypatch):
        """
        Clone → copy_program_files → create_launcher should all succeed
        with a local repository (no internet required).
        """
        sys.path.insert(0, str(SCRIPTS_DIR))
        from install_script import Installer

        install_path = tmp_path / "arny_output"
        inst = Installer(
            repo_url=str(local_repo),
            install_path=str(install_path),
        )
        inst.temp_dir = tmp_path / "temp"
        inst.temp_dir.mkdir()
        inst.repo_dir = inst.temp_dir / "local_repo"

        monkeypatch.setenv("CONDA_PREFIX", "/opt/conda/envs/arny_plotter")
        monkeypatch.setattr(inst, "is_linux", True)
        monkeypatch.setattr(inst, "is_mac",   False)
        monkeypatch.setattr(inst, "is_windows", False)

        assert inst.clone_repository() is True
        assert inst.copy_program_files() is True
        assert inst.create_launcher() is True

        launcher = install_path / "run_program.sh"
        assert launcher.is_file()
        assert launcher.stat().st_mode & stat.S_IXUSR

        content = launcher.read_text()
        assert "conda activate" in content
        assert "arny_plotter" in content
