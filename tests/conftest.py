"""
Shared pytest fixtures for Plot_RNA6 test suite.
"""

import os
import sys
import json
import pickle
import shutil
import uuid
import tempfile

import numpy as np
import pytest

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ---------------------------------------------------------------------------
# Paths to example data shipped with the repo
# ---------------------------------------------------------------------------

DATA_DIR = os.path.join(PROJECT_ROOT, "data", "examples")
TOPOLOGY_PDB = os.path.join(DATA_DIR, "frame_0_noions.pdb")       # small PDB  (single frame)
TRAJECTORY_XTC = os.path.join(DATA_DIR, "SHAW_8500_10418.xtc")    # compact XTC
TOPOLOGY_AMBER = os.path.join(DATA_DIR, "01c3d102470b44129dbf078716c3bb5b", "RNAOPC.parm7")
TRAJECTORY_NC   = os.path.join(DATA_DIR, "01c3d102470b44129dbf078716c3bb5b", "small_NPT2.nc")
FSE_TOPOLOGY    = os.path.join(DATA_DIR, "FSE", "correct_conf.pdb")
FSE_TRAJECTORY  = os.path.join(DATA_DIR, "FSE", "md_noPBC.xtc")


def _have_example_files():
    return os.path.isfile(TOPOLOGY_PDB) and os.path.isfile(TRAJECTORY_XTC)


# ---------------------------------------------------------------------------
# Flask application fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def flask_app():
    """Create a Flask test-client application with a temp uploads directory."""
    os.environ["EVENTLET_NO_GREENDNS"] = "1"

    from src.core.app import app as _app

    _app.config["TESTING"] = True
    _app.config["SECRET_KEY"] = "test_secret"
    # Disable SocketIO in tests
    _app.config["WTF_CSRF_ENABLED"] = False

    yield _app


@pytest.fixture
def client(flask_app):
    """Return a Flask test client with a fresh session."""
    with flask_app.test_client() as c:
        with flask_app.app_context():
            yield c


# ---------------------------------------------------------------------------
# Uploads directory / session fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_uploads(tmp_path, monkeypatch):
    """Redirect uploads to a temporary directory for isolation."""
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    import src.core.app as app_module
    monkeypatch.setattr(app_module, "uploads_dir", str(uploads))
    return uploads


@pytest.fixture
def session_id():
    """Return a fresh hex session ID."""
    return uuid.uuid4().hex


@pytest.fixture
def session_dir(tmp_uploads, session_id):
    """Create a session subdirectory under tmp_uploads."""
    d = tmp_uploads / session_id
    d.mkdir()
    return d


@pytest.fixture
def populated_session(session_dir, session_id):
    """
    Create a session directory with example files copied in and a
    minimal session_data.json, ready for route / worker tests.
    Skips if example files are not present.
    """
    if not _have_example_files():
        pytest.skip("Example data files not found – skipping populated session fixture.")

    topo_dest = session_dir / "frame_0_noions.pdb"
    traj_dest = session_dir / "SHAW_8500_10418.xtc"
    shutil.copy(TOPOLOGY_PDB, str(topo_dest))
    shutil.copy(TRAJECTORY_XTC, str(traj_dest))

    session_data = {
        "selected_plots": ["RMSD"],
        "n_frames": 10,
        "frame_range": "all",
        "torsionResidue": 0,
        "torsionResidues": [],
        "torsionAngles": [],
        "torsionMode": "single",
        "landscape_stride": 0,
        "landscape_first_component": 1,
        "landscape_second_component": 2,
        "plot_settings": {},
        "form": [],
        "files": {
            "nativePdb": "frame_0_noions.pdb",
            "trajXtc": "SHAW_8500_10418.xtc",
            "trajpdb": "SHAW_8500_10418.xtc",
        },
    }
    with open(str(session_dir / "session_data.json"), "w") as f:
        json.dump(session_data, f)

    return {
        "session_id": session_id,
        "session_dir": str(session_dir),
        "topology": str(topo_dest),
        "trajectory": str(traj_dest),
        "session_data": session_data,
    }


# ---------------------------------------------------------------------------
# Numpy array fixtures for unit tests (no disk I/O required)
# ---------------------------------------------------------------------------

@pytest.fixture
def rmsd_array():
    """Simulated RMSD values for 100 frames."""
    rng = np.random.default_rng(42)
    return rng.uniform(0.5, 5.0, size=100)


@pytest.fixture
def ermsd_array():
    """Simulated eRMSD values for 100 frames."""
    rng = np.random.default_rng(42)
    return rng.uniform(0.0, 3.0, size=100)


@pytest.fixture
def rg_data():
    """Simulated radius-of-gyration data dict."""
    rng = np.random.default_rng(42)
    rg_vals = rng.uniform(10.0, 20.0, size=50).tolist()
    return {
        "rg_values": rg_vals,
        "rg_components": [[v * 0.6, v * 0.5, v * 0.4] for v in rg_vals],
        "mean_rg": float(np.mean(rg_vals)),
        "std_rg": float(np.std(rg_vals)),
        "frames": list(range(50)),
        "times": [i * 0.1 for i in range(50)],
    }


@pytest.fixture
def e2e_data():
    """Simulated end-to-end distance data dict."""
    rng = np.random.default_rng(42)
    dists = rng.uniform(30.0, 80.0, size=50).tolist()
    return {
        "distances": dists,
        "mean_distance": float(np.mean(dists)),
        "std_distance": float(np.std(dists)),
        "min_distance": float(np.min(dists)),
        "max_distance": float(np.max(dists)),
        "frames": list(range(50)),
        "times": [i * 0.1 for i in range(50)],
    }


@pytest.fixture
def dimred_data():
    """Simulated dimensionality-reduction data dict."""
    rng = np.random.default_rng(42)
    n = 50
    return {
        "pca_coordinates": rng.standard_normal((n, 3)).tolist(),
        "pca_explained_variance": [0.4, 0.2, 0.1, 0.05, 0.04, 0.03, 0.02, 0.01, 0.005, 0.005],
        "umap_coordinates": rng.standard_normal((n, 2)).tolist(),
        "tsne_coordinates": rng.standard_normal((n, 2)).tolist(),
        "frames": list(range(n)),
        "times": [i * 0.1 for i in range(n)],
    }


# ---------------------------------------------------------------------------
# Celery task eager-mode fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def celery_eager(monkeypatch):
    """
    Force Celery to execute tasks synchronously (ALWAYS_EAGER = True).
    This lets us test task logic without a running broker.
    """
    from src.core import tasks as tasks_module
    celery_app = tasks_module.app
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = True
    yield celery_app
    celery_app.conf.task_always_eager = False
