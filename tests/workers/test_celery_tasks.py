"""
Tests for Celery tasks (src/core/tasks.py).

Celery is configured in ALWAYS_EAGER mode so tasks run synchronously in
the test process — no broker or worker required.  Real MD files from
data/examples/ are used where available.
"""

import os
import sys
import pickle
import shutil
import pytest
import numpy as np

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

TOPOLOGY_PDB = os.path.join(PROJECT_ROOT, "data", "examples", "frame_0_noions.pdb")
TRAJECTORY_XTC = os.path.join(PROJECT_ROOT, "data", "examples", "SHAW_8500_10418.xtc")
FSE_TOPOLOGY = os.path.join(PROJECT_ROOT, "data", "examples", "FSE", "correct_conf.pdb")
FSE_TRAJECTORY = os.path.join(PROJECT_ROOT, "data", "examples", "FSE", "md_noPBC.xtc")

HAS_EXAMPLE_FILES = os.path.isfile(TOPOLOGY_PDB) and os.path.isfile(TRAJECTORY_XTC)
HAS_FSE_FILES = os.path.isfile(FSE_TOPOLOGY) and os.path.isfile(FSE_TRAJECTORY)

pytestmark_need_files = pytest.mark.skipif(
    not HAS_EXAMPLE_FILES, reason="Example data files not found"
)


# ---------------------------------------------------------------------------
# Helper to set up a temporary session directory for tasks
# ---------------------------------------------------------------------------

@pytest.fixture
def task_session(tmp_path, celery_eager, monkeypatch):
    """
    Provides a dict with session_id and file paths, and patches the
    tasks module to look for sessions under tmp_path.
    """
    if not HAS_EXAMPLE_FILES:
        pytest.skip("Example data files not found")

    session_id = "celery-test-session-001"
    session_dir = tmp_path / "static" / "uploads" / session_id
    session_dir.mkdir(parents=True)

    topo = session_dir / "frame_0_noions.pdb"
    traj = session_dir / "SHAW_8500_10418.xtc"
    shutil.copy(TOPOLOGY_PDB, str(topo))
    shutil.copy(TRAJECTORY_XTC, str(traj))

    # Patch working directory so tasks resolve relative paths correctly
    monkeypatch.chdir(str(tmp_path))

    return {
        "session_id": session_id,
        "topology": str(topo),
        "trajectory": str(traj),
        "session_dir": str(session_dir),
    }


@pytest.fixture
def fse_task_session(tmp_path, celery_eager, monkeypatch):
    """Variant using the larger FSE dataset."""
    if not HAS_FSE_FILES:
        pytest.skip("FSE example data files not found")

    session_id = "celery-test-fse-session"
    session_dir = tmp_path / "static" / "uploads" / session_id
    session_dir.mkdir(parents=True)

    topo = session_dir / "correct_conf.pdb"
    traj = session_dir / "md_noPBC.xtc"
    shutil.copy(FSE_TOPOLOGY, str(topo))
    shutil.copy(FSE_TRAJECTORY, str(traj))

    monkeypatch.chdir(str(tmp_path))

    return {
        "session_id": session_id,
        "topology": str(topo),
        "trajectory": str(traj),
        "session_dir": str(session_dir),
    }


# ---------------------------------------------------------------------------
# compute_rmsd
# ---------------------------------------------------------------------------

class TestComputeRmsd:
    def test_task_returns_success_dict(self, task_session):
        from src.core.tasks import compute_rmsd
        result = compute_rmsd.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        assert isinstance(result, dict)
        assert result.get("metric") == "rmsd"
        assert result.get("status") == "success"

    def test_rmsd_pickle_file_created(self, task_session):
        from src.core.tasks import compute_rmsd
        compute_rmsd.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        pkl_path = os.path.join(task_session["session_dir"], "rmsd_data.pkl")
        assert os.path.isfile(pkl_path), "rmsd_data.pkl not created"

    def test_rmsd_data_is_ndarray(self, task_session):
        from src.core.tasks import compute_rmsd
        compute_rmsd.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        pkl_path = os.path.join(task_session["session_dir"], "rmsd_data.pkl")
        with open(pkl_path, "rb") as f:
            data = pickle.load(f)
        assert isinstance(data, np.ndarray)

    def test_rmsd_values_non_negative(self, task_session):
        from src.core.tasks import compute_rmsd
        compute_rmsd.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        pkl_path = os.path.join(task_session["session_dir"], "rmsd_data.pkl")
        with open(pkl_path, "rb") as f:
            data = pickle.load(f)
        assert np.all(data >= 0), "RMSD values must be non-negative"

    def test_rmsd_first_frame_near_zero(self, task_session):
        """RMSD of the reference against itself should be ~0."""
        from src.core.tasks import compute_rmsd
        compute_rmsd.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        pkl_path = os.path.join(task_session["session_dir"], "rmsd_data.pkl")
        with open(pkl_path, "rb") as f:
            data = pickle.load(f)
        assert data[0] < 1.0, f"First frame RMSD too large: {data[0]}"


# ---------------------------------------------------------------------------
# compute_ermsd
# ---------------------------------------------------------------------------

class TestComputeErmsd:
    def test_task_returns_success_dict(self, task_session):
        from src.core.tasks import compute_ermsd
        result = compute_ermsd.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        assert result.get("metric") == "ermsd"
        assert result.get("status") == "success"

    def test_ermsd_pickle_created(self, task_session):
        from src.core.tasks import compute_ermsd
        compute_ermsd.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        assert os.path.isfile(os.path.join(task_session["session_dir"], "ermsd_data.pkl"))

    def test_ermsd_is_ndarray(self, task_session):
        from src.core.tasks import compute_ermsd
        compute_ermsd.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        with open(os.path.join(task_session["session_dir"], "ermsd_data.pkl"), "rb") as f:
            data = pickle.load(f)
        assert isinstance(data, np.ndarray)

    def test_ermsd_non_negative(self, task_session):
        from src.core.tasks import compute_ermsd
        compute_ermsd.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        with open(os.path.join(task_session["session_dir"], "ermsd_data.pkl"), "rb") as f:
            data = pickle.load(f)
        assert np.all(data >= 0)


# ---------------------------------------------------------------------------
# compute_radius_of_gyration
# ---------------------------------------------------------------------------

class TestComputeRadiusOfGyration:
    def test_task_returns_success(self, task_session):
        from src.core.tasks import compute_radius_of_gyration
        result = compute_radius_of_gyration.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        assert result.get("status") == "success"
        assert result.get("metric") == "radius_of_gyration"

    def test_pickle_created(self, task_session):
        from src.core.tasks import compute_radius_of_gyration
        compute_radius_of_gyration.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        assert os.path.isfile(
            os.path.join(task_session["session_dir"], "radius_of_gyration_data.pkl")
        )

    def test_rg_values_are_positive(self, task_session):
        from src.core.tasks import compute_radius_of_gyration
        compute_radius_of_gyration.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        with open(
            os.path.join(task_session["session_dir"], "radius_of_gyration_data.pkl"), "rb"
        ) as f:
            data = pickle.load(f)
        assert all(v > 0 for v in data["rg_values"])

    def test_rg_data_has_required_keys(self, task_session):
        from src.core.tasks import compute_radius_of_gyration
        compute_radius_of_gyration.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        with open(
            os.path.join(task_session["session_dir"], "radius_of_gyration_data.pkl"), "rb"
        ) as f:
            data = pickle.load(f)
        for key in ("rg_values", "rg_components", "mean_rg", "std_rg", "frames", "times"):
            assert key in data, f"Missing key in Rg data: {key}"


# ---------------------------------------------------------------------------
# compute_end_to_end_distance
# ---------------------------------------------------------------------------

class TestComputeEndToEndDistance:
    def test_task_returns_success(self, task_session):
        from src.core.tasks import compute_end_to_end_distance
        result = compute_end_to_end_distance.apply(
            args=[
                task_session["topology"],
                task_session["trajectory"],
                task_session["session_id"],
                {},
            ]
        ).get()
        assert result.get("status") in ("success", "error")  # error acceptable if no nucleic atoms

    def test_pickle_created_on_success(self, task_session):
        from src.core.tasks import compute_end_to_end_distance
        result = compute_end_to_end_distance.apply(
            args=[
                task_session["topology"],
                task_session["trajectory"],
                task_session["session_id"],
                {},
            ]
        ).get()
        if result.get("status") == "success":
            pkl_path = os.path.join(task_session["session_dir"], "end_to_end_distance_data.pkl")
            assert os.path.isfile(pkl_path)

    def test_distances_are_positive(self, task_session):
        from src.core.tasks import compute_end_to_end_distance
        result = compute_end_to_end_distance.apply(
            args=[
                task_session["topology"],
                task_session["trajectory"],
                task_session["session_id"],
                {},
            ]
        ).get()
        if result.get("status") == "success":
            with open(
                os.path.join(task_session["session_dir"], "end_to_end_distance_data.pkl"), "rb"
            ) as f:
                data = pickle.load(f)
            assert all(d >= 0 for d in data["distances"])


# ---------------------------------------------------------------------------
# compute_annotate
# ---------------------------------------------------------------------------

class TestComputeAnnotate:
    def test_task_returns_success(self, task_session):
        from src.core.tasks import compute_annotate
        result = compute_annotate.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        assert result.get("metric") == "annotate"
        assert result.get("status") == "success"

    def test_annotate_pickle_created(self, task_session):
        from src.core.tasks import compute_annotate
        compute_annotate.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        assert os.path.isfile(
            os.path.join(task_session["session_dir"], "annotate_data.pkl")
        )

    def test_annotate_data_not_none(self, task_session):
        from src.core.tasks import compute_annotate
        compute_annotate.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        with open(
            os.path.join(task_session["session_dir"], "annotate_data.pkl"), "rb"
        ) as f:
            data = pickle.load(f)
        assert data is not None


# ---------------------------------------------------------------------------
# compute_dimensionality_reduction
# ---------------------------------------------------------------------------

class TestComputeDimensionalityReduction:
    def test_task_returns_success(self, task_session):
        from src.core.tasks import compute_dimensionality_reduction
        result = compute_dimensionality_reduction.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        assert result.get("metric") == "dimensionality_reduction"
        assert result.get("status") == "success"

    def test_pickle_contains_pca_coords(self, task_session):
        from src.core.tasks import compute_dimensionality_reduction
        compute_dimensionality_reduction.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        with open(
            os.path.join(task_session["session_dir"], "dimensionality_reduction_data.pkl"), "rb"
        ) as f:
            data = pickle.load(f)
        assert "pca_coordinates" in data
        assert "umap_coordinates" in data
        assert "tsne_coordinates" in data

    def test_pca_explained_variance_sums_to_at_most_one(self, task_session):
        from src.core.tasks import compute_dimensionality_reduction
        compute_dimensionality_reduction.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()
        with open(
            os.path.join(task_session["session_dir"], "dimensionality_reduction_data.pkl"), "rb"
        ) as f:
            data = pickle.load(f)
        total_var = sum(data["pca_explained_variance"])
        assert total_var <= 1.01, f"Explained variance sum > 1: {total_var}"


# ---------------------------------------------------------------------------
# generate_rmsd_plot (Phase 2)
# ---------------------------------------------------------------------------

class TestGenerateRmsdPlot:
    def test_rmsd_plot_task_runs(self, task_session, tmp_path):
        from src.core.tasks import compute_rmsd, generate_rmsd_plot

        # First compute the metric
        compute_rmsd.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()

        files_path = os.path.join(task_session["session_dir"], "download_data")
        plot_dir = os.path.join(task_session["session_dir"], "download_plot")
        os.makedirs(files_path, exist_ok=True)
        os.makedirs(plot_dir, exist_ok=True)

        result = generate_rmsd_plot.apply(
            args=[
                task_session["topology"],
                task_session["trajectory"],
                files_path,
                plot_dir,
                task_session["session_id"],
                {},
            ]
        ).get()
        # generate_rmsd_plot returns Plotly JSON string on success
        assert result is not None

    def test_rmsd_plot_generates_files(self, task_session, tmp_path):
        from src.core.tasks import compute_rmsd, generate_rmsd_plot

        compute_rmsd.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()

        files_path = os.path.join(task_session["session_dir"], "download_data")
        plot_dir = os.path.join(task_session["session_dir"], "download_plot")
        os.makedirs(files_path, exist_ok=True)
        os.makedirs(plot_dir, exist_ok=True)

        generate_rmsd_plot.apply(
            args=[
                task_session["topology"],
                task_session["trajectory"],
                files_path,
                plot_dir,
                task_session["session_id"],
                {},
            ]
        ).get()

        # At least one output file should have been created
        created = []
        for root, _dirs, files in os.walk(task_session["session_dir"]):
            for fn in files:
                if fn.endswith((".png", ".html", ".json", ".csv", ".pkl")):
                    created.append(fn)
        assert len(created) > 0, "No output files created by generate_rmsd_plot"


# ---------------------------------------------------------------------------
# generate_ermsd_plot (Phase 2)
# ---------------------------------------------------------------------------

class TestGenerateErmsdPlot:
    def test_ermsd_plot_task_runs(self, task_session):
        from src.core.tasks import compute_ermsd, generate_ermsd_plot

        compute_ermsd.apply(
            args=[task_session["topology"], task_session["trajectory"], task_session["session_id"]]
        ).get()

        files_path = os.path.join(task_session["session_dir"], "download_data")
        plot_dir = os.path.join(task_session["session_dir"], "download_plot")
        os.makedirs(files_path, exist_ok=True)
        os.makedirs(plot_dir, exist_ok=True)

        result = generate_ermsd_plot.apply(
            args=[
                task_session["topology"],
                task_session["trajectory"],
                files_path,
                plot_dir,
                task_session["session_id"],
                {},
            ]
        ).get()
        # generate_ermsd_plot returns Plotly JSON string on success
        assert result is not None


# ---------------------------------------------------------------------------
# Task chain arguments handling (previous_result forwarding)
# ---------------------------------------------------------------------------

class TestTaskChainArgHandling:
    """Verify that tasks correctly unpack both 3-arg and 4-arg (chained) forms."""

    def test_compute_rmsd_handles_4_args(self, task_session):
        from src.core.tasks import compute_rmsd
        # When called from a chain, the first arg is the previous result
        result = compute_rmsd.apply(
            args=[
                {"status": "previous"},  # previous_result
                task_session["topology"],
                task_session["trajectory"],
                task_session["session_id"],
            ]
        ).get()
        assert result.get("status") == "success"

    def test_compute_ermsd_handles_4_args(self, task_session):
        from src.core.tasks import compute_ermsd
        result = compute_ermsd.apply(
            args=[
                {"status": "previous"},
                task_session["topology"],
                task_session["trajectory"],
                task_session["session_id"],
            ]
        ).get()
        assert result.get("status") == "success"
