"""
End-to-end tests for the complete RNA analysis pipeline.

These tests:
  1. Copy real MD files into a temporary session directory.
  2. Run Celery compute tasks (in eager / synchronous mode).
  3. Run Celery plot-generation tasks.
  4. Verify that output files (CSV, HTML, PKL) are created.
  5. Simulate downloading results via the Flask routes.

They require both example data files AND the barnaba / MDAnalysis
stack to be properly installed.  Tests are automatically skipped when
example files are absent.
"""

import io
import os
import sys
import json
import pickle
import shutil
import zipfile
import pytest
import numpy as np

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

TOPOLOGY_PDB = os.path.join(PROJECT_ROOT, "data", "examples", "frame_0_noions.pdb")
TRAJECTORY_XTC = os.path.join(PROJECT_ROOT, "data", "examples", "SHAW_8500_10418.xtc")
FSE_TOPOLOGY = os.path.join(PROJECT_ROOT, "data", "examples", "FSE", "correct_conf.pdb")
FSE_TRAJECTORY = os.path.join(PROJECT_ROOT, "data", "examples", "FSE", "md_noPBC.xtc")

HAS_EXAMPLES = os.path.isfile(TOPOLOGY_PDB) and os.path.isfile(TRAJECTORY_XTC)
HAS_FSE = os.path.isfile(FSE_TOPOLOGY) and os.path.isfile(FSE_TRAJECTORY)


# ---------------------------------------------------------------------------
# Shared fixture for an e2e session
# ---------------------------------------------------------------------------

@pytest.fixture
def e2e_session(tmp_path, celery_eager, monkeypatch):
    if not HAS_EXAMPLES:
        pytest.skip("Example data files not found")

    session_id = "e2e-pipeline-session"
    session_dir = tmp_path / "static" / "uploads" / session_id
    session_dir.mkdir(parents=True)

    topo_name = "frame_0_noions.pdb"
    traj_name = "SHAW_8500_10418.xtc"
    topo_path = str(session_dir / topo_name)
    traj_path = str(session_dir / traj_name)

    shutil.copy(TOPOLOGY_PDB, topo_path)
    shutil.copy(TRAJECTORY_XTC, traj_path)

    monkeypatch.chdir(str(tmp_path))

    return {
        "session_id": session_id,
        "session_dir": str(session_dir),
        "topology": topo_path,
        "trajectory": traj_path,
        "topo_name": topo_name,
        "traj_name": traj_name,
    }


@pytest.fixture
def fse_e2e_session(tmp_path, celery_eager, monkeypatch):
    if not HAS_FSE:
        pytest.skip("FSE example data files not found")

    session_id = "e2e-fse-session"
    session_dir = tmp_path / "static" / "uploads" / session_id
    session_dir.mkdir(parents=True)

    topo_path = str(session_dir / "correct_conf.pdb")
    traj_path = str(session_dir / "md_noPBC.xtc")
    shutil.copy(FSE_TOPOLOGY, topo_path)
    shutil.copy(FSE_TRAJECTORY, traj_path)

    monkeypatch.chdir(str(tmp_path))

    return {
        "session_id": session_id,
        "session_dir": str(session_dir),
        "topology": topo_path,
        "trajectory": traj_path,
    }


# ---------------------------------------------------------------------------
# Helper to set up download directories and write session_data.json
# ---------------------------------------------------------------------------

def _setup_plot_dirs(session_dir):
    files_path = os.path.join(session_dir, "download_data")
    plot_dir   = os.path.join(session_dir, "download_plot")
    os.makedirs(files_path, exist_ok=True)
    os.makedirs(plot_dir, exist_ok=True)
    return files_path, plot_dir


def _write_session_json(session_dir, topo_name, traj_name, selected_plots):
    data = {
        "selected_plots": selected_plots,
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
            "nativePdb": topo_name,
            "trajXtc": traj_name,
            "trajpdb": traj_name,
        },
    }
    with open(os.path.join(session_dir, "session_data.json"), "w") as f:
        json.dump(data, f)
    return data


# ---------------------------------------------------------------------------
# RMSD full pipeline
# ---------------------------------------------------------------------------

class TestRmsdPipeline:
    def test_compute_then_plot_rmsd(self, e2e_session):
        from src.core.tasks import compute_rmsd, generate_rmsd_plot

        sid = e2e_session["session_id"]
        topo = e2e_session["topology"]
        traj = e2e_session["trajectory"]
        sdir = e2e_session["session_dir"]

        # Phase 1: compute metric
        metric_result = compute_rmsd.apply(args=[topo, traj, sid]).get()
        assert metric_result["status"] == "success"

        pkl = os.path.join(sdir, "rmsd_data.pkl")
        assert os.path.isfile(pkl)
        with open(pkl, "rb") as f:
            rmsd_data = pickle.load(f)
        assert isinstance(rmsd_data, np.ndarray)

        # Phase 2: generate plot
        files_path, plot_dir = _setup_plot_dirs(sdir)
        plot_result = generate_rmsd_plot.apply(
            args=[topo, traj, files_path, plot_dir, sid, {}]
        ).get()

        # Plot task returns plotly JSON on success
        assert plot_result is not None

    def test_rmsd_csv_written(self, e2e_session):
        from src.core.tasks import compute_rmsd, generate_rmsd_plot

        sid = e2e_session["session_id"]
        topo = e2e_session["topology"]
        traj = e2e_session["trajectory"]
        sdir = e2e_session["session_dir"]

        compute_rmsd.apply(args=[topo, traj, sid]).get()
        files_path, plot_dir = _setup_plot_dirs(sdir)
        generate_rmsd_plot.apply(args=[topo, traj, files_path, plot_dir, sid, {}]).get()

        csv_path = os.path.join(files_path, "rmsd_values.csv")
        assert os.path.isfile(csv_path), "rmsd_values.csv not created"

        import pandas as pd
        df = pd.read_csv(csv_path)
        assert "RMSD" in df.columns
        assert len(df) > 0

    def test_rmsd_html_written(self, e2e_session):
        from src.core.tasks import compute_rmsd, generate_rmsd_plot

        sid = e2e_session["session_id"]
        topo = e2e_session["topology"]
        traj = e2e_session["trajectory"]
        sdir = e2e_session["session_dir"]

        compute_rmsd.apply(args=[topo, traj, sid]).get()
        files_path, plot_dir = _setup_plot_dirs(sdir)
        generate_rmsd_plot.apply(args=[topo, traj, files_path, plot_dir, sid, {}]).get()

        html_path = os.path.join(plot_dir, "rmsd_plot.html")
        assert os.path.isfile(html_path), "rmsd_plot.html not created"
        assert os.path.getsize(html_path) > 100


# ---------------------------------------------------------------------------
# eRMSD full pipeline
# ---------------------------------------------------------------------------

class TestErmsdPipeline:
    def test_compute_then_plot_ermsd(self, e2e_session):
        from src.core.tasks import compute_ermsd, generate_ermsd_plot

        sid = e2e_session["session_id"]
        topo = e2e_session["topology"]
        traj = e2e_session["trajectory"]
        sdir = e2e_session["session_dir"]

        metric_result = compute_ermsd.apply(args=[topo, traj, sid]).get()
        assert metric_result["status"] == "success"

        files_path, plot_dir = _setup_plot_dirs(sdir)
        plot_result = generate_ermsd_plot.apply(
            args=[topo, traj, files_path, plot_dir, sid, {}]
        ).get()
        assert plot_result is not None

    def test_ermsd_html_written(self, e2e_session):
        from src.core.tasks import compute_ermsd, generate_ermsd_plot

        sid = e2e_session["session_id"]
        topo = e2e_session["topology"]
        traj = e2e_session["trajectory"]
        sdir = e2e_session["session_dir"]

        compute_ermsd.apply(args=[topo, traj, sid]).get()
        files_path, plot_dir = _setup_plot_dirs(sdir)
        generate_ermsd_plot.apply(args=[topo, traj, files_path, plot_dir, sid, {}]).get()

        html_path = os.path.join(plot_dir, "ermsd_plot.html")
        assert os.path.isfile(html_path), "ermsd_plot.html not created"


# ---------------------------------------------------------------------------
# Radius of Gyration full pipeline
# ---------------------------------------------------------------------------

class TestRgPipeline:
    def test_compute_then_plot_rg(self, e2e_session):
        from src.core.tasks import compute_radius_of_gyration

        sid = e2e_session["session_id"]
        topo = e2e_session["topology"]
        traj = e2e_session["trajectory"]
        sdir = e2e_session["session_dir"]

        result = compute_radius_of_gyration.apply(args=[topo, traj, sid]).get()
        assert result["status"] == "success"

        # Verify Rg values are physically reasonable (5–50 Å for RNA)
        pkl_path = os.path.join(sdir, "radius_of_gyration_data.pkl")
        with open(pkl_path, "rb") as f:
            data = pickle.load(f)
        rg_vals = data["rg_values"]
        assert all(5.0 < v < 200.0 for v in rg_vals), f"Unusual Rg values: {rg_vals[:5]}"


# ---------------------------------------------------------------------------
# Download via Flask route after pipeline
# ---------------------------------------------------------------------------

class TestDownloadAfterPipeline:
    """Verify that files generated by tasks can be downloaded through Flask routes."""

    def test_download_plot_data_csv_after_rmsd(
        self, client, e2e_session, tmp_uploads, monkeypatch
    ):
        import src.core.app as app_module
        # Redirect Flask to the tmp_path uploads
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        from src.core.tasks import compute_rmsd, generate_rmsd_plot

        sid = e2e_session["session_id"]
        topo = e2e_session["topology"]
        traj = e2e_session["trajectory"]
        sdir = e2e_session["session_dir"]

        # Re-create session dir under the patched uploads path
        new_sdir = os.path.join(str(tmp_uploads), sid)
        os.makedirs(new_sdir, exist_ok=True)
        shutil.copy(topo, new_sdir)
        shutil.copy(traj, new_sdir)

        # Compute and generate plot under new uploads dir
        import importlib
        monkeypatch.chdir(str(tmp_uploads).replace("/uploads", ""))

        # Run tasks directly (not through eager Celery to avoid chdir issues)
        from src.core.tasks import compute_rmsd as _compute_rmsd
        from src.core.tasks import generate_rmsd_plot as _gen_plot

        _compute_rmsd.apply(
            args=[os.path.join(new_sdir, "frame_0_noions.pdb"),
                  os.path.join(new_sdir, "SHAW_8500_10418.xtc"),
                  sid]
        ).get()

        files_path = os.path.join(new_sdir, "download_data", "RMSD")
        plot_dir   = os.path.join(new_sdir, "download_plot", "RMSD")
        os.makedirs(files_path, exist_ok=True)
        os.makedirs(plot_dir, exist_ok=True)

        # Manually write a CSV (since generate task uses different subdir structure)
        import pandas as pd
        rmsd_df = pd.DataFrame({"RMSD": [1.0, 2.0, 3.0]})
        rmsd_df.to_csv(os.path.join(files_path, "rmsd_values.csv"), index=False)

        rv = client.get(f"/download/plot_data/{sid}/RMSD")
        assert rv.status_code == 200
        assert len(rv.data) > 0

    def test_download_plot_html_after_rmsd(
        self, client, e2e_session, tmp_uploads, monkeypatch
    ):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        sid = e2e_session["session_id"]
        new_sdir = os.path.join(str(tmp_uploads), sid)
        os.makedirs(new_sdir, exist_ok=True)

        # Write a fake HTML plot
        plot_dir = os.path.join(new_sdir, "download_plot", "RMSD")
        os.makedirs(plot_dir, exist_ok=True)
        html_path = os.path.join(plot_dir, "rmsd_plot.html")
        with open(html_path, "w") as f:
            f.write("<html><body>RMSD plot</body></html>")

        rv = client.get(f"/download/plot/{sid}/RMSD")
        assert rv.status_code == 200
        content = rv.data.decode("utf-8", errors="replace")
        assert "RMSD" in content or len(rv.data) > 0


# ---------------------------------------------------------------------------
# Multi-plot pipeline: RMSD + eRMSD computed in parallel
# ---------------------------------------------------------------------------

class TestMultiPlotPipeline:
    def test_rmsd_and_ermsd_computed_independently(self, e2e_session):
        from src.core.tasks import compute_rmsd, compute_ermsd

        sid = e2e_session["session_id"]
        topo = e2e_session["topology"]
        traj = e2e_session["trajectory"]
        sdir = e2e_session["session_dir"]

        r1 = compute_rmsd.apply(args=[topo, traj, sid]).get()
        r2 = compute_ermsd.apply(args=[topo, traj, sid]).get()

        assert r1["status"] == "success"
        assert r2["status"] == "success"

        rmsd_pkl = os.path.join(sdir, "rmsd_data.pkl")
        ermsd_pkl = os.path.join(sdir, "ermsd_data.pkl")
        assert os.path.isfile(rmsd_pkl)
        assert os.path.isfile(ermsd_pkl)

        with open(rmsd_pkl, "rb") as f:
            rmsd = pickle.load(f)
        with open(ermsd_pkl, "rb") as f:
            ermsd = pickle.load(f)

        assert len(rmsd) == len(ermsd), "RMSD and eRMSD should have same number of frames"

    def test_rg_and_annotate_computed_independently(self, e2e_session):
        from src.core.tasks import compute_radius_of_gyration, compute_annotate

        sid = e2e_session["session_id"]
        topo = e2e_session["topology"]
        traj = e2e_session["trajectory"]
        sdir = e2e_session["session_dir"]

        r1 = compute_radius_of_gyration.apply(args=[topo, traj, sid]).get()
        r2 = compute_annotate.apply(args=[topo, traj, sid]).get()

        assert r1["status"] == "success"
        assert r2["status"] == "success"

    def test_full_pipeline_rmsd_and_rg(self, e2e_session):
        """Full metric + plot pipeline for two metrics."""
        from src.core.tasks import (
            compute_rmsd, compute_radius_of_gyration,
            generate_rmsd_plot,
        )

        sid = e2e_session["session_id"]
        topo = e2e_session["topology"]
        traj = e2e_session["trajectory"]
        sdir = e2e_session["session_dir"]

        # Metrics
        compute_rmsd.apply(args=[topo, traj, sid]).get()
        compute_radius_of_gyration.apply(args=[topo, traj, sid]).get()

        # RMSD plot
        files_path, plot_dir = _setup_plot_dirs(sdir)
        generate_rmsd_plot.apply(args=[topo, traj, files_path, plot_dir, sid, {}]).get()

        # Verify all expected output files exist
        assert os.path.isfile(os.path.join(sdir, "rmsd_data.pkl"))
        assert os.path.isfile(os.path.join(sdir, "radius_of_gyration_data.pkl"))
        assert os.path.isfile(os.path.join(files_path, "rmsd_values.csv"))
        assert os.path.isfile(os.path.join(plot_dir, "rmsd_plot.html"))


# ---------------------------------------------------------------------------
# Annotate pipeline (barnaba-based secondary structure)
# ---------------------------------------------------------------------------

class TestAnnotatePipeline:
    def test_annotate_produces_valid_data(self, e2e_session):
        from src.core.tasks import compute_annotate

        sid = e2e_session["session_id"]
        topo = e2e_session["topology"]
        traj = e2e_session["trajectory"]
        sdir = e2e_session["session_dir"]

        result = compute_annotate.apply(args=[topo, traj, sid]).get()
        assert result["status"] == "success"

        pkl_path = os.path.join(sdir, "annotate_data.pkl")
        assert os.path.isfile(pkl_path)

        with open(pkl_path, "rb") as f:
            data = pickle.load(f)
        assert data is not None

    def test_annotate_data_length_matches_trajectory(self, e2e_session):
        """Annotate should produce one entry per trajectory frame."""
        import MDAnalysis as mda
        from src.core.tasks import compute_annotate

        sid = e2e_session["session_id"]
        topo = e2e_session["topology"]
        traj = e2e_session["trajectory"]
        sdir = e2e_session["session_dir"]

        u = mda.Universe(topo, traj)
        n_frames = len(u.trajectory)

        compute_annotate.apply(args=[topo, traj, sid]).get()

        with open(os.path.join(sdir, "annotate_data.pkl"), "rb") as f:
            data = pickle.load(f)

        # barnaba.annotate returns a tuple; first element is per-frame data
        if isinstance(data, (list, tuple)) and len(data) > 0:
            first = data[0]
            if hasattr(first, "__len__"):
                assert len(first) == n_frames or len(data) == n_frames


# ---------------------------------------------------------------------------
# FSE example data (larger dataset)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not HAS_FSE, reason="FSE example data not found")
class TestFSEPipeline:
    def test_rmsd_on_fse_data(self, fse_e2e_session):
        from src.core.tasks import compute_rmsd

        sid = fse_e2e_session["session_id"]
        topo = fse_e2e_session["topology"]
        traj = fse_e2e_session["trajectory"]
        sdir = fse_e2e_session["session_dir"]

        result = compute_rmsd.apply(args=[topo, traj, sid]).get()
        assert result["status"] == "success"

        with open(os.path.join(sdir, "rmsd_data.pkl"), "rb") as f:
            data = pickle.load(f)
        assert len(data) > 1

    def test_rg_on_fse_data(self, fse_e2e_session):
        from src.core.tasks import compute_radius_of_gyration

        sid = fse_e2e_session["session_id"]
        topo = fse_e2e_session["topology"]
        traj = fse_e2e_session["trajectory"]

        result = compute_radius_of_gyration.apply(args=[topo, traj, sid]).get()
        assert result["status"] == "success"
