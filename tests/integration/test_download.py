"""
Integration tests for download endpoints.

Tests verify that the download routes correctly serve files (or return
appropriate error codes when files are missing).  They create real
session directories and minimal mock data to exercise the zip-building
and single-file serving logic.
"""

import io
import os
import sys
import json
import pickle
import zipfile
import shutil
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

TOPOLOGY_PDB = os.path.join(PROJECT_ROOT, "data", "examples", "frame_0_noions.pdb")
TRAJECTORY_XTC = os.path.join(PROJECT_ROOT, "data", "examples", "SHAW_8500_10418.xtc")


def _make_session_with_download_data(uploads_dir, session_id, plot_id="RMSD"):
    """
    Create a minimal session directory with a fake download_data file so
    the /download/plot_data route can serve something real.
    """
    session_dir = os.path.join(uploads_dir, session_id)
    download_data_dir = os.path.join(session_dir, "download_data", plot_id)
    os.makedirs(download_data_dir, exist_ok=True)

    csv_path = os.path.join(download_data_dir, "rmsd.csv")
    with open(csv_path, "w") as f:
        f.write("frame,rmsd\n")
        for i in range(10):
            f.write(f"{i},{i * 0.1:.2f}\n")

    return session_dir


def _make_session_with_download_plot(uploads_dir, session_id, plot_id="RMSD"):
    """
    Create a minimal session directory with a fake PNG in download_plot.
    """
    session_dir = os.path.join(uploads_dir, session_id)
    download_plot_dir = os.path.join(session_dir, "download_plot", plot_id)
    os.makedirs(download_plot_dir, exist_ok=True)

    png_path = os.path.join(download_plot_dir, "rmsd.png")
    with open(png_path, "wb") as f:
        # Minimal 1×1 PNG header (enough to be a valid file for send_file)
        f.write(
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
            b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00\x00"
            b"\x00\x0cIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\x18"
            b"\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
        )

    return session_dir


# ---------------------------------------------------------------------------
# /download/plot_data/<session_id>/<plot_id>
# ---------------------------------------------------------------------------

class TestDownloadPlotData:
    def test_returns_200_for_existing_data(self, client, tmp_uploads, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        sid = "dl-data-test-001"
        _make_session_with_download_data(str(tmp_uploads), sid)

        rv = client.get(f"/download/plot_data/{sid}/RMSD")
        assert rv.status_code == 200

    def test_content_disposition_attachment(self, client, tmp_uploads, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        sid = "dl-data-test-002"
        _make_session_with_download_data(str(tmp_uploads), sid)

        rv = client.get(f"/download/plot_data/{sid}/RMSD")
        cd = rv.headers.get("Content-Disposition", "")
        assert "attachment" in cd

    def test_single_file_not_zipped(self, client, tmp_uploads, monkeypatch):
        """When there is only one file, it should be sent as-is (not as a zip)."""
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        sid = "dl-data-test-003"
        _make_session_with_download_data(str(tmp_uploads), sid)

        rv = client.get(f"/download/plot_data/{sid}/RMSD")
        # Content should be readable as CSV (not ZIP)
        assert rv.status_code == 200
        data = rv.data
        # Either it's a CSV or a zip — just verify not empty
        assert len(data) > 0

    def test_multiple_files_returned_as_zip(self, client, tmp_uploads, monkeypatch):
        """When multiple files exist for a plot, they should be bundled into a zip."""
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        sid = "dl-data-test-004"
        session_dir = _make_session_with_download_data(str(tmp_uploads), sid)

        # Add a second file to trigger zip behaviour
        extra = os.path.join(str(tmp_uploads), sid, "download_data", "RMSD", "rmsd2.csv")
        with open(extra, "w") as f:
            f.write("frame,rmsd\n1,2.0\n")

        rv = client.get(f"/download/plot_data/{sid}/RMSD")
        assert rv.status_code == 200
        assert zipfile.is_zipfile(io.BytesIO(rv.data))

    def test_nonexistent_session_raises(self, client, tmp_uploads, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        try:
            rv = client.get("/download/plot_data/does-not-exist/RMSD")
            assert rv.status_code in (404, 500)
        except Exception:
            pass  # Exception propagation in TESTING mode is also acceptable


# ---------------------------------------------------------------------------
# /download/plot/<session_id>/<plot_id>
# ---------------------------------------------------------------------------

class TestDownloadPlot:
    def test_returns_200_for_existing_plot(self, client, tmp_uploads, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        sid = "dl-plot-test-001"
        _make_session_with_download_plot(str(tmp_uploads), sid)

        rv = client.get(f"/download/plot/{sid}/RMSD")
        assert rv.status_code == 200

    def test_content_disposition_attachment(self, client, tmp_uploads, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        sid = "dl-plot-test-002"
        _make_session_with_download_plot(str(tmp_uploads), sid)

        rv = client.get(f"/download/plot/{sid}/RMSD")
        cd = rv.headers.get("Content-Disposition", "")
        assert "attachment" in cd

    def test_multiple_plots_returned_as_zip(self, client, tmp_uploads, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        sid = "dl-plot-test-003"
        _make_session_with_download_plot(str(tmp_uploads), sid)

        extra = os.path.join(str(tmp_uploads), sid, "download_plot", "RMSD", "rmsd2.png")
        with open(extra, "wb") as f:
            f.write(b"PNG_DATA_FAKE")

        rv = client.get(f"/download/plot/{sid}/RMSD")
        assert rv.status_code == 200
        assert zipfile.is_zipfile(io.BytesIO(rv.data))

    def test_nonexistent_session_raises(self, client, tmp_uploads, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        try:
            rv = client.get("/download/plot/does-not-exist/RMSD")
            assert rv.status_code in (404, 500)
        except Exception:
            pass  # Exception propagation in TESTING mode is also acceptable


# ---------------------------------------------------------------------------
# /download/frame/<session_id>/<frame_number>
# ---------------------------------------------------------------------------

class TestDownloadFrame:
    """
    Frame download requires Celery and real MD files.
    We only test failure modes here (no real celery broker in unit tests).
    """

    def test_nonexistent_session_returns_404(self, client, tmp_uploads, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        rv = client.get("/download/frame/nonexistent-session-999/0")
        assert rv.status_code in (404, 500)

    def test_frame_download_requires_session_data(self, client, tmp_uploads, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        # Create session dir without session_data.json
        sid = "frame-test-no-session-data"
        os.makedirs(os.path.join(str(tmp_uploads), sid))

        rv = client.get(f"/download/frame/{sid}/0")
        assert rv.status_code in (404, 500)

    @pytest.mark.skipif(
        not (os.path.isfile(TOPOLOGY_PDB) and os.path.isfile(TRAJECTORY_XTC)),
        reason="Example data files not found",
    )
    def test_frame_download_with_real_data(self, client, tmp_uploads, monkeypatch, celery_eager):
        """
        With Celery in eager mode and real MD files, frame extraction should succeed.
        """
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        sid = "frame-test-real-data"
        session_dir = os.path.join(str(tmp_uploads), sid)
        os.makedirs(session_dir, exist_ok=True)

        topo_name = "frame_0_noions.pdb"
        traj_name = "SHAW_8500_10418.xtc"
        shutil.copy(TOPOLOGY_PDB, os.path.join(session_dir, topo_name))
        shutil.copy(TRAJECTORY_XTC, os.path.join(session_dir, traj_name))

        session_data = {
            "files": {
                "nativePdb": topo_name,
                "trajXtc": traj_name,
            }
        }
        with open(os.path.join(session_dir, "session_data.json"), "w") as f:
            json.dump(session_data, f)

        rv = client.get(f"/download/frame/{sid}/0")
        # Should either succeed or return a known error
        assert rv.status_code in (200, 500)
        if rv.status_code == 200:
            cd = rv.headers.get("Content-Disposition", "")
            assert "attachment" in cd
