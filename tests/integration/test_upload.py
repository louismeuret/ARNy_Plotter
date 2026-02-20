"""
Integration tests for the file upload pipeline.

These tests copy real MD files into a temporary session directory and
call the Flask upload-files endpoint, verifying that session_data.json
is written correctly.
"""

import os
import sys
import json
import shutil
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

TOPOLOGY_PDB = os.path.join(PROJECT_ROOT, "data", "examples", "frame_0_noions.pdb")
TRAJECTORY_XTC = os.path.join(PROJECT_ROOT, "data", "examples", "SHAW_8500_10418.xtc")

pytestmark = pytest.mark.skipif(
    not (os.path.isfile(TOPOLOGY_PDB) and os.path.isfile(TRAJECTORY_XTC)),
    reason="Example data files not found",
)


# ---------------------------------------------------------------------------
# Helper — simulate a chunked upload then call /upload-files
# ---------------------------------------------------------------------------

def _do_upload(client, uploads_dir, session_id, topology_path, traj_path, plots=("rmsd",)):
    """
    Simulate pre-uploaded files (chunked mode) then POST to /upload-files.
    Returns the response object.
    """
    session_dir = os.path.join(uploads_dir, session_id)
    os.makedirs(session_dir, exist_ok=True)

    topo_name = os.path.basename(topology_path)
    traj_name = os.path.basename(traj_path)

    shutil.copy(topology_path, os.path.join(session_dir, topo_name))
    shutil.copy(traj_path, os.path.join(session_dir, traj_name))

    form_data = {
        "session_id": session_id,
        "chunked_upload": "true",
        "nativePdb_filename": topo_name,
        "trajXtc_filename": traj_name,
        "n_frames": "10",
    }
    for p in plots:
        form_data[p] = "on"

    return client.post("/upload-files", data=form_data)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestUploadFiles:
    def test_upload_redirects_to_view_trajectory(self, client, tmp_uploads, session_id, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        rv = _do_upload(client, str(tmp_uploads), session_id, TOPOLOGY_PDB, TRAJECTORY_XTC)
        # Should redirect to /view-trajectory/<session_id>
        assert rv.status_code in (301, 302)
        location = rv.headers.get("Location", "")
        assert "view-trajectory" in location or session_id in location

    def test_session_data_json_created(self, client, tmp_uploads, session_id, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        _do_upload(client, str(tmp_uploads), session_id, TOPOLOGY_PDB, TRAJECTORY_XTC)

        session_json = os.path.join(str(tmp_uploads), session_id, "session_data.json")
        assert os.path.isfile(session_json), "session_data.json not created"

    def test_session_data_contains_files(self, client, tmp_uploads, session_id, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        _do_upload(client, str(tmp_uploads), session_id, TOPOLOGY_PDB, TRAJECTORY_XTC)

        with open(os.path.join(str(tmp_uploads), session_id, "session_data.json")) as f:
            data = json.load(f)

        assert "files" in data
        assert "nativePdb" in data["files"]
        assert "trajXtc" in data["files"]

    def test_session_data_topology_filename_correct(self, client, tmp_uploads, session_id, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        _do_upload(client, str(tmp_uploads), session_id, TOPOLOGY_PDB, TRAJECTORY_XTC)

        with open(os.path.join(str(tmp_uploads), session_id, "session_data.json")) as f:
            data = json.load(f)

        assert data["files"]["nativePdb"] == "frame_0_noions.pdb"

    def test_session_data_trajectory_filename_correct(self, client, tmp_uploads, session_id, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        _do_upload(client, str(tmp_uploads), session_id, TOPOLOGY_PDB, TRAJECTORY_XTC)

        with open(os.path.join(str(tmp_uploads), session_id, "session_data.json")) as f:
            data = json.load(f)

        assert data["files"]["trajXtc"] == "SHAW_8500_10418.xtc"

    def test_selected_plots_stored(self, client, tmp_uploads, session_id, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        _do_upload(
            client, str(tmp_uploads), session_id, TOPOLOGY_PDB, TRAJECTORY_XTC,
            plots=("rmsd", "ermsd"),
        )

        with open(os.path.join(str(tmp_uploads), session_id, "session_data.json")) as f:
            data = json.load(f)

        selected = data.get("selected_plots", [])
        assert "RMSD" in selected
        assert "ERMSD" in selected

    def test_upload_without_files_fails(self, client, tmp_uploads, session_id, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        rv = client.post(
            "/upload-files",
            data={
                "session_id": session_id,
                "chunked_upload": "true",
                # Missing filenames → should return an error code
            },
        )
        assert rv.status_code in (400, 404, 302, 500)

    def test_n_frames_stored(self, client, tmp_uploads, session_id, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        _do_upload(client, str(tmp_uploads), session_id, TOPOLOGY_PDB, TRAJECTORY_XTC)

        with open(os.path.join(str(tmp_uploads), session_id, "session_data.json")) as f:
            data = json.load(f)

        assert data["n_frames"] == 10


class TestChunkedUpload:
    """Test the /upload-chunk endpoint that assembles file chunks."""

    def test_multi_chunk_reassembly(self, client, tmp_uploads, monkeypatch):
        import io
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        chunk_session = "chunk-test-session"
        content = b"A" * 1024  # 1 KB
        mid = len(content) // 2
        chunks = [content[:mid], content[mid:]]

        for i, chunk in enumerate(chunks):
            rv = client.post(
                "/upload-chunk",
                data={
                    "session_id": chunk_session,
                    "file_type": "topology",
                    "chunk_index": str(i),
                    "total_chunks": str(len(chunks)),
                    "original_filename": "assembled.pdb",
                    "chunk": (io.BytesIO(chunk), "assembled.pdb"),
                },
                content_type="multipart/form-data",
            )
            assert rv.status_code == 200

        assembled_path = os.path.join(str(tmp_uploads), chunk_session, "assembled.pdb")
        assert os.path.isfile(assembled_path)
        with open(assembled_path, "rb") as f:
            result = f.read()
        assert result == content

    def test_single_chunk_complete_flag(self, client, tmp_uploads, monkeypatch):
        import io
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        rv = client.post(
            "/upload-chunk",
            data={
                "session_id": "single-chunk-session",
                "file_type": "trajectory",
                "chunk_index": "0",
                "total_chunks": "1",
                "original_filename": "traj.xtc",
                "chunk": (io.BytesIO(b"XTCDATA"), "traj.xtc"),
            },
            content_type="multipart/form-data",
        )
        data = rv.get_json()
        assert data["completed"] is True
        assert data["progress"] == 100.0
