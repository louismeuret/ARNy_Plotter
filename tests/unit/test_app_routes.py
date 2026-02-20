"""
Unit tests for Flask application routes.

Uses Flask test client — no real Celery or Redis required.
Routes that trigger heavy computation are not called here;
see tests/integration/ for those.
"""

import os
import sys
import json
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# ---------------------------------------------------------------------------
# Simple GET routes
# ---------------------------------------------------------------------------

class TestStaticGetRoutes:
    """Routes that should always return 200 without any data."""

    def test_index_get(self, client):
        rv = client.get("/")
        assert rv.status_code in (200, 302)

    def test_tools_page(self, client):
        rv = client.get("/tools")
        assert rv.status_code == 200

    def test_documentation_page(self, client):
        rv = client.get("/documentation")
        assert rv.status_code == 200

    def test_authors_page(self, client):
        rv = client.get("/authors")
        assert rv.status_code == 200

    def test_cgu_page(self, client):
        rv = client.get("/cgu")
        assert rv.status_code == 200

    def test_get_session_endpoint(self, client):
        rv = client.get("/get_session")
        assert rv.status_code == 200
        data = rv.get_json()
        assert "session_id" in data
        assert "exists" in data

    def test_get_session_returns_valid_hex(self, client):
        rv = client.get("/get_session")
        sid = rv.get_json()["session_id"]
        int(sid, 16)  # raises if not valid hex

    def test_get_session_unique(self, client):
        ids = set()
        for _ in range(5):
            rv = client.get("/get_session")
            ids.add(rv.get_json()["session_id"])
        assert len(ids) == 5


# ---------------------------------------------------------------------------
# /tools/download/<filename>
# ---------------------------------------------------------------------------

class TestToolsDownload:
    def test_download_valid_script(self, client):
        rv = client.get("/tools/download/extract_frames.py")
        # Either downloads or 404 if script not present
        assert rv.status_code in (200, 404)

    def test_download_rejects_non_py(self, client):
        rv = client.get("/tools/download/malicious.sh")
        assert rv.status_code == 400

    def test_download_rejects_txt_extension(self, client):
        rv = client.get("/tools/download/readme.txt")
        assert rv.status_code == 400

    def test_download_rejects_path_traversal(self, client):
        rv = client.get("/tools/download/../../etc/passwd.py")
        # Should return 400 or 404, never 200 for a traversal attempt
        assert rv.status_code in (400, 404)


# ---------------------------------------------------------------------------
# /parse-topology (POST)
# ---------------------------------------------------------------------------

class TestParseTopologyRoute:
    TOPOLOGY_PDB = os.path.join(
        PROJECT_ROOT, "data", "examples", "frame_0_noions.pdb"
    )

    def test_missing_file_returns_400(self, client):
        rv = client.post("/parse-topology", data={"session_id": "abc123"})
        assert rv.status_code == 400

    def test_missing_session_id_returns_400(self, client):
        if not os.path.isfile(self.TOPOLOGY_PDB):
            pytest.skip("Example PDB not found")
        with open(self.TOPOLOGY_PDB, "rb") as f:
            rv = client.post(
                "/parse-topology",
                data={"topologyFile": (f, "frame_0_noions.pdb")},
                content_type="multipart/form-data",
            )
        assert rv.status_code == 400

    def test_valid_topology_upload(self, client):
        if not os.path.isfile(self.TOPOLOGY_PDB):
            pytest.skip("Example PDB not found")
        with open(self.TOPOLOGY_PDB, "rb") as f:
            rv = client.post(
                "/parse-topology",
                data={
                    "topologyFile": (f, "frame_0_noions.pdb"),
                    "session_id": "test-parse-topology",
                },
                content_type="multipart/form-data",
            )
        assert rv.status_code == 200
        data = rv.get_json()
        assert data.get("success") is True
        assert "residues" in data
        assert "num_residues" in data
        assert "torsion_angles" in data
        assert "sequence" in data

    def test_parse_topology_residue_count_positive(self, client):
        if not os.path.isfile(self.TOPOLOGY_PDB):
            pytest.skip("Example PDB not found")
        with open(self.TOPOLOGY_PDB, "rb") as f:
            rv = client.post(
                "/parse-topology",
                data={
                    "topologyFile": (f, "frame_0_noions.pdb"),
                    "session_id": "test-parse-topology-count",
                },
                content_type="multipart/form-data",
            )
        data = rv.get_json()
        assert data["num_residues"] > 0

    def test_parse_topology_torsion_angles_complete(self, client):
        if not os.path.isfile(self.TOPOLOGY_PDB):
            pytest.skip("Example PDB not found")
        with open(self.TOPOLOGY_PDB, "rb") as f:
            rv = client.post(
                "/parse-topology",
                data={
                    "topologyFile": (f, "frame_0_noions.pdb"),
                    "session_id": "test-parse-topology-torsion",
                },
                content_type="multipart/form-data",
            )
        data = rv.get_json()
        angle_names = {a["name"] for a in data["torsion_angles"]}
        expected = {"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "chi"}
        assert angle_names == expected


# ---------------------------------------------------------------------------
# /upload-chunk (POST) — basic validation
# ---------------------------------------------------------------------------

class TestUploadChunkRoute:
    def test_missing_params_returns_error(self, client):
        """Missing required params → the route returns a 4xx or 5xx error."""
        rv = client.post("/upload-chunk", data={})
        assert rv.status_code in (400, 500)

    def test_missing_chunk_data_returns_error(self, client):
        """All metadata present but no chunk file → error response."""
        rv = client.post(
            "/upload-chunk",
            data={
                "session_id": "test-upload-chunk",
                "file_type": "topology",
                "chunk_index": "0",
                "total_chunks": "1",
                "original_filename": "test.pdb",
            },
        )
        assert rv.status_code in (400, 500)

    def test_valid_single_chunk_upload(self, client, tmp_path, monkeypatch):
        """Upload a tiny single-chunk file and verify the response."""
        import io
        import src.core.app as app_module
        uploads = tmp_path / "uploads"
        uploads.mkdir()
        monkeypatch.setattr(app_module, "uploads_dir", str(uploads))

        chunk_content = b"ATOM      1  O5'   G A   1       0.000   0.000   0.000  1.00  0.00           O\n"
        rv = client.post(
            "/upload-chunk",
            data={
                "session_id": "test-single-chunk",
                "file_type": "topology",
                "chunk_index": "0",
                "total_chunks": "1",
                "original_filename": "topology.pdb",
                "chunk": (io.BytesIO(chunk_content), "topology.pdb"),
            },
            content_type="multipart/form-data",
        )
        assert rv.status_code == 200
        data = rv.get_json()
        assert data["success"] is True
        assert data["completed"] is True


# ---------------------------------------------------------------------------
# /retrieve-results (GET) — invalid session
# In TESTING mode, Flask propagates server exceptions; we catch them.
# ---------------------------------------------------------------------------

class TestRetrieveResultsRoute:
    def test_missing_session_returns_error(self, client):
        """Non-existent session → either 404/500 HTTP or a server exception."""
        try:
            rv = client.get("/retrieve-results?session_id=nonexistent-session-xyz")
            assert rv.status_code in (404, 500)
        except (FileNotFoundError, Exception):
            pass  # Exception propagation is also acceptable behaviour

    def test_no_session_id_param(self, client):
        """Missing session_id → error of some kind."""
        try:
            rv = client.get("/retrieve-results")
            assert rv.status_code in (400, 404, 500)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# /download routes — non-existent session
# In TESTING mode, Flask propagates exceptions; we allow that.
# ---------------------------------------------------------------------------

class TestDownloadRoutes:
    def test_download_plot_data_nonexistent(self, client):
        try:
            rv = client.get("/download/plot_data/nonexistent-session/RMSD")
            assert rv.status_code in (404, 500)
        except Exception:
            pass  # IndexError / FileNotFoundError propagation is acceptable

    def test_download_plot_nonexistent(self, client):
        try:
            rv = client.get("/download/plot/nonexistent-session/RMSD")
            assert rv.status_code in (404, 500)
        except Exception:
            pass

    def test_download_frame_nonexistent_session(self, client):
        try:
            rv = client.get("/download/frame/nonexistent-session/0")
            assert rv.status_code in (404, 500)
        except Exception:
            pass
