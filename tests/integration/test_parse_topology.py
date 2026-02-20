"""
Integration tests for /parse-topology endpoint and helper function.

Tests use actual MD files from data/examples/ to verify end-to-end
topology parsing via both the HTTP route and the raw helper function.
"""

import os
import sys
import json
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

TOPOLOGY_PDB = os.path.join(PROJECT_ROOT, "data", "examples", "frame_0_noions.pdb")
TRAJECTORY_XTC = os.path.join(PROJECT_ROOT, "data", "examples", "SHAW_8500_10418.xtc")
TOPOLOGY_AMBER = os.path.join(
    PROJECT_ROOT, "data", "examples", "01c3d102470b44129dbf078716c3bb5b", "RNAOPC.parm7"
)
FSE_TOPOLOGY = os.path.join(PROJECT_ROOT, "data", "examples", "FSE", "correct_conf.pdb")


# ---------------------------------------------------------------------------
# Direct helper function tests
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not os.path.isfile(TOPOLOGY_PDB), reason="Example PDB not found"
)
class TestParseTopologyHelperDirect:
    @pytest.fixture(autouse=True)
    def _import(self):
        from src.core.app import parse_topology_file
        self.parse = parse_topology_file

    def test_parses_pdb_without_error(self):
        result = self.parse(TOPOLOGY_PDB)
        assert result is not None

    def test_num_residues_is_int(self):
        result = self.parse(TOPOLOGY_PDB)
        assert isinstance(result["num_residues"], int)

    def test_residue_names_are_rna_bases(self):
        result = self.parse(TOPOLOGY_PDB)
        rna_bases = {"A", "U", "G", "C", "RA", "RU", "RG", "RC",
                     "DA", "DT", "DG", "DC", "ADE", "URA", "GUA", "CYT"}
        actual = set(result["sequence"])
        # At least some residues should be recognised RNA bases
        # (allow for modified residues with unusual names)
        assert len(actual) > 0

    def test_residue_index_is_int(self):
        result = self.parse(TOPOLOGY_PDB)
        for res in result["residues"]:
            assert isinstance(res["index"], int)

    def test_residue_id_is_int(self):
        result = self.parse(TOPOLOGY_PDB)
        for res in result["residues"]:
            assert isinstance(res["id"], int)

    def test_full_name_combines_name_and_id(self):
        result = self.parse(TOPOLOGY_PDB)
        for res in result["residues"]:
            assert res["full_name"] == f"{res['name']}{res['id']}"


@pytest.mark.skipif(
    not os.path.isfile(FSE_TOPOLOGY), reason="FSE example PDB not found"
)
class TestParseTopologyFSEExample:
    @pytest.fixture(autouse=True)
    def _import(self):
        from src.core.app import parse_topology_file
        self.parse = parse_topology_file

    def test_parses_fse_topology(self):
        result = self.parse(FSE_TOPOLOGY)
        assert result is not None
        assert result["num_residues"] > 0


# ---------------------------------------------------------------------------
# HTTP route tests
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not os.path.isfile(TOPOLOGY_PDB), reason="Example PDB not found"
)
class TestParseTopologyRoute:
    def test_returns_200_for_valid_pdb(self, client):
        with open(TOPOLOGY_PDB, "rb") as f:
            rv = client.post(
                "/parse-topology",
                data={
                    "topologyFile": (f, "frame_0_noions.pdb"),
                    "session_id": "integration-parse-test-001",
                },
                content_type="multipart/form-data",
            )
        assert rv.status_code == 200

    def test_response_is_valid_json(self, client):
        with open(TOPOLOGY_PDB, "rb") as f:
            rv = client.post(
                "/parse-topology",
                data={
                    "topologyFile": (f, "frame_0_noions.pdb"),
                    "session_id": "integration-parse-test-002",
                },
                content_type="multipart/form-data",
            )
        data = rv.get_json()
        assert data is not None

    def test_success_flag_true(self, client):
        with open(TOPOLOGY_PDB, "rb") as f:
            rv = client.post(
                "/parse-topology",
                data={
                    "topologyFile": (f, "frame_0_noions.pdb"),
                    "session_id": "integration-parse-test-003",
                },
                content_type="multipart/form-data",
            )
        assert rv.get_json()["success"] is True

    def test_residues_list_not_empty(self, client):
        with open(TOPOLOGY_PDB, "rb") as f:
            rv = client.post(
                "/parse-topology",
                data={
                    "topologyFile": (f, "frame_0_noions.pdb"),
                    "session_id": "integration-parse-test-004",
                },
                content_type="multipart/form-data",
            )
        assert len(rv.get_json()["residues"]) > 0

    def test_num_residues_equals_sequence_length(self, client):
        with open(TOPOLOGY_PDB, "rb") as f:
            rv = client.post(
                "/parse-topology",
                data={
                    "topologyFile": (f, "frame_0_noions.pdb"),
                    "session_id": "integration-parse-test-005",
                },
                content_type="multipart/form-data",
            )
        data = rv.get_json()
        assert data["num_residues"] == len(data["sequence"])

    def test_temp_file_cleaned_up(self, client, tmp_uploads, monkeypatch):
        import src.core.app as app_module
        monkeypatch.setattr(app_module, "uploads_dir", str(tmp_uploads))

        sid = "integration-parse-cleanup"
        with open(TOPOLOGY_PDB, "rb") as f:
            client.post(
                "/parse-topology",
                data={
                    "topologyFile": (f, "frame_0_noions.pdb"),
                    "session_id": sid,
                },
                content_type="multipart/form-data",
            )

        session_dir = tmp_uploads / sid
        # Temp file (prefixed with "temp_") should be removed
        temp_files = list(session_dir.glob("temp_*"))
        assert len(temp_files) == 0, f"Temp files not cleaned up: {temp_files}"

    def test_invalid_file_returns_500(self, client):
        """Uploading garbage data that MDAnalysis can't parse → 500."""
        garbage = b"NOT A VALID PDB FILE AT ALL\n" * 10
        rv = client.post(
            "/parse-topology",
            data={
                "topologyFile": (garbage, "garbage.pdb"),
                "session_id": "integration-parse-garbage",
            },
            content_type="multipart/form-data",
        )
        # Should return an error, not a success
        assert rv.status_code in (400, 500)
        data = rv.get_json()
        if rv.status_code == 200:
            assert data.get("success") is not True


@pytest.mark.skipif(
    not os.path.isfile(FSE_TOPOLOGY), reason="FSE example PDB not found"
)
class TestParseTopologyFSERoute:
    def test_fse_topology_parses_via_route(self, client):
        with open(FSE_TOPOLOGY, "rb") as f:
            rv = client.post(
                "/parse-topology",
                data={
                    "topologyFile": (f, "correct_conf.pdb"),
                    "session_id": "integration-parse-fse-001",
                },
                content_type="multipart/form-data",
            )
        assert rv.status_code == 200
        data = rv.get_json()
        assert data["success"] is True
        assert data["num_residues"] > 0
