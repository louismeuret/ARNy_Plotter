"""
Unit tests for utility functions in src/core/app.py
"""

import os
import sys
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# ---------------------------------------------------------------------------
# convert_dimension_to_numeric
# ---------------------------------------------------------------------------

class TestConvertDimensionToNumeric:
    """Tests for the dimension-name → integer mapping used by landscape plots."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src.core.app import convert_dimension_to_numeric
        self.convert = convert_dimension_to_numeric

    def test_rmsd_returns_1(self):
        assert self.convert("RMSD") == 1

    def test_ermsd_returns_2(self):
        assert self.convert("eRMSD") == 2

    def test_q_returns_3(self):
        assert self.convert("Q") == 3

    def test_fraction_returns_4(self):
        assert self.convert("Fraction of Contact Formed") == 4

    def test_unknown_defaults_to_1(self):
        assert self.convert("SomeUnknownMetric") == 1

    def test_empty_string_defaults_to_1(self):
        assert self.convert("") == 1

    def test_case_sensitive(self):
        # The mapping is case-sensitive; lowercase should fall back to default
        assert self.convert("rmsd") == 1


# ---------------------------------------------------------------------------
# create_session_id
# ---------------------------------------------------------------------------

class TestCreateSessionId:
    """Session IDs must be non-empty hex strings."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src.core.app import create_session_id
        self.create = create_session_id

    def test_returns_string(self):
        assert isinstance(self.create(), str)

    def test_non_empty(self):
        assert len(self.create()) > 0

    def test_is_hex(self):
        sid = self.create()
        int(sid, 16)  # raises ValueError if not valid hex

    def test_unique(self):
        ids = {self.create() for _ in range(50)}
        assert len(ids) == 50, "Session IDs should be unique"


# ---------------------------------------------------------------------------
# parse_topology_file
# ---------------------------------------------------------------------------

TOPOLOGY_PDB = os.path.join(PROJECT_ROOT, "data", "examples", "frame_0_noions.pdb")


@pytest.mark.skipif(
    not os.path.isfile(TOPOLOGY_PDB),
    reason="Example topology PDB not found",
)
class TestParseTopologyFile:
    """Tests for the topology parser that extracts residue information."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src.core.app import parse_topology_file
        self.parse = parse_topology_file

    def test_returns_dict(self):
        result = self.parse(TOPOLOGY_PDB)
        assert isinstance(result, dict)

    def test_has_required_keys(self):
        result = self.parse(TOPOLOGY_PDB)
        for key in ("residues", "torsion_angles", "num_residues", "sequence"):
            assert key in result, f"Missing key: {key}"

    def test_residues_is_list(self):
        result = self.parse(TOPOLOGY_PDB)
        assert isinstance(result["residues"], list)

    def test_num_residues_matches_list(self):
        result = self.parse(TOPOLOGY_PDB)
        assert result["num_residues"] == len(result["residues"])

    def test_residue_entry_has_required_fields(self):
        result = self.parse(TOPOLOGY_PDB)
        required_fields = {"index", "name", "id", "full_name"}
        for res in result["residues"]:
            missing = required_fields - set(res.keys())
            assert not missing, f"Residue entry missing fields: {missing}"

    def test_sequence_is_list_of_strings(self):
        result = self.parse(TOPOLOGY_PDB)
        seq = result["sequence"]
        assert isinstance(seq, list)
        assert all(isinstance(s, str) for s in seq)

    def test_torsion_angles_are_rna_standard(self):
        result = self.parse(TOPOLOGY_PDB)
        angle_names = {a["name"] for a in result["torsion_angles"]}
        expected = {"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "chi"}
        assert angle_names == expected

    def test_non_existent_file_returns_none(self):
        result = self.parse("/nonexistent/path/file.pdb")
        assert result is None


# ---------------------------------------------------------------------------
# convert_topology_to_pdb  (only runs when example files are present)
# ---------------------------------------------------------------------------

TRAJECTORY_XTC = os.path.join(PROJECT_ROOT, "data", "examples", "SHAW_8500_10418.xtc")


@pytest.mark.skipif(
    not (os.path.isfile(TOPOLOGY_PDB) and os.path.isfile(TRAJECTORY_XTC)),
    reason="Example files not found",
)
class TestConvertTopologyToPdb:
    """Supported formats should not be converted; unsupported should be converted."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src.core.app import convert_topology_to_pdb
        self.convert = convert_topology_to_pdb

    def test_pdb_not_converted(self):
        result = self.convert(TOPOLOGY_PDB, TRAJECTORY_XTC, "test-session")
        assert result == TOPOLOGY_PDB

    def test_returns_string(self):
        result = self.convert(TOPOLOGY_PDB, TRAJECTORY_XTC, "test-session")
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# convert_trajectory_to_xtc
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not (os.path.isfile(TOPOLOGY_PDB) and os.path.isfile(TRAJECTORY_XTC)),
    reason="Example files not found",
)
class TestConvertTrajectoryToXtc:
    """XTC trajectories should pass through unchanged."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src.core.app import convert_trajectory_to_xtc
        self.convert = convert_trajectory_to_xtc

    def test_xtc_not_converted(self):
        result = self.convert(TOPOLOGY_PDB, TRAJECTORY_XTC, "test-session")
        assert result == TRAJECTORY_XTC

    def test_returns_string(self):
        result = self.convert(TOPOLOGY_PDB, TRAJECTORY_XTC, "test-session")
        assert isinstance(result, str)
