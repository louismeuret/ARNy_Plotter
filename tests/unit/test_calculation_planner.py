"""
Unit tests for CalculationPlanner (src/core/app.py).

Tests cover dependency mapping, shared computation detection,
and execution-tree construction.
"""

import os
import sys
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core.app import CalculationPlanner


@pytest.fixture
def planner():
    return CalculationPlanner(session_id="test-session-123")


# ---------------------------------------------------------------------------
# get_plot_dependencies
# ---------------------------------------------------------------------------

class TestGetPlotDependencies:
    def test_returns_dict(self, planner):
        deps = planner.get_plot_dependencies()
        assert isinstance(deps, dict)

    def test_landscape_needs_rmsd_and_ermsd(self, planner):
        deps = planner.get_plot_dependencies()
        assert "LANDSCAPE" in deps
        assert "RMSD" in deps["LANDSCAPE"]
        assert "ERMSD" in deps["LANDSCAPE"]

    def test_radius_of_gyration_in_dependencies(self, planner):
        deps = planner.get_plot_dependencies()
        assert "RADIUS_OF_GYRATION" in deps

    def test_end_to_end_distance_in_dependencies(self, planner):
        deps = planner.get_plot_dependencies()
        assert "END_TO_END_DISTANCE" in deps

    def test_pca_in_dependencies(self, planner):
        deps = planner.get_plot_dependencies()
        assert "PCA" in deps

    def test_umap_in_dependencies(self, planner):
        deps = planner.get_plot_dependencies()
        assert "UMAP" in deps

    def test_tsne_in_dependencies(self, planner):
        deps = planner.get_plot_dependencies()
        assert "TSNE" in deps


# ---------------------------------------------------------------------------
# get_shared_computations
# ---------------------------------------------------------------------------

class TestGetSharedComputations:
    def test_returns_dict(self, planner):
        result = planner.get_shared_computations(["RMSD", "ERMSD"])
        assert isinstance(result, dict)

    def test_annotate_shared_by_multiple_plots(self, planner):
        """SEC_STRUCTURE, DOTBRACKET, ARC, CONTACT_MAPS, ANNOTATE all use annotate."""
        annotate_plots = ["SEC_STRUCTURE", "DOTBRACKET", "ARC", "CONTACT_MAPS", "ANNOTATE"]
        result = planner.get_shared_computations(annotate_plots)
        assert "annotate" in result
        assert len(result["annotate"]) > 1

    def test_single_plot_no_shared(self, planner):
        """A single plot should not appear in shared computations."""
        result = planner.get_shared_computations(["RMSD"])
        assert "rmsd" not in result

    def test_two_rmsd_consumers_not_sharing_different_types(self, planner):
        """RMSD and ERMSD use different compute types → not shared under one key."""
        result = planner.get_shared_computations(["RMSD", "ERMSD"])
        # Each uses its own key; neither should appear merged
        assert result.get("rmsd") is None or len(result.get("rmsd", [])) < 2
        assert result.get("ermsd") is None or len(result.get("ermsd", [])) < 2

    def test_empty_list_returns_empty(self, planner):
        result = planner.get_shared_computations([])
        assert result == {}

    def test_unknown_plot_ignored(self, planner):
        result = planner.get_shared_computations(["UNKNOWN_PLOT"])
        assert result == {}


# ---------------------------------------------------------------------------
# build_execution_tree
# ---------------------------------------------------------------------------

class TestBuildExecutionTree:
    def test_returns_dict_with_three_phases(self, planner):
        tree = planner.build_execution_tree(["RMSD", "ERMSD"])
        for key in ("phase_1_metrics", "phase_2_independent", "phase_3_dependent"):
            assert key in tree

    def test_landscape_goes_to_phase_3(self, planner):
        tree = planner.build_execution_tree(["RMSD", "ERMSD", "LANDSCAPE"])
        assert "LANDSCAPE" in tree["phase_3_dependent"]

    def test_pca_goes_to_phase_3(self, planner):
        tree = planner.build_execution_tree(["PCA"])
        assert "PCA" in tree["phase_3_dependent"]

    def test_rmsd_independent_when_standalone(self, planner):
        """RMSD has no dependents → should be in phase 2 (independent) or phase 1 (metric)."""
        tree = planner.build_execution_tree(["RMSD"])
        # RMSD should not appear in phase_3_dependent
        assert "RMSD" not in tree["phase_3_dependent"]

    def test_all_phases_are_lists(self, planner):
        tree = planner.build_execution_tree(["RMSD", "LANDSCAPE"])
        assert isinstance(tree["phase_1_metrics"], list)
        assert isinstance(tree["phase_2_independent"], list)
        assert isinstance(tree["phase_3_dependent"], list)

    def test_empty_selection(self, planner):
        tree = planner.build_execution_tree([])
        assert tree["phase_1_metrics"] == []
        assert tree["phase_2_independent"] == []
        assert tree["phase_3_dependent"] == []

    def test_radius_of_gyration_dependent(self, planner):
        tree = planner.build_execution_tree(["RADIUS_OF_GYRATION"])
        assert "RADIUS_OF_GYRATION" in tree["phase_3_dependent"]

    def test_no_duplicate_plots_across_phases(self, planner):
        plots = ["RMSD", "ERMSD", "LANDSCAPE", "TORSION", "PCA", "UMAP"]
        tree = planner.build_execution_tree(plots)
        all_assigned = (
            tree["phase_1_metrics"]
            + tree["phase_2_independent"]
            + tree["phase_3_dependent"]
        )
        assert len(all_assigned) == len(set(all_assigned)), "Duplicate plots across phases"
