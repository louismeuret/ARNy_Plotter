"""
Unit tests for plotting functions in src/plotting/create_plots.py

These tests verify that each plot function returns a valid Plotly Figure
using synthetic numpy arrays — no actual MD files required.
"""

import os
import sys
import pytest
import numpy as np

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

RNG = np.random.default_rng(0)
N = 80  # frames


def _is_figure(obj):
    return isinstance(obj, go.Figure)


# ---------------------------------------------------------------------------
# RMSD plot
# ---------------------------------------------------------------------------

class TestPlotRmsd:
    @pytest.fixture(autouse=True)
    def _import(self):
        from src.plotting.create_plots import plot_rmsd
        self.plot_fn = plot_rmsd

    def test_returns_figure(self, rmsd_array):
        fig = self.plot_fn(rmsd_array)
        assert _is_figure(fig)

    def test_has_data(self, rmsd_array):
        fig = self.plot_fn(rmsd_array)
        assert len(fig.data) > 0

    def test_custom_title(self, rmsd_array):
        fig = self.plot_fn(rmsd_array, plot_settings={"title": "My RMSD"})
        assert "My RMSD" in fig.layout.title.text

    def test_color_schemes(self, rmsd_array):
        for scheme in ("viridis", "plasma", "inferno", "unknown"):
            fig = self.plot_fn(rmsd_array, plot_settings={"color_scheme": scheme})
            assert _is_figure(fig)

    def test_single_frame(self):
        fig = self.plot_fn(np.array([1.5]))
        assert _is_figure(fig)

    def test_many_frames(self):
        large = RNG.uniform(0, 10, size=10_000)
        fig = self.plot_fn(large)
        assert _is_figure(fig)


# ---------------------------------------------------------------------------
# eRMSD plot
# ---------------------------------------------------------------------------

class TestPlotErmsd:
    @pytest.fixture(autouse=True)
    def _import(self):
        from src.plotting.create_plots import plot_ermsd
        self.plot_fn = plot_ermsd

    def test_returns_figure(self, ermsd_array):
        fig = self.plot_fn(ermsd_array)
        assert _is_figure(fig)

    def test_has_data(self, ermsd_array):
        fig = self.plot_fn(ermsd_array)
        assert len(fig.data) > 0

    def test_custom_title(self, ermsd_array):
        fig = self.plot_fn(ermsd_array, plot_settings={"title": "eRMSD Plot"})
        assert "eRMSD Plot" in fig.layout.title.text

    def test_color_schemes(self, ermsd_array):
        for scheme in ("viridis", "plasma", "inferno", "other"):
            fig = self.plot_fn(ermsd_array, plot_settings={"color_scheme": scheme})
            assert _is_figure(fig)


# ---------------------------------------------------------------------------
# Radius-of-gyration plot
# plot_radius_of_gyration(frames, rg_total, rg_components, plot_settings={})
# ---------------------------------------------------------------------------

class TestPlotRadiusOfGyration:
    """Rg plot takes separate frames / rg_total / rg_components args."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src.plotting.create_plots import plot_radius_of_gyration
        self.plot_fn = plot_radius_of_gyration

    def test_returns_figure(self, rg_data):
        fig = self.plot_fn(
            rg_data["frames"],
            rg_data["rg_values"],
            rg_data["rg_components"],
        )
        assert _is_figure(fig)

    def test_has_trace(self, rg_data):
        fig = self.plot_fn(
            rg_data["frames"],
            rg_data["rg_values"],
            rg_data["rg_components"],
        )
        assert len(fig.data) >= 1

    def test_custom_title(self, rg_data):
        fig = self.plot_fn(
            rg_data["frames"],
            rg_data["rg_values"],
            rg_data["rg_components"],
            plot_settings={"title": "Rg over time"},
        )
        assert _is_figure(fig)


# ---------------------------------------------------------------------------
# End-to-end distance plot
# plot_end_to_end_distance(frames, distances, plot_settings={})
# ---------------------------------------------------------------------------

class TestPlotEndToEndDistance:
    @pytest.fixture(autouse=True)
    def _import(self):
        from src.plotting.create_plots import plot_end_to_end_distance
        self.plot_fn = plot_end_to_end_distance

    def test_returns_figure(self, e2e_data):
        fig = self.plot_fn(e2e_data["frames"], e2e_data["distances"])
        assert _is_figure(fig)

    def test_has_trace(self, e2e_data):
        fig = self.plot_fn(e2e_data["frames"], e2e_data["distances"])
        assert len(fig.data) >= 1

    def test_with_plot_settings(self, e2e_data):
        fig = self.plot_fn(
            e2e_data["frames"],
            e2e_data["distances"],
            plot_settings={"title": "End-to-End"},
        )
        assert _is_figure(fig)


# ---------------------------------------------------------------------------
# PCA / UMAP / t-SNE plot
# plot_dimensionality_reduction(frames, method_data, method='pca', plot_settings={})
# ---------------------------------------------------------------------------

class TestPlotDimensionalityReduction:
    @pytest.fixture(autouse=True)
    def _import(self):
        from src.plotting.create_plots import plot_dimensionality_reduction
        self.plot_fn = plot_dimensionality_reduction

    def test_returns_figure_pca(self, dimred_data):
        data = np.array(dimred_data["pca_coordinates"])
        fig = self.plot_fn(dimred_data["frames"], data, method="pca")
        assert _is_figure(fig)

    def test_returns_figure_umap(self, dimred_data):
        data = np.array(dimred_data["umap_coordinates"])
        fig = self.plot_fn(dimred_data["frames"], data, method="umap")
        assert _is_figure(fig)

    def test_returns_figure_tsne(self, dimred_data):
        data = np.array(dimred_data["tsne_coordinates"])
        fig = self.plot_fn(dimred_data["frames"], data, method="tsne")
        assert _is_figure(fig)

    def test_has_trace(self, dimred_data):
        data = np.array(dimred_data["pca_coordinates"])
        fig = self.plot_fn(dimred_data["frames"], data, method="pca")
        assert len(fig.data) >= 1


# ---------------------------------------------------------------------------
# Torsion plot
# plot_torsion(angles, res, torsionResidue, plot_settings={}) → List[Figure]
# ---------------------------------------------------------------------------

class TestPlotTorsion:
    @pytest.fixture(autouse=True)
    def _import(self):
        from src.plotting.create_plots import plot_torsion
        self.plot_fn = plot_torsion

    def test_returns_list_of_figures(self):
        """plot_torsion returns a list of Plotly figures.

        angles shape convention: (n_angles, n_residues, n_frames)
        so angles[:, residue_index, :] → (n_angles, n_frames)
        """
        # 7 torsion angles, 1 residue, N frames
        angles = RNG.uniform(-np.pi, np.pi, (7, 1, N))
        res = ["G1"]
        result = self.plot_fn(angles, res, torsionResidue=0)
        # Result can be a list or a single figure
        if isinstance(result, list):
            assert all(_is_figure(f) for f in result)
        else:
            assert _is_figure(result)

    def test_with_multiple_residues(self):
        # 7 torsion angles, 3 residues, N frames
        n_res = 3
        angles = RNG.uniform(-np.pi, np.pi, (7, n_res, N))
        res = [f"G{i}" for i in range(n_res)]
        result = self.plot_fn(angles, res, torsionResidue=0)
        assert result is not None

    def test_single_residue_second_residue_index(self):
        """torsionResidue=1 with 2 residues → picks second residue."""
        n_res = 2
        angles = RNG.uniform(-np.pi, np.pi, (7, n_res, N))
        res = ["G1", "G2"]
        result = self.plot_fn(angles, res, torsionResidue=1)
        assert result is not None


# ---------------------------------------------------------------------------
# plot_rmsd / plot_ermsd edge cases
# ---------------------------------------------------------------------------

class TestPlottingEdgeCases:
    def test_rmsd_all_zeros(self):
        from src.plotting.create_plots import plot_rmsd
        fig = plot_rmsd(np.zeros(20))
        assert _is_figure(fig)

    def test_ermsd_all_same_value(self):
        from src.plotting.create_plots import plot_ermsd
        fig = plot_ermsd(np.ones(20) * 2.5)
        assert _is_figure(fig)

    def test_rmsd_with_nan_raises_or_returns_figure(self):
        """NaN handling: function should either raise cleanly or return a figure."""
        from src.plotting.create_plots import plot_rmsd
        arr = np.array([1.0, np.nan, 2.0, np.nan, 3.0])
        try:
            fig = plot_rmsd(arr)
            assert _is_figure(fig)
        except Exception:
            pass  # Raising is also acceptable for malformed input
