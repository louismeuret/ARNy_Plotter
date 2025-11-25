import os
import mdtraj as md
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import pandas as pd
import barnaba as bb
import matplotlib.pyplot as plt
from plotly.subplots import make_subplots
from . import energy_3dplot
from collections import defaultdict
import MDAnalysis as mda
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from src.analysis.generate_contact_maps import *
import logging
from typing import Optional, List, Dict, Tuple, Any
import traceback
from functools import wraps

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def handle_plot_errors(func):
    """Decorator to handle common plotting errors"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        try:
            logger.info(f"Starting {func_name}")
            result = func(*args, **kwargs)
            logger.info(f"Completed {func_name} successfully")
            return result
        except Exception as e:
            logger.error(f"Error in {func_name}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    return wrapper

@handle_plot_errors
def plot_ermsd(ermsd: np.ndarray, plot_settings: dict = {}) -> go.Figure:
    # Apply plot settings with defaults
    title = plot_settings.get("title", "eRMSD Scatter Plot")
    color_scheme = plot_settings.get("color_scheme", "viridis")
    heavy_atom = plot_settings.get("heavy_atom", True)
    
    # Set marker color based on color scheme
    if color_scheme == "viridis":
        marker_color = "green"
    elif color_scheme == "plasma":
        marker_color = "purple"
    elif color_scheme == "inferno":
        marker_color = "orange"
    else:
        marker_color = "green"  # default
    
    fig = go.Figure(
        data=go.Scattergl(
            y=ermsd, 
            mode="markers", 
            legendrank=1,
            marker=dict(color=marker_color)
        ),
        layout=dict(
            title=title,
            xaxis_title="Frame",
            yaxis_title="eRMSD Value (Å)" + (" - Heavy Atoms" if heavy_atom else " - All Atoms"),
            hovermode="closest"
        )
    )
    return fig

@handle_plot_errors
def plot_rmsd(rmsd: np.ndarray, plot_settings: dict = {}) -> go.Figure:
    # Apply plot settings with defaults
    title = plot_settings.get("title", "RMSD Scatter Plot")
    color_scheme = plot_settings.get("color_scheme", "viridis")
    heavy_atom = plot_settings.get("heavy_atom", True)
    
    # Set marker color based on color scheme
    if color_scheme == "viridis":
        marker_color = "green"
    elif color_scheme == "plasma":
        marker_color = "purple"
    elif color_scheme == "inferno":
        marker_color = "orange"
    else:
        marker_color = "green"  # default
    
    fig = go.Figure(
        data=go.Scattergl(
            y=rmsd, 
            mode="markers", 
            legendrank=1,
            marker=dict(color=marker_color)
        ),
        layout=dict(
            title=title,
            xaxis_title="Frame",
            yaxis_title="RMSD Value (Å)" + (" - Heavy Atoms" if heavy_atom else " - All Atoms"),
            hovermode="closest"
        )
    )
    return fig

@handle_plot_errors
def plot_dotbracket(dotbracket_data: List[str], plot_settings: dict = {}) -> go.Figure:
    # Extract plot settings with defaults
    title = plot_settings.get('title', "Timeline of RNA Structures")
    line_width = plot_settings.get('line_width', 8)
    
    reverse_mapping = {}
    for frame, dotbracket in enumerate(dotbracket_data, start=1):
        reverse_mapping.setdefault(dotbracket, []).append(frame)

    # Count frequency of each structure
    structure_frequencies = {dotbracket: len(frames) for dotbracket, frames in reverse_mapping.items()}

    # If more than 20 structures, keep only the top 20 most frequent
    if len(structure_frequencies) > 20:
        sorted_structures = sorted(structure_frequencies.items(), key=lambda x: x[1], reverse=True)
        top_20_structures = {struct for struct, _ in sorted_structures[:20]}
        # Filter reverse_mapping to keep only top 20
        reverse_mapping = {k: v for k, v in reverse_mapping.items() if k in top_20_structures}
        print(f"Filtered dotbracket structures: showing top 20 out of {len(structure_frequencies)} total structures")

    structures_data = []
    for dotbracket, frames in reverse_mapping.items():
        for frame in frames:
            structures_data.append({"Frame": frame, "DotBracket": dotbracket})

    structures_df = pd.DataFrame(structures_data)
    structures_df.sort_values(by=["DotBracket", "Frame"], inplace=True)

    unique_structures = structures_df["DotBracket"].unique()
    structure_colors = [
        f"rgb{tuple(int(255 * x) for x in plt.cm.tab20b(i)[:3])}"
        for i in np.linspace(0, 1, len(unique_structures))
    ]
    color_dict = dict(zip(unique_structures, structure_colors))

    traces = []
    for i, dotbracket in enumerate(unique_structures):
        structure_df = structures_df[structures_df["DotBracket"] == dotbracket]
        x_values = []
        y_values = []
        prev_frame = None
        for _, row in structure_df.iterrows():
            frame = row["Frame"]
            if prev_frame is not None and frame != prev_frame + 1:
                x_values.append(None)
                y_values.append(None)
            x_values.append(frame)
            y_values.append(i + 1 - 0.2)
            prev_frame = frame
        trace = go.Scattergl(
            x=x_values,
            y=y_values,
            mode="lines",
            line=dict(color=color_dict[dotbracket], width=line_width),
            name=dotbracket,
        )
        traces.append(trace)

    layout = go.Layout(
        title=title,
        xaxis=dict(title="Frame", showgrid=False, zeroline=False, showline=False),
        yaxis=dict(
            title="Dot-Bracket Structure",
            tickmode="array",
            tickvals=list(range(1, len(unique_structures) + 1)),
            ticktext=unique_structures,
            showgrid=False,
            zeroline=False,
            showline=False,
            ticklen=1,
        ),
        showlegend=False,
        plot_bgcolor="rgba(255, 255, 255, 0)",
    )

    fig = go.Figure(data=traces, layout=layout)
    return fig

def plot_torsion_time_series(angles: np.ndarray, res: List[str], torsionResidue: int, plot_settings: dict = {}) -> go.Figure:
    """Create time series plot for torsion angles"""
    if isinstance(torsionResidue, str) and torsionResidue.isdigit():
        residue_index = int(torsionResidue)
    elif isinstance(torsionResidue, int):
        residue_index = torsionResidue
    else:
        residue_index = res.index(str(torsionResidue)) if str(torsionResidue) in res else 0
    
    residue_index = min(residue_index, len(res) - 1) if len(res) > 0 else 0
    specific_residue_angles = angles[:, residue_index, :]
    angles_names = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "chi"]
    
    # Extract plot settings with defaults
    title = plot_settings.get('title', f"Torsion Angles Time Series - Residue {res[residue_index]} ({residue_index})")
    custom_colors = plot_settings.get('angle_colors', ["#636efa", "#ef553b", "#00cc96", "#a45bf9", "#fa9f5e", "#1ad0f2", "#ff6692"])
    line_width = plot_settings.get('line_width', 2)
    marker_size = plot_settings.get('marker_size', 3)
    
    fig = go.Figure()
    
    # Add time series traces for each angle
    for i in range(7):
        y_values = specific_residue_angles[:, i]
        x_values = np.arange(len(y_values))
        valid_indices = ~np.isnan(y_values)
        
        fig.add_trace(go.Scattergl(
            x=x_values[valid_indices],
            y=y_values[valid_indices],
            mode="lines+markers",
            name=f"{angles_names[i]}",
            line=dict(color=custom_colors[i], width=line_width),
            marker=dict(size=marker_size, color=custom_colors[i]),
        ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Frame Number",
        yaxis_title="Angle (degrees)",
        height=600,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
    )
    
    return fig

def plot_torsion_distributions(angles: np.ndarray, res: List[str], torsionResidue: int, plot_settings: dict = {}) -> go.Figure:
    """Create distribution plot for torsion angles"""
    if isinstance(torsionResidue, str) and torsionResidue.isdigit():
        residue_index = int(torsionResidue)
    elif isinstance(torsionResidue, int):
        residue_index = torsionResidue
    else:
        residue_index = res.index(str(torsionResidue)) if str(torsionResidue) in res else 0
    
    residue_index = min(residue_index, len(res) - 1) if len(res) > 0 else 0
    specific_residue_angles = angles[:, residue_index, :]
    angles_names = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "chi"]
    
    # Extract plot settings with defaults
    title = plot_settings.get('title', f"Torsion Angles Distribution - Residue {res[residue_index]} ({residue_index})")
    custom_colors = plot_settings.get('angle_colors', ["#636efa", "#ef553b", "#00cc96", "#a45bf9", "#fa9f5e", "#1ad0f2", "#ff6692"])
    nbins = plot_settings.get('histogram_bins', 30)
    
    # Create subplots with 7 rows and 1 column for distributions
    fig = make_subplots(
        rows=7,
        cols=1,
        subplot_titles=[f"{angle} Distribution" for angle in angles_names],
        vertical_spacing=0.06,
    )
    
    for i in range(7):
        y_values = specific_residue_angles[:, i]
        valid_indices = ~np.isnan(y_values)
        
        fig.add_trace(
            go.Histogram(
                x=y_values[valid_indices],
                name=f"{angles_names[i]}",
                marker=dict(color=custom_colors[i], opacity=0.7),
                nbinsx=nbins,
                showlegend=False,
            ),
            row=i + 1,
            col=1,
        )
        
        # Update axis labels for each subplot
        fig.update_xaxes(title_text="Angle (radians)", row=i+1, col=1)
        fig.update_yaxes(title_text="Frequency", row=i+1, col=1)
    
    fig.update_layout(
        title=title,
        height=600,
        showlegend=False,
        margin=dict(t=50, b=50, l=60, r=20)
    )
    
    return fig

@handle_plot_errors
def plot_torsion(angles: np.ndarray, res: List[str], torsionResidue: int, plot_settings: dict = {}) -> List[go.Figure]:
    """Create both time series and distribution plots for torsion angles"""
    time_series_fig = plot_torsion_time_series(angles, res, torsionResidue, plot_settings)
    distribution_fig = plot_torsion_distributions(angles, res, torsionResidue, plot_settings)
    return [time_series_fig, distribution_fig]

# Keep the old implementation as fallback but rename it
def plot_torsion_old(angles: np.ndarray, res: List[str], torsionResidue: int) -> go.Figure:
    if isinstance(torsionResidue, str) and torsionResidue.isdigit():
        residue_index = int(torsionResidue)
    elif isinstance(torsionResidue, int):
        residue_index = torsionResidue
    else:
        residue_index = res.index(str(torsionResidue)) if str(torsionResidue) in res else 0

    # Ensure residue_index is within bounds
    residue_index = min(residue_index, len(res) - 1) if len(res) > 0 else 2
    specific_residue_angles = angles[:, residue_index, :]  # All frames for residue 2
    angles_names = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "chi"]

    # Define colors for each angle in hexadecimal
    colors = [
        "#636efa",
        "#ef553b",
        "#00cc96",
        "#a45bf9",
        "#fa9f5e",
        "#1ad0f2",
        "#ff6692",
    ]

    # Create subplots with 7 rows and 2 columns
    subplot_titles = []
    for i in range(7):
        subplot_titles.extend([
            f"{angles_names[i]} - Time Series",
            f"{angles_names[i]} - Distribution"
        ])
    
    fig = make_subplots(
        rows=7,
        cols=2,
        shared_xaxes=False,
        subplot_titles=subplot_titles,
        horizontal_spacing=0.15,  # Increase spacing between columns
        vertical_spacing=0.08,    # Reduce vertical spacing
        specs=[[{"secondary_y": False}, {"secondary_y": False}] for _ in range(7)]
    )

    # Add traces for each angle (line plots in the first column and histograms in the second column)
    for i in range(7):
        y_values = specific_residue_angles[:, i]
        x_values = np.arange(len(y_values))
        valid_indices = ~np.isnan(y_values)
        color = colors[i]

        # Line plot (time series)
        fig.add_trace(
            go.Scattergl(
                x=x_values[valid_indices],
                y=y_values[valid_indices],
                mode="lines+markers",
                name=f"{angles_names[i]}",
                line=dict(color=color, width=2),
                marker=dict(size=3, color=color),
                showlegend=True,
            ),
            row=i + 1,
            col=1,
        )

        # Histogram (distribution)
        fig.add_trace(
            go.Histogram(
                x=y_values[valid_indices],
                name=f"{angles_names[i]} Dist",
                marker=dict(color=color, opacity=0.7),
                nbinsx=30,
                showlegend=False,
            ),
            row=i + 1,
            col=2,
        )

    # Update layout
    fig.update_layout(
        height=1600,
        width=1800,
        title_text=f"Torsion Angles for Residue {res[residue_index]} ({residue_index})",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Update x and y axis labels
    for i in range(7):
        # Time series subplot
        fig.update_xaxes(title_text="Frame Number", row=i+1, col=1)
        fig.update_yaxes(title_text="Angle (degrees)", row=i+1, col=1)
        
        # Histogram subplot  
        fig.update_xaxes(title_text="Angle (degrees)", row=i+1, col=2)
        fig.update_yaxes(title_text="Frequency", row=i+1, col=2)

    return fig

def plot_torsion_enhanced(angles: np.ndarray, res: List[str], torsion_params: dict) -> go.Figure:
    """Enhanced torsion plotting with multiple residue and angle selection support"""
    
    mode = torsion_params.get("torsionMode", "single")
    selected_residues = torsion_params.get("torsionResidues", [])
    selected_angles = torsion_params.get("torsionAngles", [])
    
    angles_names = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "chi"]
    colors = ["#636efa", "#ef553b", "#00cc96", "#a45bf9", "#fa9f5e", "#1ad0f2", "#ff6692"]
    
    if mode == "all":
        # Plot all angles for all residues
        return plot_all_torsions(angles, res, angles_names, colors)
    elif mode == "multiple" and selected_residues and selected_angles:
        # Plot selected angles for selected residues
        return plot_multiple_torsions(angles, res, selected_residues, selected_angles, angles_names, colors)
    else:
        # Fallback to single residue plot
        torsion_residue = torsion_params.get("torsionResidue", 0)
        return plot_torsion(angles, res, torsion_residue)

def plot_all_torsions(angles: np.ndarray, res: List[str], angles_names: List[str], colors: List[str]) -> go.Figure:
    """Plot all torsion angles for all residues in a comprehensive view"""
    
    # Create a heatmap-style visualization
    fig = go.Figure()
    
    # For each angle type, create a heatmap
    for angle_idx, angle_name in enumerate(angles_names):
        # Extract data for this angle across all residues and frames
        angle_data = angles[:, :, angle_idx]  # frames x residues
        
        # Create heatmap
        fig.add_trace(go.Heatmap(
            z=angle_data.T,  # Transpose so residues are on y-axis
            x=list(range(angle_data.shape[0])),  # Frame numbers
            y=[f"{res[i]} ({i+1})" for i in range(len(res))],  # Residue labels
            colorscale='Viridis',
            name=angle_name,
            visible=(angle_idx == 0),  # Only show first angle initially
            colorbar=dict(title=f"{angle_name} angle (degrees)")
        ))
    
    # Create dropdown menu to select angle type
    dropdown_buttons = []
    for i, angle_name in enumerate(angles_names):
        visible_array = [False] * len(angles_names)
        visible_array[i] = True
        dropdown_buttons.append(dict(
            label=angle_name.title(),
            method='update',
            args=[{'visible': visible_array}]
        ))
    
    fig.update_layout(
        title="All Torsion Angles Across Trajectory",
        xaxis_title="Frame",
        yaxis_title="Residue",
        height=max(400, len(res) * 20),
        updatemenus=[dict(
            buttons=dropdown_buttons,
            direction="down",
            showactive=True,
            x=1.0,
            xanchor="left",
            y=1.02,
            yanchor="top"
        )]
    )
    
    return fig

def plot_multiple_torsions(angles: np.ndarray, res: List[str], selected_residues: List[str], 
                          selected_angles: List[str], angles_names: List[str], colors: List[str]) -> go.Figure:
    """Plot selected torsion angles for selected residues"""
    
    # Convert residue names/indices to actual indices
    residue_indices = []
    for res_id in selected_residues:
        try:
            if res_id.isdigit():
                idx = int(res_id)
                if 0 <= idx < len(res):
                    residue_indices.append(idx)
            else:
                # Try to find by residue name
                for i, r in enumerate(res):
                    if r == res_id or f"{r}{i+1}" == res_id:
                        residue_indices.append(i)
                        break
        except:
            continue
    
    # Convert angle names to indices
    angle_indices = []
    for angle in selected_angles:
        if angle.lower() in [a.lower() for a in angles_names]:
            angle_indices.append([a.lower() for a in angles_names].index(angle.lower()))
    
    if not residue_indices or not angle_indices:
        # Fallback to single residue plot
        return plot_torsion(angles, res, 0)
    
    # Create subplots for each residue
    n_residues = len(residue_indices)
    n_angles = len(angle_indices)
    
    subplot_titles = []
    for idx in residue_indices:
        subplot_titles.extend([
            f"Residue {res[idx]} ({idx}) - Time Series",
            f"Residue {res[idx]} ({idx}) - Distribution"
        ])
    
    fig = make_subplots(
        rows=n_residues,
        cols=2,  # Line plot and histogram
        subplot_titles=subplot_titles,
        horizontal_spacing=0.15,
        vertical_spacing=0.08,
        specs=[[{"secondary_y": False}, {"secondary_y": False}] for _ in range(n_residues)]
    )
    
    for res_idx, residue_idx in enumerate(residue_indices):
        for angle_idx in angle_indices:
            angle_name = angles_names[angle_idx]
            color = colors[angle_idx % len(colors)]
            
            y_values = angles[:, residue_idx, angle_idx]
            x_values = np.arange(len(y_values))
            valid_indices = ~np.isnan(y_values)
            
            # Line plot
            fig.add_trace(
                go.Scattergl(
                    x=x_values[valid_indices],
                    y=y_values[valid_indices],
                    mode="lines+markers",
                    name=f"{res[residue_idx]} - {angle_name}",
                    line=dict(color=color, width=2),
                    marker=dict(size=3, color=color),
                ),
                row=res_idx + 1,
                col=1,
            )
            
            # Histogram
            fig.add_trace(
                go.Histogram(
                    x=y_values[valid_indices],
                    name=f"{res[residue_idx]} - {angle_name} Distribution",
                    marker=dict(color=color, opacity=0.7),
                    nbinsx=25,
                    showlegend=False,
                ),
                row=res_idx + 1,
                col=2,
            )
    
    fig.update_layout(
        height=400 * n_residues,
        width=1800,
        title_text=f"Selected Torsion Angles for {len(residue_indices)} Residues",
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.01
        )
    )
    
    # Update axis labels for all subplots
    for i in range(n_residues):
        # Time series subplot
        fig.update_xaxes(title_text="Frame Number", row=i+1, col=1)
        fig.update_yaxes(title_text="Angle (degrees)", row=i+1, col=1)
        
        # Histogram subplot  
        fig.update_xaxes(title_text="Angle (degrees)", row=i+1, col=2)
        fig.update_yaxes(title_text="Frequency", row=i+1, col=2)
    
    return fig

@handle_plot_errors
def plot_rna_contact_map(base_pairs_df: pd.DataFrame, sequence: List[str], output_file: Optional[str] = None, frame_number: Optional[int] = None, plot_settings: dict = {}) -> go.Figure:
    """
    Create an interactive contact map visualization for RNA base pairs
    using Plotly with Leontis-Westhof classification
    
    Parameters:
    -----------
    base_pairs_df : pandas DataFrame
        DataFrame with base pair information
    sequence : list
        RNA sequence
    output_file : str, optional
        Path to save the HTML or image visualization
    frame_number : int, optional
        Frame number for multi-frame data
    
    Returns:
    --------
    fig : plotly.graph_objects.Figure
        Plotly figure object
    """
    # Extract plot settings with defaults
    title = plot_settings.get('title', "RNA Contact Map with Leontis-Westhof Base Pairs")
    colorscale = plot_settings.get('colorscale', 'default')
    marker_size = plot_settings.get('marker_size', 15)
    
    fig = go.Figure()
    if base_pairs_df.empty:
        print("No base pairs to display")
        fig.add_annotation(
            text="No base pairs to display",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color="red")
        )
        # Update layout to center the text
        fig.update_layout(
            title=title,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            width=800,
            height=700
        )
        return fig
    
    # Define a precise color mapping for each annotation
    color_map = {
        # Watson-Crick
        'WCc': '#E41A1C',    # Red - canonical Watson-Crick
        'WWc': '#E41A1C',    # Red - canonical Watson-Crick (alternative notation)
        'GUc': '#FF7F00',    # Orange - G-U wobble pairs
        
        # Hoogsteen pairs
        'WHc': '#4DAF4A',    # Green - Watson-Crick/Hoogsteen cis
        'WHt': '#984EA3',    # Purple - Watson-Crick/Hoogsteen trans
        'HWt': '#984EA3',    # Purple - Hoogsteen/Watson-Crick trans
        'HWc': '#4DAF4A',    # Green - Hoogsteen/Watson-Crick cis
        'HHc': '#A65628',    # Brown - Hoogsteen/Hoogsteen cis
        'HHt': '#F781BF',    # Pink - Hoogsteen/Hoogsteen trans
        
        # Sugar edge
        'WSc': '#377EB8',    # Blue - Watson-Crick/Sugar edge cis
        'WSt': '#FFFF33',    # Yellow - Watson-Crick/Sugar edge trans
        'SWc': '#377EB8',    # Blue - Sugar edge/Watson-Crick cis
        'SWt': '#FFFF33',    # Yellow - Sugar edge/Watson-Crick trans
        'SHc': '#66C2A5',    # Turquoise - Sugar edge/Hoogsteen cis
        'SHt': '#8DA0CB',    # Light blue - Sugar edge/Hoogsteen trans
        'HSc': '#66C2A5',    # Turquoise - Hoogsteen/Sugar edge cis
        'HSt': '#8DA0CB',    # Light blue - Hoogsteen/Sugar edge trans
        'SSc': '#FC8D62',    # Coral - Sugar edge/Sugar edge cis
        'SSt': '#BEBADA',    # Lavender - Sugar edge/Sugar edge trans
        
        # Other
        'XXX': '#C0C0C0',    # Silver - Not classified or undefined
    }
    
    
    # Group by annotation type
    annotations = base_pairs_df['anno'].unique()
    
    # Add base-pairs as scatter points, grouped by annotation
    for anno in annotations:
        # Get subset of pairs with this annotation
        subset = base_pairs_df[base_pairs_df['anno'] == anno]
        
        if not subset.empty:
            # Determine marker symbol based on the annotation type
            marker_symbol = 'circle'
            
            # For annotations in Leontis-Westhof notation, customize the marker
            if anno != 'XXX' and len(anno) == 3:
                # Different symbols based on interaction edges
                if 'H' in anno:
                    marker_symbol = 'diamond'
                elif 'S' in anno:
                    marker_symbol = 'square'
                
                # Make markers different for cis vs trans
                if anno.endswith('t'):
                    marker_symbol = 'star'
            
            # Get the color for this annotation type
            color = color_map.get(anno, '#C0C0C0')
            
            # Add points for this interaction type
            fig.add_trace(go.Scatter(
                x=subset['res_i'],
                y=subset['res_j'],
                mode='markers',
                marker=dict(
                    size=8,
                    symbol=marker_symbol,
                    color=color,
                    line=dict(width=1, color='DarkSlateGrey')
                ),
                name=anno,  # Use anno directly as the trace name
                hovertemplate=(
                    'Base pair: %{customdata[0]} ↔ %{customdata[1]}<br>' +
                    'Type: ' + anno + '<br>' +
                    'Residue i: %{x}<br>' +
                    'Residue j: %{y}<br>'
                ),
                customdata=list(zip(subset['res_i_name'], subset['res_j_name']))
            ))
            
            # Add points for the symmetrical pairs (lower triangle)
            fig.add_trace(go.Scatter(
                x=subset['res_j'],
                y=subset['res_i'],
                mode='markers',
                marker=dict(
                    size=8,
                    symbol=marker_symbol,
                    color=color,
                    line=dict(width=1, color='DarkSlateGrey')
                ),
                name=f"{anno} (sym)",
                showlegend=False,
                hovertemplate=(
                    'Base pair: %{customdata[1]} ↔ %{customdata[0]}<br>' +
                    'Type: ' + anno + '<br>' +
                    'Residue j: %{x}<br>' +
                    'Residue i: %{y}<br>'
                ),
                customdata=list(zip(subset['res_i_name'], subset['res_j_name']))
            ))
    
    # Add sequence ticks if sequence is available
    n_residues = len(sequence)
    tick_vals = list(range(1, n_residues + 1))
    tick_text = [f"{seq}{i}" for i, seq in enumerate(sequence, 1)]
    
    # Add diagonal line
    fig.add_trace(go.Scatter(
        x=[1, n_residues],
        y=[1, n_residues],
        mode='lines',
        line=dict(color='black', width=1, dash='dash'),
        showlegend=False
    ))
    
    # Update layout with better titles and hover info
    final_title = title
    if frame_number is not None:
        final_title += f" - Frame {frame_number}"
        
    fig.update_layout(
        title=final_title,
        xaxis=dict(
            title="Nucleotide position",
            tickvals=tick_vals,
            ticktext=tick_text,
            tickangle=45
        ),
        yaxis=dict(
            title="Nucleotide position",
            tickvals=tick_vals,
            ticktext=tick_text
        ),
        width=800,
        height=700,
        legend_title="Base Pair Types:",
        hovermode="closest"
    )
    
    # Save to file if specified
    if output_file:
        if output_file.endswith('.html'):
            fig.write_html(output_file)
        else:
            fig.write_image(output_file)
        print(f"Contact map saved to {output_file}")
    
    return fig
    
def plot_sec_structure(sec_structure):
    fig = go.Figure(data=go.Scattergl(y=sec_structure, mode="markers"))
    fig.update_layout(
        title="Secondary Structure Plot",
        xaxis_title="Frame",
        yaxis_title="Secondary Structure",
    )
    return fig

def plot_landscapes_3D(
    energy_matrix, Qbin, RMSDbin, max_RMSD, max_Q,  real_values, selected_regions, names_axis
):
    fig = energy_3dplot.energy_plot_3d(
        energy_matrix, Qbin, RMSDbin, max_RMSD, max_Q, real_values, selected_regions, names_axis
    )
    return fig

def plot_landscapes_2D(
    energy_matrix, Qbin, RMSDbin, max_RMSD, max_Q, real_values, selected_regions, names_axis
):
    fig = energy_3dplot.energy_plot_2d(
        energy_matrix, Qbin, RMSDbin, max_RMSD, max_Q, real_values, selected_regions, names_axis
    )
    return fig

def plot_diagram_frequency(sequence, dot_bracket_list, dotbracket_native):
    def parse_dot_bracket(dot_bracket):
        stack = []
        pairs = []
        logging.debug(f"Parsing dot-bracket: {dot_bracket}")

        for i, char in enumerate(dot_bracket):
            if char == '(':
                stack.append(i)
            elif char == ')':
                if stack:
                    pairs.append((stack.pop(), i))

        logging.debug(f"Pairs found by parser: {pairs}")
        return pairs

    # Function to calculate pair frequencies from a list of dot-bracket structures
    def calculate_pair_frequencies(dot_bracket_list):
        pair_counts = defaultdict(int)
        total_pairs = 0

        for dot_bracket in dot_bracket_list:
            pairs = parse_dot_bracket(dot_bracket)
            for pair in pairs:
                pair_counts[pair] += 1
                total_pairs += 1

        pair_frequencies = {pair: count / total_pairs for pair, count in pair_counts.items()}
        logging.debug(f"Pair frequencies: {pair_frequencies}")
        return pair_frequencies

    # Function to map nucleotide to color
    def nucleotide_color(nucleotide):
        color_map = {
            'A': 'red',
            'U': 'green',
            'G': 'blue',
            'C': 'orange'
        }
        return color_map.get(nucleotide, 'black')  # Default to black if unknown nucleotide

    # Function to plot arc diagram
    def plot_arc_diagram(sequence, pair_frequencies, pairs, plot_settings: dict = {}):
        logging.debug(f"Pairs for arc diagram: {pairs}")

        # Extract plot settings with defaults
        title = plot_settings.get('title', "RNA Secondary Structure Arc Diagram with Frequency-Based Coloring")
        node_size = plot_settings.get('node_size', 10)
        arc_width = plot_settings.get('arc_width', 2)
        custom_colorscale = plot_settings.get('arc_colorscale', [
            [0.0, 'rgb(0, 0, 255)'],   # Blue
            [1.0, 'rgb(255, 0, 0)']    # Red
        ])

        fig = go.Figure()

        # Plot nodes with nucleotide-specific colors
        for i, nucleotide in enumerate(sequence):
            color = nucleotide_color(nucleotide)
            fig.add_trace(go.Scattergl(
                x=[i], y=[0], mode='markers+text', text=nucleotide, textposition="bottom center",
                marker=dict(size=node_size, color=color),
                showlegend=False
            ))

        # Get the frequency for each pair
        pairs_unique = list(pair_frequencies.keys())
        frequencies = [pair_frequencies.get(pair, 0) for pair in pairs_unique]

        min_frequency = min(frequencies)
        max_frequency = max(frequencies)

        if frequencies:
            norm_frequencies = [(f - min(frequencies)) / (max(frequencies) - min(frequencies)) for f in frequencies]
        else:
            norm_frequencies = [0] * len(pairs)

        # Plot arcs for edges with frequency-based colors
        for (start, end), norm_frequency, frequency in zip(pairs_unique, norm_frequencies, frequencies):
            center = (start + end) / 2
            radius = (end - start) / 2
            theta = np.linspace(0, np.pi, 100)
            x = center + radius * np.cos(theta)
            y = radius * np.sin(theta)
            color = px.colors.sample_colorscale(custom_colorscale, norm_frequency)
            fig.add_trace(go.Scattergl(
                x=x, y=y, mode='lines',
                line=dict(color=color[0], width=arc_width),
                showlegend=False,
                hoverinfo='text',
                text=f'Frequency: {frequency:.2f}'
            ))

        fig.add_trace(go.Scattergl(
            x=[None], y=[None],
            mode='markers',
            marker=dict(
                colorscale=custom_colorscale,
                cmin=min_frequency, cmax=max_frequency,
                colorbar=dict(
                    title="Frequency",
                    tickvals=np.linspace(min_frequency, max_frequency, num=5),
                    ticktext=[f'{v:.2f}' for v in np.linspace(min_frequency, max_frequency, num=5)],
                    x=1.1,  # Position color bar to the right of the plot
                    y=0.5,
                    len=0.75,
                    thickness=20
                ),
                showscale=True
            ),
            hoverinfo='none'
        ))

        # Update layout
        fig.update_layout(
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            showlegend=False,
            width=800,
            height=400,
            plot_bgcolor='white',
            title=title
        )
        return fig

    logging.debug(f"Dot-bracket list: {dot_bracket_list}")
    # Calculate pair frequencies from the dot-bracket list
    pair_frequencies = calculate_pair_frequencies(dot_bracket_list)
    logging.debug(f"Final pair frequencies: {pair_frequencies}")

    pairs = parse_dot_bracket(dotbracket_native[0])
    logging.debug(f"Pairs found in native structure: {pairs}")
    # Plot arc diagram
    fig = plot_arc_diagram(sequence, pair_frequencies, pairs)
    return fig

def base_pairs_visualisation(rr):
    z = rr[:, 2]
    rho = np.sqrt(rr[:, 0]**2 + rr[:, 1]**2)
    pairs = rr[np.where(np.abs(z) < 0.18)]
    # in case it's having to much issues chuggling the data
    sampled_pairs = pairs[::]

    # Define x and y for the contour plot
    x = sampled_pairs[:, 0]
    y = sampled_pairs[:, 1]

    # Create grid and histogram
    grid_size = 100
    x_edges = np.linspace(-1.1, 1.1, grid_size)
    y_edges = np.linspace(-1.1, 1.1, grid_size)
    heatmap, x_edges, y_edges = np.histogram2d(x, y, bins=[x_edges, y_edges])
    logging.debug(f"Size x and y = {len(x), len(y), x, y}")
    if len(x) > 100000:
        max_threshold = np.percentile(heatmap, 98)  # Clip at the 98th percentile to highlight middle/high-density
        min_threshold = 5
    else:
        min_threshold = 0
        max_threshold = np.percentile(heatmap, 100)  # Clip at the 100th percentile to highlight all values
    heatmap = np.clip(heatmap, min_threshold, max_threshold)  # Clip the histogram data

    # Create the contour plot using the clipped data
    # Ensure that size is positive and within an appropriate range
    size_value = (max_threshold - min_threshold) / 20
    if size_value <= 0:
        size_value = 0.1  # Set a default size if the calculated value is not positive

    fig = go.Figure(data=go.Contour(
        z=heatmap.T,
        x=x_edges,
        y=y_edges,
        colorscale="OrRd",
        contours=dict(
            start=min_threshold,  # Start at the minimum threshold
            end=max_threshold,  # End at the maximum threshold
            size=size_value,  # Use validated size
            coloring="heatmap",
            showlabels=True,
            labelfont=dict(size=12, color="white")
        ),
        colorbar=dict(
            title="Density",
        )
    ))

    # Add concentric circles and angular lines
    circle_radii = [0.5, 0.75, 1.0]
    for r in circle_radii:
        fig.add_shape(type="circle",
                      xref="x", yref="y",
                      x0=-r, y0=-r, x1=r, y1=r,
                      line=dict(color="black", width=1, dash="dash"))

    for r, label in zip(circle_radii, ["r=0.5 nm", "r=0.75 nm", "r=1.0 nm"]):
        fig.add_annotation(x=-r, y=0, text=label, showarrow=False, font=dict(size=13), xshift=-10)

    # Add region labels
    region_labels = {
        (0.35, 0.45): "Watson-Crick",
        (0.1, 0.6): "GU",
        (0.5, 0.3): "GU",
        (-0.6, 0.4): "Hoogsteen",
        (0.6, -0.4): "Sugar"
    }

    for (x, y), text in region_labels.items():
        fig.add_annotation(x=x, y=y, text=text, showarrow=False, font=dict(size=17), font_color="black")

    def create_polygon_path(center, radius, num_vertices, orientation=0):
        points = []
        for i in range(num_vertices):
            angle = orientation + i * (2 * np.pi / num_vertices)
            x = center[0] + radius * np.cos(angle)
            y = center[1] + radius * np.sin(angle)
            points.append(f"L {x} {y}")
        path = f"M {points[0][2:]} " + " ".join(points[1:]) + " Z"
        return path

    # Hexagon (centered at [0, 0], radius = 0.28, orientation = π/2)
    hexagon_path = create_polygon_path(center=[0, 0], radius=0.28, num_vertices=6, orientation=0)
    fig.add_shape(type="path", path=hexagon_path, line_color="black")

    # Pentagon (centered at [-0.375, -0.225], radius = 0.24, orientation = -0.42 radians)
    pentagon_path = create_polygon_path(center=[-0.375, -0.225], radius=0.24, num_vertices=5, orientation=-0.08)
    fig.add_shape(type="path", path=pentagon_path, line_color="black")

    # Customize layout
    fig.update_layout(
        xaxis=dict(range=[-1.1, 1.1], title="x (nm)", zeroline=False),
        yaxis=dict(range=[-1.1, 1.1], title="y (nm)", zeroline=False),
        height=1000,
        title="Visualisation of 2d base pairing",
        showlegend=False
    )

    return fig

def save_nth_frame(traj_file, top_file, index, session):
    # Load the trajectory and topology
    u = mda.Universe(top_file, traj_file)

    # Select the nth frame
    u.trajectory[index]

    # Define the path to save the frame
    path_save = os.path.join('static', 'uploads', session, 'temp_frame.pdb')

    # Save the frame
    with mda.Writer(path_save, multiframe=False) as W:
        W.write(u)

    return path_save

def create_contact_maps(traj_file, top_file, index, session):
    path_save_contact_maps = os.path.join('static', 'uploads', session, 'target.cmp')
    path_save_figure = os.path.join('static', 'uploads', session, 'contact_map_plotly.png')

    path_traj_file = os.path.join('static', 'uploads', session, traj_file)
    path_top_file = os.path.join('static', 'uploads', session, top_file)

    path_save_nth_frame = save_nth_frame(path_traj_file, path_top_file, index, session)
    contact_map, signature = generate_contact_maps.get_contact_map_and_signature(path_save_nth_frame)
    generate_contact_maps.write_contact_map(path_save_contact_maps, contact_map, signature)
    generate_contact_maps.read_contact_map(path_save_contact_maps, path_save_figure)


@handle_plot_errors
def plot_radius_of_gyration(frames: List[int], rg_total: List[float], rg_components: List[List[float]], plot_settings: dict = {}) -> go.Figure:
    """Create interactive radius of gyration plot"""
    
    # Apply plot settings with defaults
    title = plot_settings.get("title", "Radius of Gyration Analysis")
    color_scheme = plot_settings.get("color_scheme", "viridis")
    show_components = plot_settings.get("show_components", True)
    
    # Set colors based on color scheme
    if color_scheme == "viridis":
        colors = {"total": "blue", "x": "red", "y": "green", "z": "orange"}
    elif color_scheme == "plasma":
        colors = {"total": "purple", "x": "magenta", "y": "blue", "z": "cyan"}
    elif color_scheme == "inferno":
        colors = {"total": "red", "x": "orange", "y": "yellow", "z": "pink"}
    else:
        colors = {"total": "blue", "x": "red", "y": "green", "z": "orange"}  # default
    
    # Extract individual component arrays
    rg_x = [comp[0] for comp in rg_components]
    rg_y = [comp[1] for comp in rg_components]
    rg_z = [comp[2] for comp in rg_components]
    
    fig = go.Figure()
    
    # Add total radius of gyration
    fig.add_trace(go.Scattergl(
        x=frames,
        y=rg_total,
        mode='lines',
        name='Total Rg',
        line=dict(color=colors["total"], width=2),
        hovertemplate='Frame: %{x}<br>Total Rg: %{y:.2f} Å<extra></extra>'
    ))
    
    # Add components if enabled in settings
    if show_components:
        fig.add_trace(go.Scattergl(
            x=frames,
            y=rg_x,
            mode='lines',
            name='Rg X',
            line=dict(color=colors["x"], width=1, dash='dash'),
            hovertemplate='Frame: %{x}<br>Rg X: %{y:.2f} Å<extra></extra>'
        ))
        
        fig.add_trace(go.Scattergl(
            x=frames,
            y=rg_y,
            mode='lines',
            name='Rg Y',
            line=dict(color=colors["y"], width=1, dash='dash'),
            hovertemplate='Frame: %{x}<br>Rg Y: %{y:.2f} Å<extra></extra>'
        ))
        
        fig.add_trace(go.Scattergl(
            x=frames,
            y=rg_z,
            mode='lines',
            name='Rg Z',
            line=dict(color=colors["z"], width=1, dash='dash'),
            hovertemplate='Frame: %{x}<br>Rg Z: %{y:.2f} Å<extra></extra>'
        ))
    
    # Update layout
    fig.update_layout(
        title=f"{title} ({len(frames)} frames)",
        xaxis_title="Frame",
        yaxis_title="Radius of Gyration (Å)",
        hovermode='x unified',
        showlegend=True,
        template='plotly_white'
    )
    
    return fig


@handle_plot_errors
def plot_end_to_end_distance(frames: List[int], distances: List[float], plot_settings: dict = {}) -> go.Figure:
    """Create interactive end-to-end distance plot"""
    
    # Extract plot settings with defaults
    title = plot_settings.get('title', f"End-to-End Distance (5' to 3') vs Frame ({len(frames)} frames)")
    color_scheme = plot_settings.get('color_scheme', 'purple')
    five_prime_atom = plot_settings.get('five_prime_atom', "P")
    three_prime_atom = plot_settings.get('three_prime_atom', "P")
    
    fig = go.Figure()
    
    # Add end-to-end distance trace
    fig.add_trace(go.Scattergl(
        x=frames,
        y=distances,
        mode='lines+markers',
        name='End-to-End Distance',
        line=dict(color=color_scheme, width=2),
        marker=dict(size=4),
        hovertemplate='Frame: %{x}<br>Distance: %{y:.2f} Å<extra></extra>'
    ))
    
    # Update layout
    fig.update_layout(
        title=title,
        xaxis_title="Frame",
        yaxis_title="End-to-End Distance (Å)",
        hovermode='x unified',
        showlegend=True,
        template='plotly_white'
    )
    
    return fig


@handle_plot_errors
def plot_dimensionality_reduction(frames: List[int], method_data: np.ndarray, method: str = 'pca', plot_settings: dict = {}) -> go.Figure:
    """Create interactive dimensionality reduction plot"""
    
    fig = go.Figure()
    
    # Method-specific configurations
    method_title = {
        'pca': 'Principal Component Analysis',
        'umap': 'UMAP',
        'tsne': 't-SNE'
    }
    
    axis_labels = {
        'pca': ('PC1', 'PC2'),
        'umap': ('UMAP1', 'UMAP2'),
        'tsne': ('t-SNE1', 't-SNE2')
    }
    
    # Extract plot settings with defaults
    title = plot_settings.get('title', f"{method_title.get(method, method.upper())} - Conformational Landscape ({len(frames)} frames)")
    color_scheme = plot_settings.get('color_scheme', 'Viridis')
    marker_size = plot_settings.get('marker_size', 6)
    
    # Add scatter plot with color gradient for frames
    fig.add_trace(go.Scattergl(
        x=method_data[:, 0],
        y=method_data[:, 1],
        mode='markers',
        marker=dict(
            size=marker_size,
            color=frames,
            colorscale=color_scheme,
            colorbar=dict(title="Frame")
        ),
        text=[f"Frame: {frame}" for frame in frames],
        hovertemplate='%{text}<br>PC1: %{x:.2f}<br>PC2: %{y:.2f}<extra></extra>' if method == 'pca' else f'%{{text}}<br>{method.upper()}1: %{{x:.2f}}<br>{method.upper()}2: %{{y:.2f}}<extra></extra>',
        name=f'{method.upper()} Projection'
    ))
    
    # Update layout
    fig.update_layout(
        title=title,
        xaxis_title=axis_labels.get(method, (f'{method.upper()}1', f'{method.upper()}2'))[0],
        yaxis_title=axis_labels.get(method, (f'{method.upper()}1', f'{method.upper()}2'))[1],
        hovermode='closest',
        showlegend=True,
        template='plotly_white'
    )
    
    return fig
