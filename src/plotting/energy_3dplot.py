import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mlp
import pandas as pd
import scipy.stats as stats
import os
import math
import plotly.graph_objects as go
import plotly.express as px
from matplotlib.colors import LinearSegmentedColormap

#NB Kb*T=kT = 4.11×10−21 J according to Wikipedia

#loading data
def loaddata(filename):
	df = pd.read_csv (filename, sep='\t')
	df.columns = ["frame", "Q", 'RMSD', 'traj']
	return (df, max(df['RMSD']))

#makes matrix of empty lists
def make_emptylists_matrix(dimension):
	matrix_empty=np.empty((dimension,dimension), dtype=np.object_) #NB important to specify type of values otherwise numpy can't understand
	for i in range(matrix_empty.shape[0]):
		for j in range(matrix_empty.shape[1]):
			matrix_empty[i,j]=[]
	return(matrix_empty)

#I want a matrix (24x24) to keep bins consistent with previous 2D plots

def make_matrix_probability(df, dimension, maximal_RMSD, maximal_Q, metrics_names=None):
    """
    Create probability matrix for energy landscape
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing 'Q' and 'RMSD' columns
    dimension : int
        Dimension of the output matrix (dimension x dimension)
    maximal_RMSD : float
        Maximum value for RMSD axis
    maximal_Q : float
        Maximum value for Q axis
    metrics_names : list, optional
        List of two metric names [x_axis_name, y_axis_name] for labeling (axis reversal handled in plotting)
    
    Returns:
    --------
    matrix_probability : numpy.ndarray
        Probability matrix
    matrix_txt : list
        Matrix containing frame information for each bin
    bin_size_Q : float
        Bin size for Q axis
    bin_size_RMSD : float
        Bin size for RMSD axis
    """
    # Calculate bin sizes
    bin_size_Q = maximal_Q / dimension
    bin_size_RMSD = maximal_RMSD / dimension
    
    # Calculate normalized indices (no data reversal, just normal binning)
    q_normalized = np.minimum((df['Q'] / bin_size_Q).astype(int), dimension - 1)
    rmsd_normalized = np.minimum((df['RMSD'] / bin_size_RMSD).astype(int), dimension - 1)
    
    # Create matrix_counts using numpy's histogram2d
    matrix_counts, _, _ = np.histogram2d(
        q_normalized, 
        rmsd_normalized,
        bins=[dimension, dimension],
        range=[[0, dimension], [0, dimension]]
    )
    
    # Create matrix_txt efficiently using list comprehension
    matrix_txt = [[[] for _ in range(dimension)] for _ in range(dimension)]
    for i, (q, r, t, f) in enumerate(zip(q_normalized, rmsd_normalized, df['traj'], df['frame'])):
        matrix_txt[q][r].append([t, f])
    
    # Calculate probability matrix
    matrix_probability = matrix_counts / len(df)
    
    return matrix_probability, matrix_txt, bin_size_Q, bin_size_RMSD

def probability_plot(matrix_probability, bin_size_Q, bin_size_RMSD, maximal_RMSD, maximal_Q):
	#initialize...
	fig, ax= plt.subplots(figsize=(10, 10))
	#plan graph...
	ax.set_aspect('equal')
	ax.set_xlim([0, maximal_Q])
	ax.set_ylim([0, maximal_RMSD])
	xlocs, xticks=plt.xticks(np.arange(0, 1.1, 0.1), rotation=90)
	xticks[0].set_visible(False) #removes tick at 0 so the plot is less cluttered
	ylocs, yticks=plt.yticks(np.arange(0, maximal_RMSD, 1))
	yticks[0].set_visible(False)
	plt.tick_params (axis = "both", which = "both", bottom = False, top = False, left=False, right=False)
	plt.xlabel('Q', fontsize=14)
	plt.ylabel('RMSD', fontsize=14)
	#generate the plot
	p=plt.imshow(matrix_probability.T, origin = "lower", interpolation = "gaussian", extent=[0,1,0,maximal_RMSD], aspect=float(bin_size_Q/bin_size_RMSD), cmap='Blues') #set aspect to ratio of x unity/y unity to get square plot
	cbar=plt.colorbar(p, ax=ax, ticks=np.arange(0,np.amax(matrix_probability), 0.02), shrink=0.805)
	cbar.set_label('Probability', fontsize=14, rotation=90)
#	plt.show()

def energy_plot_3d(matrix_energy_rescaled, bin_size_Q, bin_size_RMSD, maximal_RMSD, maximal_Q, real_values, squares, names_axis, x_min=None, x_max=None, y_min=None, y_max=None):
    """
    Creates a 3D surface plot representation of energy landscape with annotated regions.

    Parameters:
    -----------
    matrix_energy_rescaled : numpy.ndarray
        The energy matrix with rescaled values
    bin_size_Q : float
        Bin size for Q axis (Frobenius distance)
    bin_size_RMSD : float
        Bin size for RMSD axis
    maximal_RMSD : float
        Maximum value for RMSD
    maximal_Q : float
        Maximum value for Q
    real_values : numpy.ndarray
        Original energy values for color scale reference
    squares : list of tuples
        List of (x0, x1, y0, y1) coordinates defining regions to highlight
    names_axis : list
        List of axis names [x_axis_name, y_axis_name]
    x_min : float, optional
        Minimum value for x-axis (default: None, uses 0)
    x_max : float, optional
        Maximum value for x-axis (default: None, uses maximal_Q)
    y_min : float, optional
        Minimum value for y-axis (default: None, uses 0)
    y_max : float, optional
        Maximum value for y-axis (default: None, uses maximal_RMSD)

    Returns:
    --------
    plotly.graph_objects.Figure
        A 3D surface plot figure of the energy landscape
    """
    # Use provided limits or default to full range
    x_min = x_min if x_min is not None else 0
    x_max = x_max if x_max is not None else maximal_Q
    y_min = y_min if y_min is not None else 0
    y_max = y_max if y_max is not None else maximal_RMSD

    # Calculate the bin centers from edges for plotting
    x_centers = np.linspace(0, maximal_Q, matrix_energy_rescaled.shape[0])
    y_centers = np.linspace(0, maximal_RMSD, matrix_energy_rescaled.shape[1])

    # Create a meshgrid for the x and y bin centers
    X, Y = np.meshgrid(x_centers, y_centers)

    # Check if we need to reverse any axes for Q-values
    # reverse_x = "Q-value" in names_axis[0] or "Fraction of Contact Formed" in names_axis[0]
    # reverse_y = "Q-value" in names_axis[1] or "Fraction of Contact Formed" in names_axis[1]
    reverse_x = "RMSDirfjirf" in names_axis[0]
    reverse_y = "RMSDjfkrijf" in names_axis[1]
     # Create the surface plot
    fig = go.Figure(data=[go.Surface(
        z=matrix_energy_rescaled.T,
        x=X,
        y=Y,
        colorscale='RdYlBu_r',
        cmin=0,
        cmax=np.amax(real_values),
        connectgaps=True
    )])

    # Add rectangles for the squares (as 3D boxes)
    colours = dict(zip(squares, px.colors.qualitative.T10[:len(squares)]))
    labels = dict(zip(squares, [ascii_uppercase[i] for i in range(len(squares))]))

    for square in squares:
        x0, x1, y0, y1 = square
        fig.add_trace(go.Scatter3d(
            x=[x0, x1, x1, x0, x0],
            y=[y0, y0, y1, y1, y0],
            z=[np.amax(real_values)]*5,
            mode='lines',
            line=dict(color=colours[square], width=10),
            name=labels[square]
        ))

    # Determine if custom ranges are set
    custom_x_range = x_min is not None or x_max is not None
    custom_y_range = y_min is not None or y_max is not None

    # Set autorange: False if custom range, 'reversed' if Q-value and no custom range, True otherwise
    x_autorange = False if custom_x_range else ('reversed' if reverse_x else True)
    y_autorange = False if custom_y_range else ('reversed' if reverse_y else True)

    # Update layout with potential axis reversal
    fig.update_layout(
        title='Conformational Landscape (3D)',
        scene=dict(
            xaxis_title=f"{names_axis[0]}",
            yaxis_title=f"{names_axis[1]}",
            zaxis_title='-ln(p)  (Frequency)',
            xaxis=dict(
                autorange=x_autorange,
                range=[x_max, x_min] if reverse_x else [x_min, x_max]
            ),
            yaxis=dict(
                autorange=y_autorange,
                range=[y_max, y_min] if reverse_y else [y_min, y_max]
            ),
            zaxis=dict(range=[0, np.amax(real_values)])
        ),
        width=800,
        height=800,
    )
    return fig


def make_matrix_energy(matrix_probability, maximal, dimension):
	#convert probability into energy
	matrix_energy=np.where(matrix_probability>0, -np.log(matrix_probability), 100) #np.where includes a condition to use first vs second value to build new array in corresponding position
	#rescale so that min value is 0
	matrix_energy_rescaled=np.where(matrix_energy!=100, matrix_energy-np.amin(matrix_energy), 100)
	real_values=np.where(matrix_energy_rescaled!=100, matrix_energy_rescaled, -100)
	matrix_energy_rescaled=np.where(matrix_energy!=100, matrix_energy_rescaled, np.amax(real_values)+1)
	return(matrix_energy_rescaled, real_values)

def energy_plot(matrix_energy_rescaled, bin_size_Q, bin_size_RMSD, maximal_RMSD, real_values, squares, path_landscape):
	#now let's make the figure:
	#initialize...
	colors = ["#ff0000", "#0000ff"]
	custom_cmap = LinearSegmentedColormap.from_list("custom_cmap", colors)
	fig, ax= plt.subplots(figsize=(10, 10))
	ax.set_title('-ln(p) (frequency)')
	colours = dict(zip(squares, plt.cm.tab10.colors[:len(squares)]))
	from string import ascii_uppercase
	labels=dict(zip(squares,[ascii_uppercase[i] for i in range(len(squares))]))
	#plan graph...
	#ax.set_aspect('equal')
	ax.set_aspect(0.25)
	ax.set_xlim([0, 1])
	ax.set_ylim([0, 4])
	#ax.autoscale(enable=True, axis='y',tight=True)
	xlocs, xticks=plt.xticks(np.arange(0, 1.1, 0.1), rotation=90)
	xticks[0].set_visible(False)
	ylocs, yticks=plt.yticks(np.arange(0, 4, 0.1))
	yticks[0].set_visible(False)
	plt.tick_params (axis = "both", which = "both", bottom = True, top = False, left=True, right=False)
	plt.xlabel('Q (Froebenius distance)', fontsize=14)
	plt.ylabel('RMSD (nm)', fontsize=14)
	#generate the plot
	#cmap = plt.cm.get_cmap('RdYlBu') #grab standard colormap
	cmap=custom_cmap.reversed() #inverts color range to get blue for lower energy as convention!!
	cmap.set_over('white') #needed to set all values above threshold=unreal values to white
	p=plt.imshow(matrix_energy_rescaled.T, origin='lower', extent=[0,1,0.01,maximal_RMSD], interpolation='gaussian', aspect=0.25, cmap=cmap, vmax=np.amax(real_values)) #set aspect to ratio of x unity/y unity to get square plot
	#cbar=plt.colorbar(p, ax=ax, ticks=np.arange(np.amin(matrix_energy_rescaled.T), np.amax(real_values), 1.0), shrink=0.82)
	cbar=plt.colorbar(p, ax=ax, ticks=np.arange(1, np.amax(real_values), 1.0), shrink=0.805)
	plt.clim(2.5, np.amax(real_values)) #sets range for colorbar
	cbar.set_label('-ln(p)', fontsize=14, rotation=90)
	#optional grid to help select ranges of Q and RMSD:
	plt.grid()
	#optional to show regin where you extracted intermediates:
	for tupl in squares:
		from matplotlib.patches import Rectangle
		ax.add_patch(Rectangle((tupl[0], tupl[2]), float(tupl[1]-tupl[0]), float(tupl[3]-tupl[2]), edgecolor=colours[tupl], facecolor="None" ,label=labels[tupl], linewidth=1.5 ))
	ax.legend(loc='upper right', framealpha=1, edgecolor='white')
	#plt.show()
	plt.savefig(path_landscape)

def energy_plot(matrix_energy_rescaled, bin_size_Q, bin_size_RMSD, maximal_RMSD, real_values, squares, path_landscape, names_axis):
	#now let's make the figure:
	#initialize...
	colors = ["#ff0000", "#0000ff"]
	custom_cmap = LinearSegmentedColormap.from_list("custom_cmap", colors)
	fig, ax= plt.subplots(figsize=(10, 10))
	ax.set_title('-ln(p) (frequency)')
	colours = dict(zip(squares, plt.cm.tab10.colors[:len(squares)]))
	from string import ascii_uppercase
	labels=dict(zip(squares,[ascii_uppercase[i] for i in range(len(squares))]))
	#plan graph...
	#ax.set_aspect('equal')
	ax.set_aspect(0.25)
	ax.set_xlim([0, 1])
	ax.set_ylim([0, 4])
	#ax.autoscale(enable=True, axis='y',tight=True)
	xlocs, xticks=plt.xticks(np.arange(0, 1.1, 0.1), rotation=90)
	xticks[0].set_visible(False)
	ylocs, yticks=plt.yticks(np.arange(0, 4, 0.1))
	yticks[0].set_visible(False)
	plt.tick_params (axis = "both", which = "both", bottom = True, top = False, left=True, right=False)
	plt.xlabel(f'{names_axis[0]}', fontsize=14)
	plt.ylabel(f'{names_axis[1]}', fontsize=14)
	#generate the plot
	#cmap = plt.cm.get_cmap('RdYlBu') #grab standard colormap
	cmap=custom_cmap.reversed() #inverts color range to get blue for lower energy as convention!!
	cmap.set_over('white') #needed to set all values above threshold=unreal values to white
	p=plt.imshow(matrix_energy_rescaled.T, origin='lower', extent=[0,1,0.01,maximal_RMSD], interpolation='gaussian', aspect=0.25, cmap=cmap, vmax=np.amax(real_values)) #set aspect to ratio of x unity/y unity to get square plot
	#cbar=plt.colorbar(p, ax=ax, ticks=np.arange(np.amin(matrix_energy_rescaled.T), np.amax(real_values), 1.0), shrink=0.82)
	cbar=plt.colorbar(p, ax=ax, ticks=np.arange(1, np.amax(real_values), 1.0), shrink=0.805)
	plt.clim(2.5, np.amax(real_values)) #sets range for colorbar
	cbar.set_label('-ln(p)', fontsize=14, rotation=90)
	#optional grid to help select ranges of Q and RMSD:
	plt.grid()
	#optional to show regin where you extracted intermediates:
	for tupl in squares:
		from matplotlib.patches import Rectangle
		ax.add_patch(Rectangle((tupl[0], tupl[2]), float(tupl[1]-tupl[0]), float(tupl[3]-tupl[2]), edgecolor=colours[tupl], facecolor="None" ,label=labels[tupl], linewidth=1.5 ))
	ax.legend(loc='upper right', framealpha=1, edgecolor='white')
	#plt.show()
	plt.savefig(path_landscape)

def energy_plot_2d(matrix_energy_rescaled, bin_size_Q, bin_size_RMSD, maximal_RMSD, maximal_Q, real_values, squares, names_axis, x_min=None, x_max=None, y_min=None, y_max=None):
    """
    Creates a 2D heatmap representation of energy landscape with annotated regions.

    Parameters:
    -----------
    matrix_energy_rescaled : numpy.ndarray
        The energy matrix with rescaled values
    bin_size_Q : float
        Bin size for Q axis (Frobenius distance)
    bin_size_RMSD : float
        Bin size for RMSD axis
    maximal_RMSD : float
        Maximum value for RMSD
    maximal_Q : float
        Maximum value for Q
    real_values : numpy.ndarray
        Original energy values for color scale reference
    squares : list of tuples
        List of (x0, x1, y0, y1) coordinates defining regions to highlight
    names_axis : list
        List of axis names [x_axis_name, y_axis_name]
    x_min : float, optional
        Minimum value for x-axis (default: None, uses 0)
    x_max : float, optional
        Maximum value for x-axis (default: None, uses maximal_Q)
    y_min : float, optional
        Minimum value for y-axis (default: None, uses 0)
    y_max : float, optional
        Maximum value for y-axis (default: None, uses maximal_RMSD)

    Returns:
    --------
    plotly.graph_objects.Figure
        A 2D heatmap figure of the energy landscape
    """
    import numpy as np
    import plotly.graph_objects as go
    import plotly.express as px
    from string import ascii_uppercase

    # Use provided limits or default to full range
    x_min = x_min if x_min is not None else 0
    x_max = x_max if x_max is not None else maximal_Q
    y_min = y_min if y_min is not None else 0
    y_max = y_max if y_max is not None else maximal_RMSD

    # Calculate the bin centers from edges for plotting
    x_centers = np.linspace(0, maximal_Q, matrix_energy_rescaled.shape[0])
    y_centers = np.linspace(0, maximal_RMSD, matrix_energy_rescaled.shape[1])
    
    # Check if we need to reverse any axes for Q-values
    # reverse_x = "Q-value" in names_axis[0] or "Fraction of Contact Formed" in names_axis[0]
    # reverse_y = "Q-value" in names_axis[1] or "Fraction of Contact Formed" in names_axis[1]
    
    reverse_x = "RMSDrijfirj" in names_axis[0]
    reverse_y = "RMSDkrikfri" in names_axis[1]
    # Create the heatmap
    fig = go.Figure()
    
    # Add the heatmap
    fig.add_trace(go.Heatmap(
        z=matrix_energy_rescaled.T,
        x=x_centers,
        y=y_centers,
        colorscale='RdYlBu_r',
        zmin=0,
        zmax=np.amax(real_values),
        connectgaps=True,
        colorbar=dict(
            title='-ln(p)',
            thickness=20,
        )
    ))
    
    # Determine if custom ranges are set
    custom_x_range = x_min is not None or x_max is not None
    custom_y_range = y_min is not None or y_max is not None

    # Set autorange: False if custom range, 'reversed' if RMSD and no custom range, True otherwise
    x_autorange = False if custom_x_range else ('reversed' if reverse_x else True)
    y_autorange = False if custom_y_range else ('reversed' if reverse_y else True)

    # Update layout with a cleaner modern theme and potential axis reversal
    fig.update_layout(
        title=dict(
            text='Conformational Landscape',
            font=dict(size=24, family="Arial", color="#333333"),
            x=0.5,
            xanchor='center'
        ),
        xaxis=dict(
            title=f'{names_axis[0]}',
            tickfont=dict(size=12),
            showgrid=False,
            gridcolor='rgba(211, 211, 211, 0.4)',
            zeroline=False,
            autorange=x_autorange,
        ),
        yaxis=dict(
            title=f'{names_axis[1]}',
            tickfont=dict(size=12),
            showgrid=False,
            gridcolor='rgba(211, 211, 211, 0.4)',
            zeroline=False,
            autorange=y_autorange,
        ),
        xaxis_range=[x_max, x_min] if reverse_x else [x_min, x_max],
        yaxis_range=[y_max, y_min] if reverse_y else [y_min, y_max],
        width=900,
        height=700,
        plot_bgcolor='#a50026',  # Dark blue matching the 0 value in RdYlBu_r colorscale
        paper_bgcolor='white',
        margin=dict(l=80, r=80, t=100, b=80),
        hoverlabel=dict(
            bgcolor="white",
            font_size=12,
            font_family="Arial"
        ),
    )
    
    # Add contour lines for better interpretation of the energy landscape
    fig.add_trace(go.Contour(
        z=matrix_energy_rescaled.T,
        x=x_centers,
        y=y_centers,
        contours=dict(
            coloring='none',
            showlabels=True,
            labelfont=dict(size=10, color='black')
        ),
        line=dict(width=1.5, color='rgba(0,0,0,0.5)'),
        showscale=False,
        hoverinfo='skip'
    ))
    
    # Interactive hover template
    #fig.update_traces(
    #    hovertemplate=f"<b>{names_axis[0]}':</b> %{x:.3f}<br><b>{names_axis[1]}:</b> %{y:.3f} nm<br><b>Energy:</b> %{z:.3f}<extra></extra>",
    #    selector=dict(type='heatmap')
    #)
    
    return fig
#inputs:
size=65
#selected_regions=[(0.06,0.17,0.95,1.05),(0.15,0.22,0.78,0.92),(0.2,0.27,0.67,0.77),(0.25,0.33,0.49,0.63)] #list of tuples with minQ, maxQ, minRMSD, maxRMSD FROM CLOSEST TO NATIVE TO UNFOLDED!!!
selected_regions=[] #list of tuples with minQ, maxQ, minRMSD, maxRMSD FROM CLOSEST TO NATIVE TO UNFOLDED!!!
#selected_regions=[] #list of tuples with minQ, maxQ, minRMSD, maxRMSD FROM CLOSEST TO NATIVE TO UNFOLDED!!!
#main:
#dataframe, max_RMSD=loaddata('concatenated_Q_RMSD')
#dataframe, max_RMSD=loaddata('concat_Q_RMSD_rescaled')
#probability_matrix, allframes_matrix, Qbin, RMSDbin=make_matrix_probability(dataframe, size, max_RMSD)
#probability_plot(probability_matrix, Qbin, RMSDbin, max_RMSD)
#energy_matrix, real_values=make_matrix_energy(probability_matrix, max_RMSD, size)
#energy_plot(energy_matrix, Qbin, RMSDbin, max_RMSD, real_values, selected_regions)
