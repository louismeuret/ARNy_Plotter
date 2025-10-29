"""
Clean Celery Tasks for RNA Analysis
Two-phase approach: Metrics computation then Plot generation
"""

import os
import time
import pickle
import logging
from celery import Celery
from functools import wraps
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from src.plotting.create_plots import *
from plotly.io import to_json
import orjson

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Register orjson serializer with Celery
from kombu.serialization import register

def orjson_dumps(obj):
    return orjson.dumps(obj)

def orjson_loads(s):
    return orjson.loads(s)

register('orjson', orjson_dumps, orjson_loads,
         content_type='application/x-orjson',
         content_encoding='utf-8')

# Celery app configuration
app = Celery('rna_tasks')
app.conf.update(
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0',
    task_serializer='orjson',
    result_serializer='orjson',
    accept_content=['orjson', 'json'],  # Accept both orjson and json for compatibility
    task_acks_late=False,
    worker_prefetch_multiplier=1,
    result_expires=3600,
)

# Check dependencies
try:
    import barnaba as bb
    BARNABA_AVAILABLE = True
except ImportError:
    BARNABA_AVAILABLE = False
    logger.warning("Barnaba not available")

def plotly_to_json(fig):
    return to_json(fig, validate=False, engine="orjson")

def load_shared_trajectory_data(session_id):
    """Load cached trajectory data if available, otherwise load fresh"""
    import MDAnalysis as mda
    
    # Try to load cached data first
    session_dir = os.path.join("static", "uploads", session_id)
    cache_path = os.path.join(session_dir, "trajectory_cache.pkl")
    
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'rb') as f:
                cached_data = pickle.load(f)
            logger.info("Using cached trajectory data for efficient loading")
            return cached_data
        except Exception as e:
            logger.warning(f"Failed to load cached trajectory data: {e}")
    
    return None

def get_universe_from_cache_or_load(session_id, topology_file=None, trajectory_file=None):
    """Get Universe object using cached data or load fresh if needed"""
    import MDAnalysis as mda
    
    cached_data = load_shared_trajectory_data(session_id)
    
    if cached_data:
        # Use paths from cached data
        topology_path = cached_data['topology_path']
        trajectory_path = cached_data['trajectory_path']
        logger.info(f"Loading Universe from cached paths: {os.path.basename(topology_path)}, {os.path.basename(trajectory_path)}")
        return mda.Universe(topology_path, trajectory_path), cached_data
    else:
        # Fallback to provided paths
        if topology_file and trajectory_file:
            logger.info(f"Loading Universe from provided paths (no cache): {os.path.basename(topology_file)}, {os.path.basename(trajectory_file)}")
            return mda.Universe(topology_file, trajectory_file), None
        else:
            raise ValueError("No cached data available and no file paths provided")
    
def log_task(func):
    """Simple task logging decorator"""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        task_name = func.__name__
        start_time = time.time()
        
        try:
            logger.info(f"Starting {task_name}")
            result = func(self, *args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"Completed {task_name} in {duration:.2f}s")
            return result
        except Exception as exc:
            duration = time.time() - start_time
            logger.error(f"Failed {task_name} after {duration:.2f}s: {exc}")
            raise
    return wrapper

# Phase 1: Metric Computation Tasks
@app.task(bind=True, max_retries=3)
@log_task
def compute_rmsd(self, *args):
    """Compute RMSD metric"""
    # Handle chain arguments
    if len(args) == 4:  # previous_result, topology_file, trajectory_file, session_id
        _, topology_file, trajectory_file, session_id = args
    else:  # topology_file, trajectory_file, session_id
        topology_file, trajectory_file, session_id = args
        
    if not BARNABA_AVAILABLE:
        raise ImportError("Barnaba not available")
    
    import barnaba as bb
    rmsd_result = bb.rmsd(topology_file, trajectory_file, topology=topology_file, heavy_atom=True)
    
    # Save to session directory
    session_dir = os.path.join("static", "uploads", session_id)
    os.makedirs(session_dir, exist_ok=True)
    
    rmsd_path = os.path.join(session_dir, "rmsd_data.pkl")
    with open(rmsd_path, 'wb') as f:
        pickle.dump(rmsd_result, f)
    
    logger.info(f"RMSD computed and saved to {rmsd_path}")
    return {"metric": "rmsd", "status": "success", "path": rmsd_path}

@app.task(bind=True, max_retries=3)
@log_task
def compute_ermsd(self, *args):
    """Compute eRMSD metric"""
    # Handle chain arguments
    if len(args) == 4:  # previous_result, topology_file, trajectory_file, session_id
        _, topology_file, trajectory_file, session_id = args
    else:  # topology_file, trajectory_file, session_id
        topology_file, trajectory_file, session_id = args
        
    if not BARNABA_AVAILABLE:
        raise ImportError("Barnaba not available")
    
    import barnaba as bb
    ermsd_result = bb.ermsd(topology_file, trajectory_file, topology=topology_file)
    
    # Save to session directory
    session_dir = os.path.join("static", "uploads", session_id)
    os.makedirs(session_dir, exist_ok=True)
    
    ermsd_path = os.path.join(session_dir, "ermsd_data.pkl")
    with open(ermsd_path, 'wb') as f:
        pickle.dump(ermsd_result, f)
    
    logger.info(f"eRMSD computed and saved to {ermsd_path}")
    return {"metric": "ermsd", "status": "success", "path": ermsd_path}

@app.task(bind=True, max_retries=3)
@log_task
def compute_annotate(self, *args):
    """Compute annotate metric"""
    # Handle chain arguments
    if len(args) == 4:  # previous_result, topology_file, trajectory_file, session_id
        _, topology_file, trajectory_file, session_id = args
    else:  # topology_file, trajectory_file, session_id
        topology_file, trajectory_file, session_id = args
        
    if not BARNABA_AVAILABLE:
        raise ImportError("Barnaba not available")
    
    import barnaba as bb
    annotate_result = bb.annotate(trajectory_file, topology=topology_file)
    
    # Save to session directory
    session_dir = os.path.join("static", "uploads", session_id)
    os.makedirs(session_dir, exist_ok=True)
    
    annotate_path = os.path.join(session_dir, "annotate_data.pkl")
    with open(annotate_path, 'wb') as f:
        pickle.dump(annotate_result, f)
    
    logger.info(f"Annotate computed and saved to {annotate_path}")
    return {"metric": "annotate", "status": "success", "path": annotate_path}

# Phase 2: Plot Generation Tasks
@app.task(bind=True, max_retries=3)
@log_task
def generate_rmsd_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id):
    """Generate RMSD plot using pre-computed data"""
    try:
        # Load pre-computed RMSD data
        rmsd_path = os.path.join("static", "uploads", session_id, "rmsd_data.pkl")
        if os.path.exists(rmsd_path):
            with open(rmsd_path, 'rb') as f:
                rmsd = pickle.load(f)
            print(f"LOADED RMSD FROM SAVED DATA")
        else:
            # Fallback: compute if not available using cached trajectory data
            logger.info("Computing RMSD using cached trajectory data")
            cached_data = load_shared_trajectory_data(session_id)
            
            if cached_data:
                # Use cached paths
                topology_file = cached_data['topology_path']
                trajectory_file = cached_data['trajectory_path']
                logger.info(f"Using cached trajectory paths for RMSD computation")
            
            if not BARNABA_AVAILABLE:
                raise ImportError("Barnaba not available and no pre-computed data")
            import barnaba as bb
            rmsd = bb.rmsd(topology_file, trajectory_file, topology=topology_file, heavy_atom=True)

        # Create plot using create_plots functions
        #try:
        logging.info("Default plots are used")
        fig = plot_rmsd(rmsd)
        print(fig)
    
        # Save data and plot
        import pandas as pd
        rmsd_df = pd.DataFrame({"RMSD": rmsd})
        rmsd_df.to_csv(os.path.join(files_path, "rmsd_values.csv"), index=False)
        fig.write_html(os.path.join(plot_dir, "rmsd_plot.html"))

        # Convert to JSON
        plotly_data = plotly_to_json(fig)
        return plotly_data
        """
        except ImportError:
            # Fallback to simple matplotlib plot
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.plot(rmsd)
            ax.set_xlabel('Frame')
            ax.set_ylabel('RMSD (Å)')
            ax.set_title('RMSD Analysis')
            
            plot_path = os.path.join(plot_dir, "rmsd_plot.png")
            os.makedirs(plot_dir, exist_ok=True)
            plt.savefig(plot_path, dpi=150, bbox_inches='tight')
            plt.close()
            
            return {"path": plot_path, "status": "success"}
        """
        
    except Exception as e:
        logger.error(f"RMSD plot generation failed: {e}")
        raise

@app.task(bind=True, max_retries=3)
@log_task
def generate_ermsd_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id):
    """Generate eRMSD plot using pre-computed data"""
    try:
        # Load pre-computed eRMSD data
        ermsd_path = os.path.join("static", "uploads", session_id, "ermsd_data.pkl")
        if os.path.exists(ermsd_path):
            with open(ermsd_path, 'rb') as f:
                ermsd = pickle.load(f)
            print(f"LOADED eRMSD FROM SAVED DATA")
        else:
            # Fallback: compute if not available
            print(f"USED FALLBACK")
            if not BARNABA_AVAILABLE:
                raise ImportError("Barnaba not available and no pre-computed data")
            import barnaba as bb
            ermsd = bb.ermsd(topology_file, trajectory_file, topology=topology_file)

        # Create plot using create_plots functions
        try:
            fig = plot_ermsd(ermsd)
            
            # Save data and plot
            import pandas as pd
            ermsd_df = pd.DataFrame({"ERMSD": ermsd})
            ermsd_df.to_csv(os.path.join(files_path, "ermsd_values.csv"), index=False)
            fig.write_html(os.path.join(plot_dir, "ermsd_plot.html"))

            # Convert to JSON
            plotly_data = plotly_to_json(fig)
            return plotly_data
            
        except ImportError:
            # Fallback to simple matplotlib plot
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.plot(ermsd)
            ax.set_xlabel('Frame')
            ax.set_ylabel('eRMSD')
            ax.set_title('eRMSD Analysis')
            
            plot_path = os.path.join(plot_dir, "ermsd_plot.png")
            os.makedirs(plot_dir, exist_ok=True)
            plt.savefig(plot_path, dpi=150, bbox_inches='tight')
            plt.close()
            
            return {"path": plot_path, "status": "success"}
        
    except Exception as e:
        logger.error(f"eRMSD plot generation failed: {e}")
        raise

# Placeholder tasks for other plots that don't need metrics
@app.task(bind=True, max_retries=3)
@log_task  
def generate_torsion_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, torsion_residue=0):
    """Generate torsion plot"""
    try:
        if not BARNABA_AVAILABLE:
            raise ImportError("Barnaba not available")
            
        import barnaba as bb
        angles, res = bb.backbone_angles(trajectory_file, topology=topology_file)
        logger.info(f"Calculated torsion angles for {len(res)} residues")
        
        try:
            # Ensure current directory is in path for imports
            import sys
            import os
            current_dir = os.path.dirname(os.path.abspath(__file__))
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)
            
            from src.plotting.create_plots import plot_torsion, plot_torsion_enhanced
            logger.info(f"Torsion plot import successful")
            
            # Handle backward compatibility
            if isinstance(torsion_residue, dict):
                logger.info(f"Using enhanced torsion plot with params: {torsion_residue}")
                fig = plot_torsion_enhanced(angles, res, torsion_residue)
                # Enhanced plot returns a single figure
                plotly_data = plotly_to_json(fig)
                return plotly_data
            else:
                logger.info(f"Using dual torsion plots for residue: {torsion_residue}")
                figures = plot_torsion(angles, res, torsion_residue)
                
                if isinstance(figures, list) and len(figures) == 2:
                    time_series_fig, distribution_fig = figures
                    logger.info(f"Generated time series and distribution plots")
                    
                    # Ensure plot directory exists
                    os.makedirs(plot_dir, exist_ok=True)
                    
                    # Save both plots
                    time_series_path = os.path.join(plot_dir, "torsion_time_series.html")
                    distribution_path = os.path.join(plot_dir, "torsion_distribution.html")
                    
                    time_series_fig.write_html(time_series_path)
                    distribution_fig.write_html(distribution_path)
                    
                    logger.info(f"Torsion plots saved to: {time_series_path}, {distribution_path}")
                    
                    # Convert both to JSON
                    time_series_data = plotly_to_json(time_series_fig)
                    distribution_data = plotly_to_json(distribution_fig)
                    
                    # Save data
                    import pandas as pd
                    angles_df = pd.DataFrame(angles.reshape(-1, angles.shape[-1]), 
                                           columns=["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "chi"])
                    csv_path = os.path.join(files_path, "torsion_angles.csv")
                    angles_df.to_csv(csv_path, index=False)
                    logger.info(f"Torsion data saved to: {csv_path}")
                    
                    logger.info(f"Torsion plots conversion to JSON successful")
                    return [time_series_data, distribution_data]
                else:
                    # Fallback for single figure
                    fig = figures if not isinstance(figures, list) else figures[0]
                    os.makedirs(plot_dir, exist_ok=True)
                    html_path = os.path.join(plot_dir, "torsion_plot.html")
                    fig.write_html(html_path)
                    plotly_data = plotly_to_json(fig)
                    return plotly_data
            
        except ImportError as e:
            logger.error(f"Torsion plot import failed: {e}")
            return {"path": f"static/uploads/{session_id}/torsion_plot.png", "status": "fallback"}
        except Exception as plot_error:
            logger.error(f"Torsion plot generation failed: {plot_error}")
            return {"path": f"static/uploads/{session_id}/torsion_plot.png", "status": "fallback"}
            
    except Exception as exc:
        logger.error(f"Torsion calculation failed: {str(exc)}")
        raise exc

@app.task(bind=True, max_retries=3)
@log_task
def generate_sec_structure_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id):
    """Generate secondary structure plot"""
    try:
        if not BARNABA_AVAILABLE:
            raise ImportError("Barnaba not available")
            
        import barnaba as bb
        stackings, pairings, res = bb.annotate(trajectory_file, topology=topology_file)
        dotbracket_data, res2 = bb.dot_bracket(pairings, res)
        return [dotbracket_data, res2.strip()]
    except Exception as exc:
        logger.error(f"Secondary structure calculation failed: {str(exc)}")
        raise exc

@app.task(bind=True, max_retries=3)
@log_task
def generate_dotbracket_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id):
    """Generate dot-bracket plot"""
    try:
        # Load pre-computed annotate data if available
        annotate_path = os.path.join("static", "uploads", session_id, "annotate_data.pkl")
        if os.path.exists(annotate_path):
            with open(annotate_path, 'rb') as f:
                stackings, pairings, res = pickle.load(f)
            print(f"LOADED ANNOTATE FROM SAVED DATA FOR DOTBRACKET")
        else:
            # Fallback: compute if not available
            if not BARNABA_AVAILABLE:
                raise ImportError("Barnaba not available")
            import barnaba as bb
            stackings, pairings, res = bb.annotate(trajectory_file, topology=topology_file)
        
        dotbracket_data = bb.dot_bracket(pairings, res)[0]
        
        try:
            from src.plotting.create_plots import plot_dotbracket
            fig = plot_dotbracket(dotbracket_data)
            fig.write_html(os.path.join(plot_dir, "dotbracket_timeline_plot.html"))
            plotly_data = plotly_to_json(fig)
            return plotly_data
        except ImportError:
            return {"path": f"static/uploads/{session_id}/dotbracket_plot.png", "status": "fallback"}
            
    except Exception as exc:
        logger.error(f"Dotbracket calculation failed: {str(exc)}")
        raise exc

@app.task(bind=True, max_retries=3)
@log_task
def generate_arc_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id):
    """Generate arc plot"""
    try:
        print("ARC")
        # Load pre-computed annotate data if available
        annotate_path = os.path.join("static", "uploads", session_id, "annotate_data.pkl")
        if os.path.exists(annotate_path):
            with open(annotate_path, 'rb') as f:
                stackings, pairings, res = pickle.load(f)
            print(f"LOADED ANNOTATE FROM SAVED DATA FOR ARC")
        else:
            # Fallback: compute if not available
            if not BARNABA_AVAILABLE:
                raise ImportError("Barnaba not available")
            import barnaba as bb
            stackings, pairings, res = bb.annotate(trajectory_file, topology=topology_file)
        
        dotbracket_data = bb.dot_bracket(pairings, res)[0]
        # Dotbracket for the native state:
        stackings_native, pairings_native, res_native = bb.annotate(topology_file)
        dotbracket_native = bb.dot_bracket(pairings_native, res_native)[0]
        print(f"DOTBRACKET NATIVE = {str(dotbracket_native[0])}")

        sequence = ''.join([item[0] for item in res])
        resids = [item.split("_")[1] for item in res]
        print(f"ARC SEQUENCE = {str(sequence)}")
        
        try:
            import pandas as pd
            dotbracket_df = pd.DataFrame(dotbracket_data, columns=["DotBracket"])
            dotbracket_df.to_csv(os.path.join(files_path, "dotbracket_data.csv"), index=False)
            
            from src.plotting.create_plots import plot_diagram_frequency
            fig = plot_diagram_frequency(sequence, dotbracket_data, dotbracket_native)
            fig.write_html(os.path.join(plot_dir, "arc_diagram_plot.html"))
            plotly_data = plotly_to_json(fig)
            return [plotly_data, resids]
        except ImportError:
            return {"path": f"static/uploads/{session_id}/arc_plot.png", "status": "fallback"}
            
    except Exception as exc:
        logger.error(f"Arc plot calculation failed: {str(exc)}")
        raise exc

@app.task(bind=True, max_retries=3)
@log_task
def generate_contact_map_plot(self, topology_file, trajectory_file, files_path, plot_dir, generate_data_path, session_id):
    """Generate contact map plot"""
    def process_barnaba_pairings(pairings, res):
        """Process barnaba pairings and residue information into frames dictionary"""
        import pandas as pd
        frames_dict = {}
        # Create sequence from residue information
        sequence = [r[0] for r in res]  # Assuming res contains nucleotide types

        # Process each frame
        for frame_num, frame_data in enumerate(pairings):
            base_pairs = []

            # Each frame contains a list of pairs and their annotations
            if len(frame_data) == 2:
                pair_indices = frame_data[0]
                annotations = frame_data[1]

                for pair_idx, pair in enumerate(pair_indices):
                    if not pair:
                        continue

                    res_i = pair[0] + 1  # Convert to 1-based indexing
                    res_j = pair[1] + 1

                    # Get residue names from the sequence
                    if 0 <= res_i - 1 < len(sequence) and 0 <= res_j - 1 < len(sequence):
                        res_i_name = f"{sequence[res_i - 1]}{res_i}"
                        res_j_name = f"{sequence[res_j - 1]}{res_j}"
                    else:
                        res_i_name = f"N{res_i}"
                        res_j_name = f"N{res_j}"

                    anno = annotations[pair_idx] if pair_idx < len(annotations) else 'XXX'

                    base_pairs.append({
                        'res_i': res_i,
                        'res_j': res_j,
                        'res_i_name': res_i_name,
                        'res_j_name': res_j_name,
                        'anno': anno
                    })

            # Always assign a DataFrame (even if empty)
            frames_dict[frame_num] = pd.DataFrame(base_pairs)

        return frames_dict, sequence

    try:
        # Load pre-computed annotate data if available
        annotate_path = os.path.join("static", "uploads", session_id, "annotate_data.pkl")
        if os.path.exists(annotate_path):
            with open(annotate_path, 'rb') as f:
                stackings, pairings, res = pickle.load(f)
            print(f"LOADED ANNOTATE FROM SAVED DATA FOR CONTACT MAP")
        else:
            # Fallback: compute if not available
            if not BARNABA_AVAILABLE:
                raise ImportError("Barnaba not available")
            print("COULDN'T LOAD RESULTS FROM ANNOTATE FOR CONTACT_MAP PLOT")
            import barnaba as bb
            stackings, pairings, res = bb.annotate(trajectory_file, topology=topology_file)

        # Process barnaba results into our format
        frames_dict, sequence = process_barnaba_pairings(pairings, res)
        print(len(frames_dict))
        print(f"RNA length: {len(sequence)} nucleotides")
        print(f"Found {len(frames_dict)} frames in the data")
        frame_num, frame_data = sorted(frames_dict.items())[0]

        # Save pairings to CSV
        import pandas as pd
        pairings_df = pd.DataFrame(pairings)
        pairings_df.to_csv(os.path.join(files_path, "pairings.csv"), index=False)

        # Save frames_dict and sequence to pickle
        os.makedirs(generate_data_path, exist_ok=True)
        data_to_save = {
            'frames_dict': frames_dict,
            'sequence': sequence
        }
        with open(os.path.join(generate_data_path, "contact_map_data.pkl"), 'wb') as f:
            pickle.dump(data_to_save, f)

        try:
            from src.plotting.create_plots import plot_rna_contact_map
            fig = plot_rna_contact_map(frame_data, sequence, output_file=os.path.join(plot_dir, "contact_map.html"), frame_number=frame_num)
            plotly_data = plotly_to_json(fig)
            return plotly_data
        except ImportError:
            logger.warning("create_plots module not available, contact map data saved but using fallback display")
            return {"path": f"static/uploads/{session_id}/contact_map_plot.png", "status": "fallback"}
            
    except Exception as exc:
        logger.error(f"Contact map calculation failed: {str(exc)}")
        raise exc

@app.task(bind=True, max_retries=3)
@log_task
def generate_annotate_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id):
    """Generate annotate plot using pre-computed data"""
    try:
        # Load pre-computed annotate data
        annotate_path = os.path.join("static", "uploads", session_id, "annotate_data.pkl")
        if os.path.exists(annotate_path):
            with open(annotate_path, 'rb') as f:
                stackings, pairings, res = pickle.load(f)
            print(f"LOADED ANNOTATE FROM SAVED DATA")
        else:
            # Fallback: compute if not available
            print(f"USED FALLBACK")
            if not BARNABA_AVAILABLE:
                raise ImportError("Barnaba not available and no pre-computed data")
            import barnaba as bb
            stackings, pairings, res = bb.annotate(trajectory_file, topology=topology_file)
        
        return ["ANNOTATE", "annotate", stackings, pairings, res]
        
    except Exception as e:
        logger.error(f"Annotate plot generation failed: {e}")
        raise

@app.task(bind=True, max_retries=3)
@log_task
def generate_ds_motif_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id):
    """Generate double-strand motif plot"""
    return {"path": f"static/uploads/{session_id}/ds_motif_plot.png", "status": "placeholder"}

@app.task(bind=True, max_retries=3)
@log_task
def generate_ss_motif_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id):
    """Generate single-strand motif plot"""
    return {"path": f"static/uploads/{session_id}/ss_motif_plot.png", "status": "placeholder"}

@app.task(bind=True, max_retries=3)
@log_task
def generate_jcoupling_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id):
    """Generate J-coupling plot"""
    try:
        if not BARNABA_AVAILABLE:
            raise ImportError("Barnaba not available")
            
        import barnaba as bb
        couplings, res = bb.jcouplings(trajectory_file, topology=topology_file)
        return ["JCOUPLING", "jcoupling", couplings]
    except Exception as exc:
        logger.error(f"J-coupling calculation failed: {str(exc)}")
        raise exc

@app.task(bind=True, max_retries=3)
@log_task
def generate_escore_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id):
    """Generate e-score plot"""
    return {"path": f"static/uploads/{session_id}/escore_plot.png", "status": "placeholder"}

@app.task(bind=True, max_retries=3)
@log_task
def generate_landscape_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, landscape_params, generate_data_path):
    """Generate landscape plot using pre-computed metrics"""
    try:
        # Load pre-computed RMSD and eRMSD data if available
        rmsd_data = None
        ermsd_data = None
        
        rmsd_path = os.path.join("static", "uploads", session_id, "rmsd_data.pkl")
        if os.path.exists(rmsd_path):
            with open(rmsd_path, 'rb') as f:
                rmsd_data = pickle.load(f)
            print(f"LOADED RMSD FROM SAVED DATA FOR LANDSCAPE")
                
        ermsd_path = os.path.join("static", "uploads", session_id, "ermsd_data.pkl")
        if os.path.exists(ermsd_path):
            with open(ermsd_path, 'rb') as f:
                ermsd_data = pickle.load(f)
            print(f"LOADED eRMSD FROM SAVED DATA FOR LANDSCAPE")
        
        # If data not available, fall back to computation
        if rmsd_data is None or ermsd_data is None:
            logger.warning("Pre-computed metrics not available, computing for landscape plot")
            if not BARNABA_AVAILABLE:
                raise ImportError("Barnaba not available and no pre-computed data")
            
            import barnaba as bb
            if rmsd_data is None:
                rmsd_data = bb.rmsd(topology_file, trajectory_file, topology=topology_file, heavy_atom=True)
            if ermsd_data is None:
                ermsd_data = bb.ermsd(topology_file, trajectory_file, topology=topology_file)
        
        # Extract landscape parameters
        stride = int(landscape_params[0])

        # Map dimension selections to actual metric names
        # Users select: 1 = RMSD, 2 = eRMSD (can be extended for more metrics)
        dimension_map = {
            1: ("RMSD (Å)", rmsd_data),
            2: ("eRMSD (Å)", ermsd_data),
            "1": ("RMSD (Å)", rmsd_data),
            "2": ("eRMSD (Å)", ermsd_data)
        }

        # Get user-selected dimensions (default to RMSD and eRMSD if not specified)
        first_dim = landscape_params[1] if len(landscape_params) > 1 else 1
        second_dim = landscape_params[2] if len(landscape_params) > 2 else 2

        component1_name, component1_data = dimension_map.get(first_dim, ("RMSD (Å)", rmsd_data))
        component2_name, component2_data = dimension_map.get(second_dim, ("eRMSD (Å)", ermsd_data))

        # Apply stride to the data
        component1 = component1_data[::stride]  # X-axis (stored as "Q" in DataFrame for legacy reasons)
        component2 = component2_data[::stride]  # Y-axis (stored as "RMSD" in DataFrame for legacy reasons)

        # Create DataFrame for landscape
        import pandas as pd
        df = pd.DataFrame({
            "frame": list(range(len(component1))),
            "Q": component1,  # Actually RMSD (X-axis)
            "RMSD": component2,  # Actually eRMSD (Y-axis)
            "traj": "traj_1",
        })
        
        # Save dataframe for updates
        os.makedirs(generate_data_path, exist_ok=True)
        with open(os.path.join(generate_data_path, "dataframe.pkl"), 'wb') as f:
            pickle.dump(df, f)
        print("Dataframe saved for landscape")
        
        # Generate landscape plot
        size = 65
        selected_regions = []
        max_RMSD, max_Q = max(df["RMSD"]), max(df['Q'])
        
        try:
            from src.plotting import energy_3dplot
            from src.plotting.create_plots import plot_landscapes_3D, plot_landscapes_2D
            
            (probability_matrix, allframes_matrix, Qbin, RMSDbin) = energy_3dplot.make_matrix_probability(df, size, max_RMSD, max_Q)
            energy_matrix, real_values = energy_3dplot.make_matrix_energy(probability_matrix, max_RMSD, size)
            
            metrics_to_calculate = [component1_name, component2_name]
            fig = plot_landscapes_3D(energy_matrix, Qbin, RMSDbin, max_RMSD, max_Q, real_values, selected_regions, metrics_to_calculate)
            fig2 = plot_landscapes_2D(energy_matrix, Qbin, RMSDbin, max_RMSD, max_Q, real_values, selected_regions, metrics_to_calculate)
            
            fig.write_html(os.path.join(plot_dir, "landscape.html"))
            
            plotly_data = plotly_to_json(fig)
            plotly_data2 = plotly_to_json(fig2)
            return [plotly_data, plotly_data2]
            
        except ImportError:
            logger.warning("energy_3dplot or landscape plot functions not available")
            return {"path": f"static/uploads/{session_id}/landscape_plot.png", "status": "fallback"}
        
    except Exception as exc:
        logger.error(f"Landscape plot calculation failed: {str(exc)}")
        raise exc

@app.task(bind=True, max_retries=3)
@log_task
def generate_2Dpairing_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id):
    """Generate 2D pairing plot"""
    try:
        if not BARNABA_AVAILABLE:
            raise ImportError("Barnaba not available")
            
        import barnaba as bb
        import numpy as np
        rvecs_traj, res_traj = bb.dump_rvec(trajectory_file, topology=topology_file, cutoff=100.0)
        nonzero = np.where(np.sum(rvecs_traj**2, axis=3) > 0.01)
        rr = rvecs_traj[nonzero]
        
        try:
            from src.plotting.create_plots import base_pairs_visualisation
            fig = base_pairs_visualisation(rr)
            fig.write_html(os.path.join(plot_dir, "2D_pairing.html"))
            plotly_data = plotly_to_json(fig)
            return plotly_data
        except ImportError:
            return {"path": f"static/uploads/{session_id}/2dpairing_plot.png", "status": "fallback"}
            
    except Exception as exc:
        logger.error(f"2D pairing calculation failed: {str(exc)}")
        raise exc

# Update tasks for interactive plots
@app.task(bind=True, max_retries=3)
@log_task
def update_contact_map_plot(self, generate_data_path, plot_path, frame_number, session_id):
    """Update contact map plot for a specific frame"""
    try:
        with open(os.path.join(generate_data_path, "contact_map_data.pkl"), 'rb') as f:
            loaded_data = pickle.load(f)

        frames_dict = loaded_data['frames_dict']
        sequence = loaded_data['sequence']

        frame_num, frame_data = sorted(frames_dict.items())[frame_number]
        print(f"Updating contact map for frame {frame_num}")
        
        try:
            from src.plotting.create_plots import plot_rna_contact_map
            fig = plot_rna_contact_map(frame_data, sequence, output_file=os.path.join(plot_path, "contact_map.html"), frame_number=frame_num)
            plotly_data = plotly_to_json(fig)
            return plotly_data
        except ImportError:
            logger.warning("create_plots module not available for update, using fallback")
            return {"path": f"static/uploads/{session_id}/contact_map_update.png", "status": "fallback"}
            
    except Exception as exc:
        logger.error(f"Contact map update failed: {str(exc)}")
        raise exc

@app.task(bind=True, max_retries=3)
@log_task
def update_landscape_frame(self, generate_data_path, coordinates):
    """Update landscape plot to show specific frame based on coordinates"""
    try:
        with open(os.path.join(generate_data_path, "dataframe.pkl"), 'rb') as f:
            loaded_data = pickle.load(f)
        
        df = loaded_data
        target_Q = coordinates['Q']
        target_RMSD = coordinates['RMSD']

        # Calculate Euclidean distance between target coordinates and all points
        import numpy as np
        distances = np.sqrt((df['Q'] - target_Q)**2 + (df['RMSD'] - target_RMSD)**2)
        closest_frame_idx = distances.idxmin()
        closest_frame = int(df.iloc[closest_frame_idx]['frame'])  # Convert to Python int
        
        logger.info(f"Found closest frame {closest_frame} for coordinates Q={target_Q}, RMSD={target_RMSD}")
        return closest_frame
        
    except Exception as exc:
        logger.error(f"Landscape frame update failed: {str(exc)}")
        raise exc

if __name__ == "__main__":
    logger.info("RNA analysis tasks module loaded")
