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
    worker_prefetch_multiplier=4,  # Allow workers to prefetch more tasks for better parallelism
    worker_concurrency=4,  # Set explicit concurrency level
    # Temporarily disable task routes to avoid queue routing issues
    # task_routes={
    #     # Route compute tasks to a specific queue for better resource management
    #     'src.core.tasks.compute_*': {'queue': 'compute'},
    #     'src.core.tasks.generate_*': {'queue': 'plots'},
    # },
    result_expires=3600,
    # Enable task result persistence for better debugging
    task_track_started=True,
    task_send_sent_event=True,
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

def load_cached_mdtraj_objects(session_id):
    """Load cached MDTraj objects for efficient Barnaba computations"""
    import mdtraj as md
    
    session_dir = os.path.join("static", "uploads", session_id)
    mdtraj_ref_path = os.path.join(session_dir, "mdtraj_reference.pkl")
    mdtraj_traj_path = os.path.join(session_dir, "mdtraj_trajectory.pkl")
    
    if os.path.exists(mdtraj_ref_path) and os.path.exists(mdtraj_traj_path):
        try:
            with open(mdtraj_ref_path, 'rb') as f:
                reference_traj = pickle.load(f)
            with open(mdtraj_traj_path, 'rb') as f:
                target_traj = pickle.load(f)
            
            logger.info(f"🚀 Loaded cached MDTraj objects: {target_traj.n_frames} frames, {target_traj.n_atoms} atoms")
            return reference_traj, target_traj
        except Exception as e:
            logger.warning(f"Failed to load cached MDTraj objects: {e}")
            return None, None
    else:
        logger.info("No cached MDTraj objects found")
        return None, None
    
def log_task(func):
    """Simple task logging decorator with parallel execution tracking"""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        task_name = func.__name__
        task_id = self.request.id if hasattr(self, 'request') else 'unknown'
        start_time = time.time()
        
        try:
            logger.info(f"🚀 Starting {task_name} [Task ID: {task_id}] [Worker: {os.getpid()}]")
            result = func(self, *args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"✅ Completed {task_name} [Task ID: {task_id}] in {duration:.2f}s [Worker: {os.getpid()}]")
            return result
        except Exception as exc:
            duration = time.time() - start_time
            logger.error(f"❌ Failed {task_name} [Task ID: {task_id}] after {duration:.2f}s [Worker: {os.getpid()}]: {exc}")
            raise
    return wrapper

# Phase 1: Metric Computation Tasks
@app.task(bind=True, max_retries=3)
@log_task
def compute_rmsd(self, *args):
    """Compute RMSD metric using cached MDTraj objects"""
    # Handle chain arguments
    if len(args) == 4:  # previous_result, topology_file, trajectory_file, session_id
        _, topology_file, trajectory_file, session_id = args
    else:  # topology_file, trajectory_file, session_id
        topology_file, trajectory_file, session_id = args
        
    if not BARNABA_AVAILABLE:
        raise ImportError("Barnaba not available")
    
    import barnaba as bb
    
    # Try to use cached MDTraj objects for massive performance improvement
    reference_traj, target_traj = load_cached_mdtraj_objects(session_id)
    
    if reference_traj is not None and target_traj is not None:
        logger.info("🚀 Using cached MDTraj objects for RMSD computation")
        rmsd_result = bb.rmsd_traj(reference_traj, target_traj, heavy_atom=True)
    else:
        # Fallback to file loading
        logger.info("⚠️  Using fallback file loading for RMSD")
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
    """Compute eRMSD metric using cached MDTraj objects"""
    # Handle chain arguments
    if len(args) == 4:  # previous_result, topology_file, trajectory_file, session_id
        _, topology_file, trajectory_file, session_id = args
    else:  # topology_file, trajectory_file, session_id
        topology_file, trajectory_file, session_id = args
        
    if not BARNABA_AVAILABLE:
        raise ImportError("Barnaba not available")
    
    import barnaba as bb
    
    # Try to use cached MDTraj objects for massive performance improvement
    reference_traj, target_traj = load_cached_mdtraj_objects(session_id)
    
    if reference_traj is not None and target_traj is not None:
        logger.info("🚀 Using cached MDTraj objects for eRMSD computation")
        ermsd_result = bb.ermsd_traj(reference_traj, target_traj)
    else:
        # Fallback to file loading
        logger.info("⚠️  Using fallback file loading for eRMSD")
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
    """Compute annotate metric using cached MDTraj objects"""
    # Handle chain arguments
    if len(args) == 4:  # previous_result, topology_file, trajectory_file, session_id
        _, topology_file, trajectory_file, session_id = args
    else:  # topology_file, trajectory_file, session_id
        topology_file, trajectory_file, session_id = args
        
    if not BARNABA_AVAILABLE:
        raise ImportError("Barnaba not available")
    
    import barnaba as bb
    
    # Try to use cached MDTraj objects for massive performance improvement
    reference_traj, target_traj = load_cached_mdtraj_objects(session_id)
    
    if target_traj is not None:
        logger.info("🚀 Using cached MDTraj objects for annotate computation")
        annotate_result = bb.annotate_traj(target_traj)
    else:
        # Fallback to file loading
        logger.info("⚠️  Using fallback file loading for annotate")
        annotate_result = bb.annotate(trajectory_file, topology=topology_file)
    
    # Save to session directory
    session_dir = os.path.join("static", "uploads", session_id)
    os.makedirs(session_dir, exist_ok=True)
    
    annotate_path = os.path.join(session_dir, "annotate_data.pkl")
    with open(annotate_path, 'wb') as f:
        pickle.dump(annotate_result, f)
    
    logger.info(f"Annotate computed and saved to {annotate_path}")
    return {"metric": "annotate", "status": "success", "path": annotate_path}

@app.task(bind=True, max_retries=3)
@log_task
def compute_radius_of_gyration(self, *args):
    """Compute Radius of Gyration using MDAnalysis"""
    # Handle chain arguments
    if len(args) == 4:  # previous_result, topology_file, trajectory_file, session_id
        _, topology_file, trajectory_file, session_id = args
    else:  # topology_file, trajectory_file, session_id
        topology_file, trajectory_file, session_id = args
        
    try:
        import MDAnalysis as mda
        from MDAnalysis.analysis.base import AnalysisBase
        import numpy as np
    except ImportError as e:
        raise ImportError(f"MDAnalysis not available: {e}")
    
    class RadiusOfGyration(AnalysisBase):
        """Calculate radius of gyration for RNA"""
        
        def __init__(self, universe, selection="nucleic", **kwargs):
            super().__init__(universe.trajectory, **kwargs)
            self.selection = universe.select_atoms(selection)
            self.results.rg = []
            self.results.rg_components = []  # x, y, z components
        
        def _single_frame(self):
            # Calculate radius of gyration
            positions = self.selection.positions
            masses = self.selection.masses
            
            # Center of mass
            com = np.average(positions, axis=0, weights=masses)
            
            # Distances from COM
            distances = positions - com
            
            # Radius of gyration
            rg_squared = np.average(np.sum(distances**2, axis=1), weights=masses)
            rg = np.sqrt(rg_squared)
            
            # Components (principal axes)
            rg_x = np.sqrt(np.average(distances[:, 0]**2, weights=masses))
            rg_y = np.sqrt(np.average(distances[:, 1]**2, weights=masses))
            rg_z = np.sqrt(np.average(distances[:, 2]**2, weights=masses))
            
            self.results.rg.append(rg)
            self.results.rg_components.append([rg_x, rg_y, rg_z])
    
    # Load trajectory with MDAnalysis
    u = mda.Universe(topology_file, trajectory_file)
    
    # Calculate radius of gyration
    logger.info("Computing radius of gyration...")
    rg_analysis = RadiusOfGyration(u, selection="nucleic")
    rg_analysis.run()
    
    rg_values = np.array(rg_analysis.results.rg)
    rg_components = np.array(rg_analysis.results.rg_components)
    
    # Prepare result data
    result_data = {
        'rg_values': rg_values.tolist(),
        'rg_components': rg_components.tolist(),
        'mean_rg': float(np.mean(rg_values)),
        'std_rg': float(np.std(rg_values)),
        'frames': list(range(len(rg_values))),
        'times': [i * 0.1 for i in range(len(rg_values))]  # Adjust timestep as needed
    }
    
    # Save to session directory
    session_dir = os.path.join("static", "uploads", session_id)
    os.makedirs(session_dir, exist_ok=True)
    
    rg_path = os.path.join(session_dir, "radius_of_gyration_data.pkl")
    with open(rg_path, 'wb') as f:
        pickle.dump(result_data, f)
    
    logger.info(f"Radius of gyration computed and saved to {rg_path}")
    logger.info(f"Rg range: {np.min(rg_values):.2f} - {np.max(rg_values):.2f} Å")
    return {"metric": "radius_of_gyration", "status": "success", "path": rg_path}

@app.task(bind=True, max_retries=3)
@log_task
def compute_end_to_end_distance(self, *args):
    """Compute End-to-End Distance (5' to 3' distance) using MDAnalysis"""
    # Handle chain arguments
    if len(args) == 5:  # previous_result, topology_file, trajectory_file, session_id, plot_settings
        _, topology_file, trajectory_file, session_id, plot_settings = args
    elif len(args) == 4:  # topology_file, trajectory_file, session_id, plot_settings
        topology_file, trajectory_file, session_id, plot_settings = args
    else:  # fallback for old calls without plot_settings
        topology_file, trajectory_file, session_id = args[:3]
        plot_settings = {}
        
    try:
        import MDAnalysis as mda
        from MDAnalysis.analysis.base import AnalysisBase
        import numpy as np
    except ImportError as e:
        raise ImportError(f"MDAnalysis not available: {e}")
    
    class EndToEndDistance(AnalysisBase):
        """Calculate end-to-end distance for RNA (5' to 3' distance)"""
        
        def __init__(self, universe, five_prime_atom="C5'", three_prime_atom="C3'", **kwargs):
            super().__init__(universe.trajectory, **kwargs)
            self.universe = universe
            self.five_prime_atom_name = five_prime_atom
            self.three_prime_atom_name = three_prime_atom
            
            # Find 5' and 3' atoms with fallback mechanism
            residues = self.universe.select_atoms("nucleic").residues
            
            if len(residues) == 0:
                # If no nucleic residues found, try to use all atoms as fallback
                logger.warning("No nucleic acid residues found, trying all atoms")
                all_atoms = self.universe.atoms
                if len(all_atoms) == 0:
                    raise ValueError("No atoms found in the structure")
                # Use first and last atom as ultimate fallback
                self.five_prime = all_atoms[[0]]  # Use first atom
                self.three_prime = all_atoms[[-1]]  # Use last atom
                logger.warning(f"Using first atom as 5' end: {self.five_prime[0]}")
                logger.warning(f"Using last atom as 3' end: {self.three_prime[0]}")
            else:
                # First residue - 5' end (use specified atom)
                first_res = residues[0]
                self.five_prime = first_res.atoms.select_atoms(f"name {self.five_prime_atom_name}")
                if len(self.five_prime) == 0:
                    # Fallback: try the other common atom type
                    fallback_atom = "P" if self.five_prime_atom_name == "C5'" else "C5'"
                    self.five_prime = first_res.atoms.select_atoms(f"name {fallback_atom}")
                
                # Last residue - 3' end (use specified atom)
                last_res = residues[-1]
                self.three_prime = last_res.atoms.select_atoms(f"name {self.three_prime_atom_name}")
                if len(self.three_prime) == 0:
                    # Fallback: try the other common atom type
                    fallback_atom = "P" if self.three_prime_atom_name == "C3'" else "C3'"
                    self.three_prime = last_res.atoms.select_atoms(f"name {fallback_atom}")
                
                # Fallback to first/last atoms if specific atoms not found
                if len(self.five_prime) == 0:
                    logger.warning(f"Could not find 5' atom (C5' or P) in first residue, using first atom as fallback")
                    self.five_prime = first_res.atoms[[0]]  # Use first atom of first residue
                
                if len(self.three_prime) == 0:
                    logger.warning(f"Could not find 3' atom (C3' or P) in last residue, using last atom as fallback")
                    self.three_prime = last_res.atoms[[-1]]  # Use last atom of last residue
                
                logger.info(f"5' atom: {self.five_prime[0]} (residue {first_res.resname}{first_res.resid})")
                logger.info(f"3' atom: {self.three_prime[0]} (residue {last_res.resname}{last_res.resid})")
            
            self.results.distances = []
        
        def _single_frame(self):
            distance = np.linalg.norm(self.five_prime.positions[0] - self.three_prime.positions[0])
            self.results.distances.append(distance)
    
    # Load trajectory with MDAnalysis
    u = mda.Universe(topology_file, trajectory_file)
    
    # Extract atom preferences from plot settings
    five_prime_atom = plot_settings.get("five_prime_atom", "C5'")
    three_prime_atom = plot_settings.get("three_prime_atom", "C3'")
    logger.info(f"Using atom preferences: 5' = {five_prime_atom}, 3' = {three_prime_atom}")
    
    try:
        distances = None
        
        # Try primary method: EndToEndDistance class
        try:
            logger.info("Computing end-to-end distances...")
            end_to_end = EndToEndDistance(u, five_prime_atom=five_prime_atom, three_prime_atom=three_prime_atom)
            end_to_end.run()
            distances = np.array(end_to_end.results.distances)
            
        except Exception as e:
            # Ultimate fallback: simple distance calculation between first and last atom
            logger.warning(f"EndToEndDistance class failed: {e}")
            logger.warning("Using simple fallback: distance between first and last atom")
            
            # Get all atoms and calculate distance between first and last
            all_atoms = u.atoms
            if len(all_atoms) < 2:
                raise ValueError("Not enough atoms to calculate end-to-end distance")
                
            first_atom = all_atoms[0]
            last_atom = all_atoms[-1]
            
            distances = []
            for ts in u.trajectory:
                distance = np.linalg.norm(first_atom.position - last_atom.position)
                distances.append(distance)
            
            distances = np.array(distances)
            logger.info(f"Fallback calculation successful using atoms: {first_atom} to {last_atom}")
        
        # Prepare result data (runs regardless of which calculation method succeeded)
        result_data = {
            'distances': distances.tolist(),
            'mean_distance': float(np.mean(distances)),
            'std_distance': float(np.std(distances)),
            'min_distance': float(np.min(distances)),
            'max_distance': float(np.max(distances)),
            'frames': list(range(len(distances))),
            'times': [i * 0.1 for i in range(len(distances))]  # Adjust timestep as needed
        }
        
        # Save to session directory
        session_dir = os.path.join("static", "uploads", session_id)
        os.makedirs(session_dir, exist_ok=True)
        
        e2e_path = os.path.join(session_dir, "end_to_end_distance_data.pkl")
        with open(e2e_path, 'wb') as f:
            pickle.dump(result_data, f)
        
        logger.info(f"End-to-end distance computed and saved to {e2e_path}")
        logger.info(f"Distance range: {np.min(distances):.2f} - {np.max(distances):.2f} Å")
        return {"metric": "end_to_end_distance", "status": "success", "path": e2e_path}
        
    except ValueError as e:
        # Handle case where atoms are not found
        error_msg = str(e)
        logger.error(f"End-to-end distance calculation failed: {error_msg}")
        return {"metric": "end_to_end_distance", "status": "error", "error": error_msg}

@app.task(bind=True, max_retries=3)
@log_task
def compute_q_value(self, *args):
    """Compute Q-value (fraction of native contacts) using MDTraj"""
    # Handle chain arguments
    if len(args) == 4:  # previous_result, topology_file, trajectory_file, session_id
        _, topology_file, trajectory_file, session_id = args
    else:  # topology_file, trajectory_file, session_id
        topology_file, trajectory_file, session_id = args
        
    try:
        import mdtraj as md
        import numpy as np
        from itertools import combinations
    except ImportError as e:
        raise ImportError(f"Required packages not available: {e}")
    
    def best_hummer_q(traj, native):
        """Compute the fraction of native contacts according the definition from
        Best, Hummer and Eaton [1]

        Parameters
        ----------
        traj : md.Trajectory
            The trajectory to do the computation for
        native : md.Trajectory
            The 'native state'. This can be an entire trajectory, or just a single frame.
            Only the first conformation is used

        Returns
        -------
        q : np.array, shape=(len(traj),)
            The fraction of native contacts in each frame of `traj`

        References
        ----------
        ..[1] Best, Hummer, and Eaton, "Native contacts determine protein folding
              mechanisms in atomistic simulations" PNAS (2013)
        """

        BETA_CONST = 10
        LAMBDA_CONST = 1.8
        NATIVE_CUTOFF = 0.45  # nanometers

        # get the indices of all of the heavy atoms
        heavy = native.topology.select('all')
        # get the pairs of heavy atoms which are farther than 3
        # residues apart
        heavy_pairs = np.array(
            [(i,j) for (i,j) in combinations(heavy, 2)
                if abs(native.topology.atom(i).residue.index - \
                       native.topology.atom(j).residue.index) > 3])

        # compute the distances between these pairs in the native state
        heavy_pairs_distances = md.compute_distances(native[0], heavy_pairs)[0]
        # and get the pairs s.t. the distance is less than NATIVE_CUTOFF
        native_contacts = heavy_pairs[heavy_pairs_distances < NATIVE_CUTOFF]
        print(f"Number of native contacts: {len(native_contacts)}")

        # now compute these distances for the whole trajectory
        r = md.compute_distances(traj, native_contacts)
        # and recompute them for just the native state
        r0 = md.compute_distances(native[0], native_contacts)

        q = np.mean(1.0 / (1 + np.exp(BETA_CONST * (r - LAMBDA_CONST * r0))), axis=1)
        return q
    
    # Try to use cached MDTraj objects for massive performance improvement
    reference_traj, target_traj = load_cached_mdtraj_objects(session_id)
    
    if reference_traj is not None and target_traj is not None:
        logger.info("Using cached MDTraj objects for Q-value computation")
        q_values = best_hummer_q(target_traj, reference_traj)
    else:
        # Fallback to file loading
        logger.info("Using fallback file loading for Q-value")
        reference_traj = md.load(topology_file)
        target_traj = md.load(trajectory_file, top=topology_file)
        q_values = best_hummer_q(target_traj, reference_traj)
    
    # Prepare result data
    result_data = {
        'q_values': q_values.tolist(),
        'mean_q': float(np.mean(q_values)),
        'std_q': float(np.std(q_values)),
        'min_q': float(np.min(q_values)),
        'max_q': float(np.max(q_values)),
        'frames': list(range(len(q_values))),
        'times': [i * 0.1 for i in range(len(q_values))]  # Adjust timestep as needed
    }
    
    # Save to session directory
    session_dir = os.path.join("static", "uploads", session_id)
    os.makedirs(session_dir, exist_ok=True)
    
    q_path = os.path.join(session_dir, "q_value_data.pkl")
    with open(q_path, 'wb') as f:
        pickle.dump(result_data, f)
    
    logger.info(f"Q-value computed and saved to {q_path}")
    logger.info(f"Q range: {np.min(q_values):.3f} - {np.max(q_values):.3f}")
    return {"metric": "q_value", "status": "success", "path": q_path}

@app.task(bind=True, max_retries=3)
@log_task
def compute_dimensionality_reduction(self, *args):
    """Compute PCA, UMAP, and t-SNE for conformational analysis"""
    # Handle chain arguments
    if len(args) == 4:  # previous_result, topology_file, trajectory_file, session_id
        _, topology_file, trajectory_file, session_id = args
    else:  # topology_file, trajectory_file, session_id
        topology_file, trajectory_file, session_id = args
        
    try:
        import MDAnalysis as mda
        import numpy as np
        from sklearn.decomposition import PCA
        from sklearn.manifold import TSNE
        import umap
    except ImportError as e:
        raise ImportError(f"Required packages not available: {e}")
    
    def prepare_coordinates(universe, selection="nucleic"):
        """Extract coordinates for all frames"""
        atoms = universe.select_atoms(selection)
        logger.info(f"Selected {len(atoms)} atoms for dimensionality reduction")
        
        coordinates = []
        for ts in universe.trajectory:
            coords = atoms.positions.flatten()
            coordinates.append(coords)
        
        coordinates = np.array(coordinates)
        logger.info(f"Coordinate matrix shape: {coordinates.shape}")
        return coordinates, atoms
    
    # Load trajectory with MDAnalysis
    u = mda.Universe(topology_file, trajectory_file)
    
    # Get coordinates
    logger.info("Preparing coordinates for dimensionality reduction...")
    coords, selected_atoms = prepare_coordinates(u, selection="nucleic")
    
    # Align to remove translational and rotational motion
    logger.info("Aligning structures...")
    reference_coords = coords[0].reshape(-1, 3)
    aligned_coords = []
    
    for i, frame_coords in enumerate(coords):
        frame_coords_3d = frame_coords.reshape(-1, 3)
        
        # Simple alignment (center at origin)
        ref_center = reference_coords.mean(axis=0)
        frame_center = frame_coords_3d.mean(axis=0)
        
        aligned_frame = frame_coords_3d - frame_center + ref_center
        aligned_coords.append(aligned_frame.flatten())
    
    aligned_coords = np.array(aligned_coords)
    logger.info(f"Aligned coordinates shape: {aligned_coords.shape}")
    
    # PCA Analysis
    logger.info("Running PCA...")
    # Ensure n_components is valid for the data
    max_components = min(aligned_coords.shape[0] - 1, aligned_coords.shape[1])
    n_components = min(10, max_components)
    
    if n_components < 2:
        logger.warning(f"Cannot perform PCA: insufficient data (shape: {aligned_coords.shape}, max_components: {max_components})")
        # Create dummy data for compatibility
        pca_coords = aligned_coords[:, :2] if aligned_coords.shape[1] >= 2 else np.column_stack([aligned_coords[:, 0], aligned_coords[:, 0]])
        explained_variance = np.array([1.0, 0.0])  # Dummy variance
    else:
        logger.info(f"Using PCA with {n_components} components (data shape: {aligned_coords.shape})")
        pca = PCA(n_components=n_components)
        pca_coords = pca.fit_transform(aligned_coords)
        explained_variance = pca.explained_variance_ratio_
    
    logger.info(f"PC1 explains {explained_variance[0]*100:.1f}% of variance")
    if len(explained_variance) > 1:
        logger.info(f"PC2 explains {explained_variance[1]*100:.1f}% of variance")
    else:
        logger.info("Only 1 principal component available")
    
    # UMAP Analysis
    logger.info("Running UMAP...")
    # Adjust n_neighbors for small datasets (must be < n_samples)
    n_neighbors = min(15, aligned_coords.shape[0] - 1)
    n_neighbors = max(2, n_neighbors)  # Ensure at least 2 neighbors
    
    logger.info(f"Using UMAP with {n_neighbors} neighbors (data shape: {aligned_coords.shape})")
    umap_reducer = umap.UMAP(
        n_components=2, 
        n_neighbors=n_neighbors, 
        min_dist=0.1, 
        random_state=42
    )
    umap_coords = umap_reducer.fit_transform(aligned_coords)
    
    # t-SNE Analysis
    logger.info("Running t-SNE...")
    # Use PCA preprocessing for t-SNE (recommended for high-dimensional data)
    # n_components must be <= min(n_samples, n_features)
    max_components = min(aligned_coords.shape[0] - 1, aligned_coords.shape[1])
    n_components_pca = min(50, max_components)
    
    if n_components_pca < 1:
        logger.warning(f"Cannot perform PCA preprocessing: insufficient data (shape: {aligned_coords.shape})")
        # Skip PCA preprocessing and use original data directly
        pca_coords_50 = aligned_coords
    else:
        logger.info(f"Using PCA with {n_components_pca} components (data shape: {aligned_coords.shape})")
        pca_50 = PCA(n_components=n_components_pca)
        pca_coords_50 = pca_50.fit_transform(aligned_coords)
    
    tsne = TSNE(
        n_components=2, 
        perplexity=min(30, len(aligned_coords)//4), 
        random_state=42,
        init='pca'
    )
    tsne_coords = tsne.fit_transform(pca_coords_50)
    
    # Prepare result data
    result_data = {
        'pca_coordinates': pca_coords[:, :3].tolist(),  # First 3 PCs
        'pca_explained_variance': explained_variance[:10].tolist(),
        'umap_coordinates': umap_coords.tolist(),
        'tsne_coordinates': tsne_coords.tolist(),
        'frames': list(range(len(pca_coords))),
        'times': [i * 0.1 for i in range(len(pca_coords))]  # Adjust timestep as needed
    }
    
    # Save to session directory
    session_dir = os.path.join("static", "uploads", session_id)
    os.makedirs(session_dir, exist_ok=True)
    
    dimred_path = os.path.join(session_dir, "dimensionality_reduction_data.pkl")
    with open(dimred_path, 'wb') as f:
        pickle.dump(result_data, f)
    
    logger.info(f"Dimensionality reduction computed and saved to {dimred_path}")
    return {"metric": "dimensionality_reduction", "status": "success", "path": dimred_path}

# Phase 2: Plot Generation Tasks
@app.task(bind=True, max_retries=3)
@log_task
def generate_rmsd_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, plot_settings={}):
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
            
            # Try to use cached MDTraj objects first
            # Use heavy_atom setting from plot_settings (default: True)
            heavy_atom = plot_settings.get("heavy_atom", True)
            
            reference_traj, target_traj = load_cached_mdtraj_objects(session_id)
            
            if reference_traj is not None and target_traj is not None:
                logger.info("🚀 Using cached MDTraj objects for RMSD plot fallback")
                rmsd = bb.rmsd_traj(reference_traj, target_traj, heavy_atom=heavy_atom)
            else:
                logger.info("⚠️  Using file loading for RMSD plot fallback")
                rmsd = bb.rmsd(topology_file, trajectory_file, topology=topology_file, heavy_atom=heavy_atom)

        # Create plot using create_plots functions with settings
        #try:
        logging.info("Default plots are used")
        fig = plot_rmsd(rmsd, plot_settings)
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
def generate_ermsd_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, plot_settings={}):
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
            fig = plot_ermsd(ermsd, plot_settings)
            
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
def generate_torsion_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, torsion_residue=0, plot_settings={}):
    """Generate torsion plot"""
    try:
        if not BARNABA_AVAILABLE:
            raise ImportError("Barnaba not available")
            
        import barnaba as bb
        
        # Try to use cached MDTraj objects for performance improvement
        reference_traj, target_traj = load_cached_mdtraj_objects(session_id)
        
        if target_traj is not None:
            logger.info("🚀 Using cached MDTraj objects for torsion computation")
            angles, res = bb.backbone_angles_traj(target_traj)
        else:
            logger.info("⚠️  Using fallback file loading for torsion")
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
def generate_sec_structure_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, plot_settings={}):
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
def generate_dotbracket_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, plot_settings={}):
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
def generate_arc_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, plot_settings={}):
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
def generate_contact_map_plot(self, topology_file, trajectory_file, files_path, plot_dir, generate_data_path, session_id, plot_settings={}):
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

    def process_barnaba_stackings(stackings, res):
        """Process barnaba stackings and residue information into frames dictionary"""
        import pandas as pd
        frames_dict = {}
        
        # Create sequence from residue information
        sequence = [r[0] for r in res]  # Assuming res contains nucleotide types
        
        print(f"Processing stackings data. Total frames: {len(stackings)}")
        print(f"Sample stacking data structure: {stackings[:2] if stackings else 'Empty'}")
        
        # Process each frame
        for frame_num, frame_data in enumerate(stackings):
            stacking_interactions = []
            
            # Debug: print frame data structure
            if frame_num < 2:  # Only print first 2 frames for debugging
                print(f"Frame {frame_num} stacking data: {frame_data}")
            
            # Each frame contains a list of stackings and their annotations
            if len(frame_data) == 2:
                stack_indices = frame_data[0]
                annotations = frame_data[1]
                
                if frame_num < 2:
                    print(f"Frame {frame_num} - Indices: {stack_indices[:5] if stack_indices else 'Empty'}")
                    print(f"Frame {frame_num} - Annotations: {annotations[:5] if annotations else 'Empty'}")
                
                for stack_idx, stack in enumerate(stack_indices):
                    if not stack:
                        continue
                    
                    res_i = stack[0] + 1  # Convert to 1-based indexing
                    res_j = stack[1] + 1
                    
                    # Get residue names from the sequence
                    if 0 <= res_i - 1 < len(sequence) and 0 <= res_j - 1 < len(sequence):
                        res_i_name = f"{sequence[res_i - 1]}{res_i}"
                        res_j_name = f"{sequence[res_j - 1]}{res_j}"
                    else:
                        res_i_name = f"N{res_i}"
                        res_j_name = f"N{res_j}"
                    
                    anno = annotations[stack_idx] if stack_idx < len(annotations) else 'XXX'
                    
                    stacking_interactions.append({
                        'res_i': res_i,
                        'res_j': res_j,
                        'res_i_name': res_i_name,
                        'res_j_name': res_j_name,
                        'anno': anno
                    })
            
            # Always assign a DataFrame (even if empty)
            frames_dict[frame_num] = pd.DataFrame(stacking_interactions)
            
            if frame_num < 2:
                print(f"Frame {frame_num} processed: {len(stacking_interactions)} stacking interactions")
        
        print(f"Stacking processing complete. Processed {len(frames_dict)} frames")
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
        stackings_frames_dict, stackings_sequence = process_barnaba_stackings(stackings, res)
        print(len(frames_dict))
        print(f"RNA length: {len(sequence)} nucleotides")
        print(f"Found {len(frames_dict)} frames in the data")
        print(f"Found {len(stackings_frames_dict)} frames in stackings data")
        frame_num, frame_data = sorted(frames_dict.items())[0]

        # Save pairings to CSV
        import pandas as pd
        pairings_df = pd.DataFrame(pairings)
        pairings_df.to_csv(os.path.join(files_path, "pairings.csv"), index=False)
        
        # Save stackings to CSV
        stackings_df = pd.DataFrame(stackings)
        stackings_df.to_csv(os.path.join(files_path, "stackings.csv"), index=False)

        # Save frames_dict and sequence to pickle
        os.makedirs(generate_data_path, exist_ok=True)
        data_to_save = {
            'frames_dict': frames_dict,
            'sequence': sequence
        }
        with open(os.path.join(generate_data_path, "contact_map_data.pkl"), 'wb') as f:
            pickle.dump(data_to_save, f)
            
        # Save stackings data to pickle  
        stackings_data_to_save = {
            'frames_dict': stackings_frames_dict,
            'sequence': stackings_sequence
        }
        with open(os.path.join(generate_data_path, "stacking_map_data.pkl"), 'wb') as f:
            pickle.dump(stackings_data_to_save, f)

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
def generate_annotate_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, plot_settings={}):
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
def generate_jcoupling_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, plot_settings={}):
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
def generate_escore_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, plot_settings={}):
    """Generate e-score plot"""
    return {"path": f"static/uploads/{session_id}/escore_plot.png", "status": "placeholder"}

@app.task(bind=True, max_retries=3)
@log_task
def generate_landscape_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, landscape_params, generate_data_path, plot_settings={}):
    """Generate landscape plot using pre-computed metrics"""
    try:
        # Load pre-computed RMSD, eRMSD, and Q-value data if available
        rmsd_data = None
        ermsd_data = None
        q_value_data = None
        
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
        
        q_value_path = os.path.join("static", "uploads", session_id, "q_value_data.pkl")
        if os.path.exists(q_value_path):
            with open(q_value_path, 'rb') as f:
                q_value_data_dict = pickle.load(f)
                q_value_data = q_value_data_dict['q_values']
            print(f"LOADED Q-VALUE FROM SAVED DATA FOR LANDSCAPE")
        
        # If data not available, fall back to computation
        needs_computation = False
        components_needed = []
        
        # Check which components we need to compute
        first_dim = landscape_params[1] if len(landscape_params) > 1 else 1
        second_dim = landscape_params[2] if len(landscape_params) > 2 else 2
        
        if first_dim == 1 or second_dim == 1:  # RMSD needed
            if rmsd_data is None:
                components_needed.append("rmsd")
                needs_computation = True
        
        if first_dim == 2 or second_dim == 2:  # eRMSD needed
            if ermsd_data is None:
                components_needed.append("ermsd")
                needs_computation = True
        
        if first_dim in [3, 4] or second_dim in [3, 4]:  # Q-value needed for both Q and Fraction of Contact Formed
            if q_value_data is None:
                components_needed.append("q_value")
                needs_computation = True
        
        if needs_computation:
            logger.warning(f"Pre-computed metrics not available, computing: {components_needed}")
            
            # Try to use cached MDTraj objects for landscape fallback computations
            reference_traj, target_traj = load_cached_mdtraj_objects(session_id)
            
            if reference_traj is not None and target_traj is not None:
                logger.info("🚀 Using cached MDTraj objects for landscape fallback computations")
                
                if "rmsd" in components_needed:
                    if not BARNABA_AVAILABLE:
                        raise ImportError("Barnaba not available and no pre-computed RMSD data")
                    import barnaba as bb
                    rmsd_data = bb.rmsd_traj(reference_traj, target_traj, heavy_atom=True)
                
                if "ermsd" in components_needed:
                    if not BARNABA_AVAILABLE:
                        raise ImportError("Barnaba not available and no pre-computed eRMSD data")
                    import barnaba as bb
                    ermsd_data = bb.ermsd_traj(reference_traj, target_traj)
                
                if "q_value" in components_needed:
                    import mdtraj as md
                    import numpy as np
                    from itertools import combinations
                    
                    def best_hummer_q(traj, native):
                        BETA_CONST = 10
                        LAMBDA_CONST = 1.8
                        NATIVE_CUTOFF = 0.45  # nanometers
                        heavy = native.topology.select('all')
                        heavy_pairs = np.array(
                            [(i,j) for (i,j) in combinations(heavy, 2)
                                if abs(native.topology.atom(i).residue.index - \
                                       native.topology.atom(j).residue.index) > 3])
                        heavy_pairs_distances = md.compute_distances(native[0], heavy_pairs)[0]
                        native_contacts = heavy_pairs[heavy_pairs_distances < NATIVE_CUTOFF]
                        r = md.compute_distances(traj, native_contacts)
                        r0 = md.compute_distances(native[0], native_contacts)
                        q = np.mean(1.0 / (1 + np.exp(BETA_CONST * (r - LAMBDA_CONST * r0))), axis=1)
                        return q
                    
                    q_value_data = best_hummer_q(target_traj, reference_traj).tolist()
            
            else:
                logger.info("Using file loading for landscape fallback computations")
                import mdtraj as md
                
                if "rmsd" in components_needed:
                    if not BARNABA_AVAILABLE:
                        raise ImportError("Barnaba not available and no pre-computed RMSD data")
                    import barnaba as bb
                    rmsd_data = bb.rmsd(topology_file, trajectory_file, topology=topology_file, heavy_atom=True)
                
                if "ermsd" in components_needed:
                    if not BARNABA_AVAILABLE:
                        raise ImportError("Barnaba not available and no pre-computed eRMSD data")
                    import barnaba as bb
                    ermsd_data = bb.ermsd(topology_file, trajectory_file, topology=topology_file)
                
                if "q_value" in components_needed:
                    reference_traj = md.load(topology_file)
                    target_traj = md.load(trajectory_file, top=topology_file)
                    
                    import numpy as np
                    from itertools import combinations
                    
                    def best_hummer_q(traj, native):
                        BETA_CONST = 10
                        LAMBDA_CONST = 1.8
                        NATIVE_CUTOFF = 0.45  # nanometers
                        heavy = native.topology.select('all')
                        heavy_pairs = np.array(
                            [(i,j) for (i,j) in combinations(heavy, 2)
                                if abs(native.topology.atom(i).residue.index - \
                                       native.topology.atom(j).residue.index) > 3])
                        heavy_pairs_distances = md.compute_distances(native[0], heavy_pairs)[0]
                        native_contacts = heavy_pairs[heavy_pairs_distances < NATIVE_CUTOFF]
                        r = md.compute_distances(traj, native_contacts)
                        r0 = md.compute_distances(native[0], native_contacts)
                        q = np.mean(1.0 / (1 + np.exp(BETA_CONST * (r - LAMBDA_CONST * r0))), axis=1)
                        return q
                    
                    q_value_data = best_hummer_q(target_traj, reference_traj).tolist()
        
        # Extract landscape parameters
        stride = int(landscape_params[0])

        # Map dimension selections to actual metric names after data computation
        # Users select: 1 = RMSD, 2 = eRMSD, 3 = Q-value, 4 = Fraction of Contact Formed
        dimension_map = {
            1: ("RMSD (nm)", rmsd_data),
            2: ("eRMSD", ermsd_data),
            3: ("Q-value", q_value_data),
            4: ("Fraction of Contact Formed", q_value_data),  # Using same Q-value calculation for now
            "1": ("RMSD (nm)", rmsd_data),
            "2": ("eRMSD", ermsd_data),
            "3": ("Q-value", q_value_data),
            "4": ("Fraction of Contact Formed", q_value_data)  # Using same Q-value calculation for now
        }

        # Get user-selected dimensions (default to RMSD and eRMSD if not specified)
        first_dim = landscape_params[1] if len(landscape_params) > 1 else 1
        second_dim = landscape_params[2] if len(landscape_params) > 2 else 2

        component1_name, component1_data = dimension_map.get(first_dim, ("RMSD (nm)", rmsd_data))
        component2_name, component2_data = dimension_map.get(second_dim, ("eRMSD", ermsd_data))
        
        # Check if required data is available
        if component1_data is None:
            raise ValueError(f"Data for first dimension (dim {first_dim}) not available. Required metrics may not have been computed.")
        if component2_data is None:
            raise ValueError(f"Data for second dimension (dim {second_dim}) not available. Required metrics may not have been computed.")

        # Apply stride to the data
        component1 = component1_data[::stride]  # X-axis
        component2 = component2_data[::stride]  # Y-axis

        # Create DataFrame for landscape with correct column names
        # Note: Using "Q" and "RMSD" as column names for backward compatibility with plotting functions
        # but the actual data corresponds to the user-selected dimensions
        import pandas as pd
        df = pd.DataFrame({
            "frame": list(range(len(component1))),
            "Q": component1,  # X-axis data (could be RMSD, eRMSD, or Q-value)
            "RMSD": component2,  # Y-axis data (could be RMSD, eRMSD, or Q-value)
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

            metrics_to_calculate = [component1_name, component2_name]
            (probability_matrix, allframes_matrix, Qbin, RMSDbin) = energy_3dplot.make_matrix_probability(df, size, max_RMSD, max_Q, metrics_to_calculate)
            energy_matrix, real_values = energy_3dplot.make_matrix_energy(probability_matrix, max_RMSD, size)

            # Extract axis limits from plot settings if available
            x_min = plot_settings.get('x_min', None)
            x_max = plot_settings.get('x_max', None)
            y_min = plot_settings.get('y_min', None)
            y_max = plot_settings.get('y_max', None)

            # Convert empty strings to None
            x_min = None if x_min == '' or x_min is None else float(x_min)
            x_max = None if x_max == '' or x_max is None else float(x_max)
            y_min = None if y_min == '' or y_min is None else float(y_min)
            y_max = None if y_max == '' or y_max is None else float(y_max)

            fig = plot_landscapes_3D(energy_matrix, Qbin, RMSDbin, max_RMSD, max_Q, real_values, selected_regions, metrics_to_calculate, x_min, x_max, y_min, y_max)
            fig2 = plot_landscapes_2D(energy_matrix, Qbin, RMSDbin, max_RMSD, max_Q, real_values, selected_regions, metrics_to_calculate, x_min, x_max, y_min, y_max)
            
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
def generate_2Dpairing_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, plot_settings={}):
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
def update_stacking_map_plot(self, generate_data_path, plot_path, frame_number, session_id):
    """Update stacking map plot for a specific frame"""
    try:
        with open(os.path.join(generate_data_path, "stacking_map_data.pkl"), 'rb') as f:
            loaded_data = pickle.load(f)

        frames_dict = loaded_data['frames_dict']
        sequence = loaded_data['sequence']

        frame_num, frame_data = sorted(frames_dict.items())[frame_number]
        print(f"Updating stacking map for frame {frame_num}")
        
        try:
            # Ensure the plot directory exists
            os.makedirs(plot_path, exist_ok=True)
            
            from src.plotting.create_plots import plot_rna_stacking_map
            fig = plot_rna_stacking_map(frame_data, sequence, output_file=os.path.join(plot_path, "stacking_map.html"), frame_number=frame_num)
            plotly_data = plotly_to_json(fig)
            return plotly_data
        except ImportError:
            logger.warning("create_plots module not available for stacking update, using fallback")
            return {"path": f"static/uploads/{session_id}/stacking_map_update.png", "status": "fallback"}
            
    except Exception as exc:
        logger.error(f"Stacking map update failed: {str(exc)}")
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

@app.task(bind=True, max_retries=3)
@log_task
def generate_radius_of_gyration_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, plot_settings={}):
    """Generate interactive plot for radius of gyration analysis"""
    try:
        logger.info(f"Starting radius of gyration plot generation for session {session_id}")
        
        # Load precomputed data
        pickle_path = os.path.join("static", "uploads", session_id, 'radius_of_gyration_data.pkl')
        if not os.path.exists(pickle_path):
            raise FileNotFoundError(f"Radius of gyration data not found: {pickle_path}")
        
        with open(pickle_path, 'rb') as f:
            data = pickle.load(f)
        
        # Extract data
        frames = data['frames']
        rg_total = data['rg_values']  # Total Rg values
        rg_components = data['rg_components']  # Components as [[x,y,z], [x,y,z], ...]
        
        # Extract individual component arrays for CSV
        rg_x = [comp[0] for comp in rg_components]
        rg_y = [comp[1] for comp in rg_components]
        rg_z = [comp[2] for comp in rg_components]
        
        # Create plot using create_plots function
        logger.info(f"RADIUS_OF_GYRATION: Calling plot_radius_of_gyration with {len(frames)} frames")
        fig = plot_radius_of_gyration(frames, rg_total, rg_components, plot_settings)
        logger.info(f"RADIUS_OF_GYRATION: Plot figure created, type: {type(fig)}")
        
        # Save data to CSV
        import pandas as pd
        rg_df = pd.DataFrame({
            'Frame': frames,
            'Total_Rg': rg_total,
            'Rg_X': rg_x,
            'Rg_Y': rg_y,
            'Rg_Z': rg_z
        })
        csv_path = os.path.join(files_path, "radius_of_gyration_values.csv")
        rg_df.to_csv(csv_path, index=False)
        
        # Save plot to HTML
        html_path = os.path.join(plot_dir, "radius_of_gyration_plot.html")
        fig.write_html(html_path)
        
        # Convert to JSON
        plot_json = plotly_to_json(fig)
        logger.info(f"RADIUS_OF_GYRATION: JSON conversion complete, returning data for session {session_id}")
        
        logger.info(f"Radius of gyration plot generated successfully for session {session_id}")
        logger.info(f"Data saved to: {csv_path}")
        logger.info(f"Plot saved to: {html_path}")
        return plot_json
        
    except Exception as exc:
        logger.error(f"Radius of gyration plot generation failed for session {session_id}: {str(exc)}")
        raise exc


@app.task(bind=True, max_retries=3)
@log_task
def generate_end_to_end_distance_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, plot_settings=None):
    """Generate interactive plot for end-to-end distance analysis"""
    try:
        logger.info(f"Starting end-to-end distance plot generation for session {session_id}")
        
        # Load precomputed data
        pickle_path = os.path.join("static", "uploads", session_id, 'end_to_end_distance_data.pkl')
        if not os.path.exists(pickle_path):
            raise FileNotFoundError(f"End-to-end distance data not found: {pickle_path}")
        
        with open(pickle_path, 'rb') as f:
            data = pickle.load(f)
        
        # Check for errors
        if 'error' in data:
            logger.error(f"End-to-end distance contains error: {data['error']}")
            raise ValueError(data['error'])
        
        # Extract data
        frames = data['frames']
        distances = data['distances']
        
        # Create plot using create_plots function
        logger.info(f"END_TO_END_DISTANCE: Calling plot_end_to_end_distance with {len(frames)} frames")
        if plot_settings is None:
            plot_settings = {}
        fig = plot_end_to_end_distance(frames, distances, plot_settings)
        logger.info(f"END_TO_END_DISTANCE: Plot figure created, type: {type(fig)}")
        
        # Save data to CSV
        import pandas as pd
        e2e_df = pd.DataFrame({
            'Frame': frames,
            'End_to_End_Distance': distances
        })
        csv_path = os.path.join(files_path, "end_to_end_distance_values.csv")
        e2e_df.to_csv(csv_path, index=False)
        
        # Save plot to HTML
        html_path = os.path.join(plot_dir, "end_to_end_distance_plot.html")
        fig.write_html(html_path)
        
        # Convert to JSON
        plot_json = plotly_to_json(fig)
        logger.info(f"END_TO_END_DISTANCE: JSON conversion complete, returning data for session {session_id}")
        
        logger.info(f"End-to-end distance plot generated successfully for session {session_id}")
        logger.info(f"Data saved to: {csv_path}")
        logger.info(f"Plot saved to: {html_path}")
        return plot_json
        
    except Exception as exc:
        logger.error(f"End-to-end distance plot generation failed for session {session_id}: {str(exc)}")
        raise exc


@app.task(bind=True, max_retries=3)
@log_task
def generate_dimensionality_reduction_plot(self, topology_file, trajectory_file, files_path, plot_dir, session_id, method='pca', plot_settings={}):
    """Generate interactive plot for dimensionality reduction analysis"""
    try:
        logger.info(f"Starting {method.upper()} plot generation for session {session_id}")
        
        # Load precomputed data
        pickle_path = os.path.join("static", "uploads", session_id, 'dimensionality_reduction_data.pkl')
        if not os.path.exists(pickle_path):
            raise FileNotFoundError(f"Dimensionality reduction data not found: {pickle_path}")
        
        with open(pickle_path, 'rb') as f:
            data = pickle.load(f)
        
        # Map method names to data keys
        method_key_map = {
            'pca': 'pca_coordinates',
            'umap': 'umap_coordinates', 
            'tsne': 'tsne_coordinates'
        }
        
        # Check if requested method exists
        if method not in method_key_map:
            raise ValueError(f"Method {method} not supported. Available: {list(method_key_map.keys())}")
        
        data_key = method_key_map[method]
        if data_key not in data:
            raise ValueError(f"Method data {data_key} not found in dimensionality reduction data")
        
        method_data = data[data_key]
        frames = data['frames']
        
        # Convert to numpy array for easier indexing
        import numpy as np
        method_data = np.array(method_data)
        
        # Create plot using create_plots function
        logger.info(f"{method.upper()}: Calling plot_dimensionality_reduction with {len(frames)} frames, method={method}")
        fig = plot_dimensionality_reduction(frames, method_data, method)
        logger.info(f"{method.upper()}: Plot figure created, type: {type(fig)}")
        
        # Axis labels for CSV saving
        axis_labels = {
            'pca': ('PC1', 'PC2'),
            'umap': ('UMAP1', 'UMAP2'),
            'tsne': ('t-SNE1', 't-SNE2')
        }
        
        # Save data to CSV
        import pandas as pd
        method_df = pd.DataFrame({
            'Frame': frames,
            f'{axis_labels.get(method, (f"{method.upper()}1", f"{method.upper()}2"))[0]}': method_data[:, 0],
            f'{axis_labels.get(method, (f"{method.upper()}1", f"{method.upper()}2"))[1]}': method_data[:, 1]
        })
        csv_path = os.path.join(files_path, f"{method}_coordinates.csv")
        method_df.to_csv(csv_path, index=False)
        
        # Save plot to HTML
        html_path = os.path.join(plot_dir, f"{method}_plot.html")
        fig.write_html(html_path)
        
        # Convert to JSON
        plot_json = plotly_to_json(fig)
        logger.info(f"{method.upper()}: JSON conversion complete, returning data for session {session_id}")
        logger.info(f"{method.upper()} plot generated successfully for session {session_id}")
        return plot_json
        
    except Exception as exc:
        logger.error(f"{method.upper()} plot generation failed for session {session_id}: {str(exc)}")
        raise exc


@app.task(bind=True)
@log_task
def test_parallel_execution(self, task_number, duration=5):
    """Test task to verify parallel execution"""
    import time
    import os
    
    logger.info(f"Test task {task_number} starting on worker PID {os.getpid()}")
    time.sleep(duration)
    logger.info(f"Test task {task_number} completed on worker PID {os.getpid()}")
    return f"Task {task_number} completed in {duration}s on PID {os.getpid()}"

def test_celery_parallelism():
    """Helper function to test if Celery is running tasks in parallel"""
    from celery import group
    
    # Create 4 test tasks that each take 3 seconds
    test_jobs = [
        test_parallel_execution.s(i, 3) for i in range(4)
    ]
    
    start_time = time.time()
    logger.info("Starting parallel test - 4 tasks of 3 seconds each")
    
    # If running in parallel: should take ~3 seconds
    # If running sequentially: would take ~12 seconds
    test_group = group(test_jobs)
    result = test_group.apply_async()
    
    try:
        results = result.get(timeout=15)
        duration = time.time() - start_time
        
        logger.info(f"Parallel test completed in {duration:.2f}s")
        if duration < 6:  # Allow some overhead
            logger.info("✅ PARALLEL EXECUTION CONFIRMED")
        else:
            logger.warning("⚠️  SEQUENTIAL EXECUTION DETECTED")
            
        return duration, results
    except Exception as e:
        logger.error(f"Parallel test failed: {e}")
        return None, None

if __name__ == "__main__":
    logger.info("RNA analysis tasks module loaded")
