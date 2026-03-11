import os
import sys

# Set environment variables to prevent conflicts before importing eventlet
os.environ['EVENTLET_NO_GREENDNS'] = '1'

# Try eventlet import with better error handling
try:
    import eventlet
    # Conservative monkey patching to avoid conflicts
    eventlet.monkey_patch(socket=True, dns=False, time=False, select=True, thread=False, os=False)
    EVENTLET_AVAILABLE = True
except ImportError as e:
    print(f"Warning: eventlet import failed: {e}")
    EVENTLET_AVAILABLE = False
except Exception as e:
    print(f"Warning: eventlet configuration failed: {e}")
    EVENTLET_AVAILABLE = False

import time
import pickle
import hashlib
import concurrent.futures
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Optional, Tuple
from flask import (
    Flask,
    request,
    render_template,
    send_from_directory,
    redirect,
    url_for,
    session,
    send_file,
    jsonify,
    flash,
)
from werkzeug.utils import secure_filename
import uuid
import os
import glob
import pandas as pd
import MDAnalysis as mda
from MDAnalysis.coordinates import PDB
from MDAnalysis.analysis import rms
from MDAnalysis.topology.guessers import guess_types
from MDAnalysis.analysis.align import alignto
from rq import Queue
from redis import Redis
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import numpy as np
import json
import plotly
from plotly.tools import mpl_to_plotly

from flask_socketio import SocketIO, join_room, leave_room, emit
from io import BytesIO
from PIL import Image, ImageDraw
import sys
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
# Try to import analysis module, fallback if not available
try:
    from src.analysis.generate_contact_maps import *
except ImportError:
    print("Warning: Analysis module not found, some features may be limited")
import plotly.graph_objects as go
from string import ascii_uppercase
import barnaba as bb
import pickle
import io
import zipfile

from src.core.tasks import *
from src.core.tasks import app as celery_app
from celery.result import AsyncResult
import threading
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime, timedelta

template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../templates'))
static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../static'))
uploads_dir = os.path.abspath(os.path.join(static_dir, 'uploads'))

app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
app.secret_key = "pi"
app.debug = False

# Performance optimizations
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 86400  # Cache static files for 24 hours

# Ensure uploads directory exists
os.makedirs(uploads_dir, exist_ok=True)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def setup_session_logger(session_id: str) -> logging.FileHandler:
    """Add a file handler to the logger that writes to the session's upload directory.
    Returns the handler so it can be removed when the request is done."""
    session_dir = os.path.join(uploads_dir, session_id)
    os.makedirs(session_dir, exist_ok=True)
    log_path = os.path.join(session_dir, "session.log")
    handler = logging.FileHandler(log_path, mode='a')
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.info(f"=== Session {session_id} started ===")
    return handler


def remove_session_logger(handler: logging.FileHandler):
    """Remove and close the per-session file handler."""
    logger.removeHandler(handler)
    handler.close()


def convert_dimension_to_numeric(dimension_name: str) -> int:
    """
    Convert dimension name string to numeric value for backend processing.
    
    Args:
        dimension_name: String dimension name from frontend
        
    Returns:
        Numeric dimension code (1=RMSD, 2=eRMSD, 3=Q, 4=Fraction of Contact Formed)
    """
    dimension_map = {
        "RMSD": 1,
        "eRMSD": 2,
        "Q": 3,
        "Fraction of Contact Formed": 4
    }
    return dimension_map.get(dimension_name, 1)  # Default to RMSD

def convert_topology_to_pdb(topology_path: str, trajectory_path: str, session_id: str) -> str:
    """
    Convert topology file to PDB format if it's not supported by mdtraj and molstar.

    Supported formats (no conversion needed): .pdb, .gro, .mol2
    Unsupported formats (will be converted): .parm7, .prmtop, .psf, .top, .tpr, etc.

    Args:
        topology_path: Path to topology file
        trajectory_path: Path to trajectory file (needed to load coordinates for PDB writing)
        session_id: Session ID for logging

    Returns:
        Path to the topology file (original if already supported, converted if not)
    """
    logger = logging.getLogger(__name__)

    _, ext = os.path.splitext(topology_path.lower())
    supported_formats = ['.pdb', '.gro', '.mol2']

    if ext in supported_formats:
        logger.info(f"Topology format {ext} is supported, no conversion needed")
        return topology_path

    logger.info(f"Topology format {ext} is not supported by mdtraj/molstar, converting to PDB")

    try:
        directory = os.path.dirname(topology_path)
        base_name = os.path.splitext(os.path.basename(topology_path))[0]
        pdb_path = os.path.join(directory, f"{base_name}_converted.pdb")

        # Load with MDAnalysis using the trajectory to get coordinates
        u = mda.Universe(topology_path, trajectory_path)
        u.trajectory[0]  # Use first frame

        # Write as PDB
        logger.info(f"Converting topology to PDB: {pdb_path}")
        u.atoms.write(pdb_path)

        logger.info(f"Topology conversion completed: {pdb_path}")
        return pdb_path

    except Exception as e:
        logger.error(f"Failed to convert topology to PDB: {e}")
        emit_error(session_id, f"Topology conversion failed: {e}. Using original format.", 'warning')
        return topology_path


def convert_trajectory_to_xtc(topology_path: str, trajectory_path: str, session_id: str) -> str:
    """
    Convert trajectory file to XTC format if it's not in a supported format.
    
    Supported formats: dcd, nc, nctraj, trr, xtc
    Unsupported formats (will be converted): pdb with multiple frames, other formats
    
    Args:
        topology_path: Path to topology file (PDB/GRO)
        trajectory_path: Path to trajectory file
        session_id: Session ID for logging and file naming
        
    Returns:
        Path to the trajectory file (original if already supported, converted if not)
    """
    logger = logging.getLogger(__name__)
    
    # Extract file extension
    _, ext = os.path.splitext(trajectory_path.lower())
    supported_formats = ['.dcd', '.trr', '.xtc']
    
    # If already in supported format, return original path
    if ext in supported_formats:
        logger.info(f"Trajectory format {ext} is supported, no conversion needed")
        return trajectory_path
    
    # Check if it's a multi-frame PDB or other unsupported format
    logger.info(f"Trajectory format {ext} is not supported, converting to XTC")
    
    try:
        # Create output path
        directory = os.path.dirname(trajectory_path)
        base_name = os.path.splitext(os.path.basename(trajectory_path))[0]
        xtc_path = os.path.join(directory, f"{base_name}_converted.xtc")
        
        # Load and convert using MDAnalysis
        logger.info(f"Loading trajectory for conversion: {trajectory_path}")
        u = mda.Universe(topology_path, trajectory_path)
        
        # Check if trajectory has multiple frames
        n_frames = len(u.trajectory)
        logger.info(f"Trajectory has {n_frames} frames")
        
        if n_frames == 1:
            logger.warning(f"Trajectory appears to have only 1 frame, this might not be a proper trajectory file")
        
        # Write to XTC format
        logger.info(f"Converting trajectory to XTC: {xtc_path}")
        with mda.Writer(xtc_path, u.atoms.n_atoms) as writer:
            for ts in u.trajectory:
                writer.write(u.atoms)
        
        logger.info(f"Trajectory conversion completed: {xtc_path}")
        return xtc_path
        
    except Exception as e:
        logger.error(f"Failed to convert trajectory to XTC: {e}")
        # Emit error to user via socket if possible
        emit_error(session_id, f"Trajectory conversion failed: {e}. Using original format.", 'warning')
        # If conversion fails, try to return original path and let downstream handle the error
        return trajectory_path

class CalculationPlanner:
    """Plans and manages calculation resources"""

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.logger = logging.getLogger(f'{__name__}.{session_id}')

    def get_plot_dependencies(self) -> Dict[str, List[str]]:
        """Simple dependency mapping - which plots need which metrics computed first"""
        return {
            'LANDSCAPE': ['RMSD', 'ERMSD'],  # Landscape needs both RMSD and ERMSD
            'RADIUS_OF_GYRATION': ['METRICS_PHASE'],  # RG plot needs metrics phase complete
            'END_TO_END_DISTANCE': ['METRICS_PHASE'],  # E2E plot needs metrics phase complete  
            'PCA': ['METRICS_PHASE'],  # PCA plot needs metrics phase complete
            'UMAP': ['METRICS_PHASE'],  # UMAP plot needs metrics phase complete
            'TSNE': ['METRICS_PHASE'],  # t-SNE plot needs metrics phase complete
            # Add other dependencies as needed
        }

    def get_shared_computations(self, selected_plots: List[str]) -> Dict[str, List[str]]:
        """Identify computations that can be shared between plots"""
        shared_computations = {}

        # Map computation types to plots that need them
        # Simple mapping based on plot types
        plot_to_computation = {
            'RMSD': 'rmsd',
            'ERMSD': 'ermsd', 
            'TORSION': 'torsion',
            'SEC_STRUCTURE': 'annotate',
            'DOTBRACKET': 'annotate',
            'ARC': 'annotate',
            'CONTACT_MAPS': 'annotate',
            'ANNOTATE': 'annotate',
            'DS_MOTIF': 'motif',
            'SS_MOTIF': 'motif',
            'JCOUPLING': 'jcoupling',
            'ESCORE': 'escore',
            'LANDSCAPE': 'landscape',
            'BASE_PAIRING': 'base_pairing'
        }

        computation_to_plots = {}
        for plot in selected_plots:
            if plot in plot_to_computation:
                calc_type = plot_to_computation[plot]
                if calc_type not in computation_to_plots:
                    computation_to_plots[calc_type] = []
                computation_to_plots[calc_type].append(plot)

        # Identify shared computations (needed by multiple plots)
        for calc_type, plots in computation_to_plots.items():
            if len(plots) > 1:
                shared_computations[calc_type] = plots

        return shared_computations

    def build_execution_tree(self, selected_plots: List[str]) -> Dict:
        """Build execution tree showing dependencies"""
        dependencies = self.get_plot_dependencies()
        
        # Separate plots into those with dependencies and those without
        independent_plots = []
        dependent_plots = []
        required_metrics = set()
        
        for plot in selected_plots:
            if plot in dependencies:
                dependent_plots.append(plot)
                required_metrics.update(dependencies[plot])
            else:
                independent_plots.append(plot)
        
        # Find which metrics are actually needed (some might already be in selected_plots)
        metrics_to_compute = []
        for metric in required_metrics:
            if metric in selected_plots:
                metrics_to_compute.append(metric)
            elif metric == 'METRICS_PHASE':
                # Special case: plots that depend on metrics computation phase
                # These will be handled in phase 3 (dependent plots)
                pass
        
        execution_tree = {
            'phase_1_metrics': metrics_to_compute,
            'phase_2_independent': [p for p in independent_plots if p not in metrics_to_compute],
            'phase_3_dependent': dependent_plots
        }
        
        return execution_tree


    
socketio = SocketIO(
    app,
    logger=False,
    engineio_logger=False,
    async_mode='eventlet' if EVENTLET_AVAILABLE else 'threading',
    cors_allowed_origins=[
        "https://arny-plotter.rpbs.univ-paris-diderot.fr",
        "http://arny-plotter.rpbs.univ-paris-diderot.fr",
        "http://localhost:4242",
        "http://127.0.0.1:4242",
        "http://172.27.7.130:4242"
    ],

    cors_credentials=True,     # Allow credentials in CORS requests
    allow_upgrades=True,       # Allow protocol upgrades
    transports=['websocket', 'polling'],  # Support both transport methods

    ping_timeout=30,           # Reduced timeout for faster detection
    ping_interval=15,          # Less frequent pings to reduce overhead

    engineio_options={
        'ping_timeout': 30,
        'ping_interval': 15,
        'upgrade_timeout': 30,
        'max_http_buffer_size': 1000000,
        'cors_allowed_origins': [
            "https://arny-plotter.rpbs.univ-paris-diderot.fr",
            "http://arny-plotter.rpbs.univ-paris-diderot.fr",
            "http://localhost:4242",
            "http://127.0.0.1:4242",
            "http://172.27.7.130:4242"
        ],
        'cors_credentials': True,
    }
)
redis_conn = Redis()
plot_queue = Queue('plot_queue', connection=redis_conn)


def emit_error(session_id: str, message: str, error_type: str = 'error'):
    """
    Emit an error message to the frontend terminal.

    Args:
        session_id: The session ID to emit to
        message: The error message to display
        error_type: Type of error ('error', 'warning', 'network', 'processing')
    """
    if session_id:
        socketio.emit('update_error', {
            'message': message,
            'error_type': error_type
        }, to=session_id)
        logger.error(f"[Session {session_id}] {error_type.upper()}: {message}")


def emit_progress_with_error_check(session_id: str, progress: int, message: str, is_error: bool = False, error_type: str = 'error'):
    """
    Emit progress update, with optional error flagging.

    Args:
        session_id: The session ID to emit to
        progress: Progress percentage (0-100)
        message: The message to display
        is_error: If True, also emit as error
        error_type: Type of error if is_error is True
    """
    socketio.emit('update_progress', {"progress": progress, "message": message}, to=session_id)
    if is_error:
        emit_error(session_id, message, error_type)


@socketio.on("connect")
def handle_connect():
    session_id = request.args.get("session_id")
    if session_id:
        join_room(session_id)
        socketio.emit(
            "command_output",
            {"output": "Connected to room: " + session_id},
            to=session_id,
        )
    print("Client connected")

def create_session_id():
    return uuid.uuid4().hex

def schedule_session_cleanup(session_id):
    """
    Schedule automatic deletion of session folder after one month.

    Args:
        session_id (str): The session ID to schedule for deletion
    """
    try:
        # Calculate delay for one month (30 days)
        cleanup_time = datetime.utcnow() + timedelta(days=30)

        # Schedule the cleanup task to run in one month
        delete_session_folder.apply_async(
            args=[session_id, uploads_dir],
            eta=cleanup_time
        )

        logger.info(f"Session {session_id} scheduled for cleanup on {cleanup_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    except Exception as e:
        logger.warning(f"Failed to schedule cleanup for session {session_id}: {str(e)}")

def parse_topology_file(file_path):
    """Parse topology file and extract residue information for torsion angle calculation"""
    try:
        u = mda.Universe(file_path)
        residues_info = []
        
        for residue in u.residues:
            res_info = {
                'index': int(residue.resindex),  # Convert numpy int64 to Python int
                'name': str(residue.resname),    # Ensure string type
                'id': int(residue.resid),        # Convert numpy int64 to Python int
                'full_name': f"{residue.resname}{residue.resid}"
            }
            residues_info.append(res_info)
        
        # Standard RNA torsion angles
        torsion_angles = [
            {'name': 'alpha', 'description': 'Alpha (O3\'-P-O5\'-C5\')'},
            {'name': 'beta', 'description': 'Beta (P-O5\'-C5\'-C4\')'},
            {'name': 'gamma', 'description': 'Gamma (O5\'-C5\'-C4\'-C3\')'},
            {'name': 'delta', 'description': 'Delta (C5\'-C4\'-C3\'-O3\')'},
            {'name': 'epsilon', 'description': 'Epsilon (C4\'-C3\'-O3\'-P)'},
            {'name': 'zeta', 'description': 'Zeta (C3\'-O3\'-P-O5\')'},
            {'name': 'chi', 'description': 'Chi (O4\'-C1\'-N1-C2/C4)'}
        ]
        
        return {
            'residues': residues_info,
            'torsion_angles': torsion_angles,
            'num_residues': len(residues_info),
            'sequence': [str(res.resname) for res in u.residues]  # Ensure strings
        }
    except Exception as e:
        logger.error(f"Error parsing topology file {file_path}: {str(e)}")
        return None


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        directory = request.form["directory"]
        session_id = request.form["session_id"]
        # Perform actions based on the form input, e.g., list files
        files = list_files(directory)
        return render_template("results.html", files=files, session_id=session_id)
    
    # Check if there's an existing session with results
    existing_session_id = session.get("session_id")
    if existing_session_id:
        session_path = os.path.join(uploads_dir, existing_session_id)
        if os.path.isdir(session_path):
            # Check if there are any results in the session directory
            plot_files = glob.glob(os.path.join(session_path, "plots", "*"))
            if plot_files:
                # Redirect to retrieve-results page
                return redirect(url_for('retrieve_results', session_id=existing_session_id))
    
    # Create new session if no existing session with results
    session_id = uuid.uuid4().hex
    session["session_id"] = session_id

    # Schedule automatic cleanup after one month
    schedule_session_cleanup(session_id)

    return render_template("index.html", session_id=session_id)

@app.route("/get_session", methods=["GET"])
def get_session():
    session["session_id"] = uuid.uuid4().hex

    session_id = session["session_id"]
    session_path = os.path.join(uploads_dir, session_id)

    # Check if the session_id exists as a directory in /static/uploads
    session_exists = os.path.isdir(session_path)

    return jsonify({"session_id": session_id, "exists": session_exists})

@app.route("/parse-topology", methods=["POST"])
def parse_topology():
    """Parse uploaded topology file and return residue information"""
    if "topologyFile" not in request.files:
        return jsonify({"error": "No topology file provided"}), 400
    
    topology_file = request.files["topologyFile"]
    if topology_file.filename == "":
        return jsonify({"error": "No file selected"}), 400
    
    session_id = request.form.get("session_id")
    if not session_id:
        return jsonify({"error": "No session ID provided"}), 400

    # Create session directory if it doesn't exist
    session_dir = os.path.join(uploads_dir, session_id)
    os.makedirs(session_dir, exist_ok=True)
    
    # Save the file temporarily for parsing
    temp_filename = secure_filename(topology_file.filename)
    temp_path = os.path.join(session_dir, f"temp_{temp_filename}")
    
    try:
        topology_file.save(temp_path)
        
        # Parse the file
        topology_info = parse_topology_file(temp_path)
        
        if topology_info is None:
            return jsonify({"error": "Failed to parse topology file"}), 500
        
        return jsonify({
            "success": True,
            "residues": topology_info["residues"],
            "torsion_angles": topology_info["torsion_angles"],
            "num_residues": topology_info["num_residues"],
            "sequence": topology_info["sequence"]
        })
        
    except Exception as e:
        logger.error(f"Error parsing topology file: {str(e)}")
        return jsonify({"error": f"Error parsing file: {str(e)}"}), 500
    finally:
        # Clean up temporary file
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass

@app.route("/cgu")
def cgu():
    return render_template("cgu.html")

@app.route("/authors")
def authors():
    return render_template("authors.html")

@app.route("/documentation")
def documentation():
    return render_template("documentation.html")

@app.route("/tools")
def tools():
    scripts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts/tools'))
    scripts = sorted([f for f in os.listdir(scripts_dir) if f.endswith('.py')]) if os.path.isdir(scripts_dir) else []
    return render_template("tools.html", scripts=scripts)

@app.route("/tools/download/<filename>")
def download_tool(filename):
    scripts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts/tools'))
    if not filename.endswith('.py'):
        return "Invalid file", 400
    return send_from_directory(scripts_dir, filename, as_attachment=True)

@app.route("/cache-debug")
def cache_debug():
    return render_template("cache-debug.html")

@app.route("/simple-test")
def simple_test():
    return render_template("simple-test.html")


@app.route("/benchmark-performance/<session_id>", methods=['POST'])
def benchmark_performance_route(session_id):
    """Run performance benchmark for different optimization strategies"""
    try:
        # Get file paths from session
        directory_path = os.path.join(uploads_dir, session_id)
        
        with open(os.path.join(directory_path, "session_data.json"), "r") as file:
            session_data = json.load(file)
        
        native_pdb = session_data['files']['nativePdb']
        traj_xtc = session_data['files']['trajXtc']
        
        native_pdb_path = os.path.join(directory_path, native_pdb)
        traj_xtc_path = os.path.join(directory_path, traj_xtc)
        
        # Start benchmark task
        benchmark_job = performance_benchmark.apply_async(
            args=[native_pdb_path, traj_xtc_path, session_id]
        )
        
        # Wait for results (with timeout)
        try:
            results = benchmark_job.get(timeout=30000)  # 8.3 hours timeout
            return jsonify({
                'success': True,
                'benchmark_results': results,
                'session_id': session_id
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': f'Benchmark timeout or failed: {str(e)}',
                'task_id': benchmark_job.id
            }), 500
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route("/documentationll")
def documentation2():
    return render_template("documentationll.html")

def list_files(directory):
    # Your implementation of listing files
    return os.listdir(directory)  # Simplified example




def plotly_to_json(fig):
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

@app.route("/upload-chunk", methods=["POST"])
def upload_chunk():
    """Handle chunked file uploads with progress tracking"""
    try:
        # Get chunk metadata
        session_id = request.form.get("session_id")
        file_type = request.form.get("file_type")  # 'topology' or 'trajectory'
        chunk_index = int(request.form.get("chunk_index"))
        total_chunks = int(request.form.get("total_chunks"))
        original_filename = request.form.get("original_filename")

        if not all([session_id, file_type, original_filename]):
            return jsonify({"error": "Missing required parameters"}), 400

        # Create session directory
        session_dir = os.path.join(uploads_dir, session_id)
        os.makedirs(session_dir, exist_ok=True)

        # Get chunk data
        chunk_data = request.files.get("chunk")
        if not chunk_data:
            return jsonify({"error": "No chunk data received"}), 400

        # Temporary file path for chunks
        temp_filename = f"temp_{file_type}_{secure_filename(original_filename)}"
        temp_path = os.path.join(session_dir, temp_filename)

        # Append chunk to file
        with open(temp_path, "ab") as f:
            chunk_data.save(f)

        # Calculate progress
        progress = ((chunk_index + 1) / total_chunks) * 100

        # Emit progress via SocketIO
        socketio.emit('upload_progress', {
            'file_type': file_type,
            'progress': progress,
            'chunk_index': chunk_index + 1,
            'total_chunks': total_chunks,
            'filename': original_filename
        }, to=session_id)

        # If this is the last chunk, rename to final filename
        if chunk_index + 1 == total_chunks:
            final_path = os.path.join(session_dir, secure_filename(original_filename))
            if os.path.exists(final_path):
                os.remove(final_path)
            os.rename(temp_path, final_path)

            return jsonify({
                "success": True,
                "completed": True,
                "progress": 100.0,
                "message": f"Upload complete: {original_filename}",
                "file_path": final_path
            })

        return jsonify({
            "success": True,
            "completed": False,
            "progress": progress
        })

    except Exception as e:
        logger.error(f"Error in chunked upload: {str(e)}")
        if session_id:
            emit_error(session_id, f"Upload failed: {str(e)}", 'error')
        return jsonify({"error": str(e)}), 500

@app.route("/upload-files", methods=["POST"])
def upload_files():
    logger.info("=== Upload Files Endpoint Called ===")
    logger.info(f"Files: {list(request.files.keys())}")
    logger.info(f"Form data: {dict(request.form)}")

    session_id = request.form.get("session_id")
    if not session_id:
        session_id = session.get("session_id")

    logger.info(f"Session_id = {session_id}")
    session_dir = os.path.join(uploads_dir, session_id)
    os.makedirs(session_dir, exist_ok=True)

    # Check if files were already uploaded via chunked upload
    chunked_upload = request.form.get("chunked_upload") == "true"
    logger.info(f"Chunked upload mode: {chunked_upload}")

    if chunked_upload:
        # Files already uploaded via chunks, just get filenames
        native_pdb_filename = request.form.get("nativePdb_filename")
        traj_xtc_filename = request.form.get("trajXtc_filename")
        logger.info(f"Chunked upload filenames: topology={native_pdb_filename}, trajectory={traj_xtc_filename}")

        if not native_pdb_filename or not traj_xtc_filename:
            logger.error("Missing file information for chunked upload")
            emit_error(session_id, "Missing file information for chunked upload. Please try again.", 'error')
            return "Missing file information for chunked upload", 400

        native_pdb_path = os.path.join(session_dir, secure_filename(native_pdb_filename))
        traj_xtc_path = os.path.join(session_dir, secure_filename(traj_xtc_filename))
        logger.info(f"Expected file paths: topology={native_pdb_path}, trajectory={traj_xtc_path}")

        # Verify files exist
        topology_exists = os.path.exists(native_pdb_path)
        trajectory_exists = os.path.exists(traj_xtc_path)
        logger.info(f"File existence check: topology={topology_exists}, trajectory={trajectory_exists}")

        if not topology_exists or not trajectory_exists:
            error_msg = f"Uploaded files not found after upload. Topology: {topology_exists}, Trajectory: {trajectory_exists}"
            logger.error(error_msg)
            emit_error(session_id, error_msg, 'error')
            return error_msg, 404

        native_pdb_name = native_pdb_filename
        traj_xtc_name = traj_xtc_filename
        logger.info(f"Using chunked upload files: {native_pdb_name}, {traj_xtc_name}")

    else:
        if "nativePdb" not in request.files or "trajXtc" not in request.files:
            return redirect(request.url)

        native_pdb = request.files["nativePdb"]
        traj_xtc = request.files["trajXtc"]

        if native_pdb.filename == "" or traj_xtc.filename == "":
            return redirect(request.url)

        native_pdb_path = os.path.join(session_dir, secure_filename(native_pdb.filename))
        traj_xtc_path = os.path.join(session_dir, secure_filename(traj_xtc.filename))
        print(traj_xtc_path)

        try:
            native_pdb.save(native_pdb_path)
            traj_xtc.save(traj_xtc_path)
        except Exception as e:
            app.logger.error(f"Error saving files: {e}")
            return "Error saving files.", 500

        native_pdb_name = native_pdb.filename
        traj_xtc_name = traj_xtc.filename

    selected_plots = []
    n_frames = int(request.form.get("n_frames", 1))  # Default to 1 if not specified
    first_frame = request.form.get("firstFrame", "")
    last_frame = request.form.get("lastFrame", "")
    stride = request.form.get("stride", "")
    frame_range = "all" if not (first_frame and last_frame and stride) else f"{first_frame}-{last_frame}:{stride}"

    for plot in [
        "RMSD",
        "ERMSD",
        "CONTACT_MAPS",
        "TORSION",
        "SEC_STRUCTURE",
        "DOTBRACKET",
        "ARC",
        "ANNOTATE",
        "DS_MOTIF",
        "SS_MOTIF",
        "JCOUPLING",
        "ESCORE",
        "LANDSCAPE",
        "BASE_PAIRING",
        "RADIUS_OF_GYRATION",
        "END_TO_END_DISTANCE",
        "PCA",
        "UMAP",
        "TSNE",
    ]:  # Adjust based on your available plots
        app.logger.info(plot)
        print(str(plot.lower()))
        if plot.lower() in request.form:
            app.logger.info(request.form)
            app.logger.info("In the Request part of the code")
            print(plot)
            selected_plots.append(plot)

    # Handle torsion angle parameters
    torsion_residues = request.form.getlist("torsionResidues")
    torsion_angles = request.form.getlist("torsionAngles")
    torsion_mode = request.form.get("torsionMode", "single")  # single, multiple, all
    
    # Handle plot settings
    plot_settings = {}
    plot_settings_json = request.form.get("plot_settings")
    if plot_settings_json:
        try:
            plot_settings = json.loads(plot_settings_json)
            logger.info(f"Received plot settings: {plot_settings}")
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse plot settings JSON: {e}")
            plot_settings = {}
    
    atom_selection_mode = request.form.get("atom_selection_mode", "none")
    custom_selection = request.form.get("custom_selection", "").strip()

    session_data = {
        "selected_plots": selected_plots,
        "n_frames": n_frames,
        "frame_range": frame_range,
        "torsionResidue": request.form.get("torsionResidue", 0),
        "torsionResidues": torsion_residues,
        "torsionAngles": torsion_angles,
        "torsionMode": torsion_mode,
        "landscape_stride": request.form.get("landscape_stride", 0),
        "landscape_first_component": convert_dimension_to_numeric(request.form.get("firstDimension", "RMSD")),
        "landscape_second_component": convert_dimension_to_numeric(request.form.get("secondDimension", "eRMSD")),
        "plot_settings": plot_settings,
        "atom_selection_mode": atom_selection_mode,
        "custom_selection": custom_selection,
        "form": list(request.form),
        "files": {
            "nativePdb": str(native_pdb_name),
            "trajXtc": str(traj_xtc_name),
            "trajpdb": str(traj_xtc_name)
        }
    }

    session.update(session_data)

    session_file_path = os.path.join(session_dir, "session_data.json")
    with open(session_file_path, "w") as session_file:
        json.dump(session_data, session_file)

    app.logger.info(f"Selected plots: {selected_plots}")

    redirect_url = url_for(
        "view_trajectory",
        session_id=session_id
    )
    logger.info(f"Redirecting to: {redirect_url}")

    return redirect(redirect_url)


@app.route("/retrieve-results", methods=["GET"])
def retrieve_results():
    session_id = request.args.get("session_id")
    session_log_handler = setup_session_logger(session_id)
    try:
        return _retrieve_results_impl(session_id)
    finally:
        remove_session_logger(session_log_handler)


def _retrieve_results_impl(session_id):
    logger.info("Retrieving results")

    directory_path = os.path.join(uploads_dir, session_id)
    pickle_file_path = os.path.join(directory_path, "plot_data.pkl")

    with open(os.path.join(directory_path, "session_data.json"), "r") as file:
        session_data = json.load(file)
        print(session_data)

    native_pdb = session_data['files']['nativePdb']
    traj_xtc = session_data['files']['trajXtc']


    native_pdb_path = os.path.join(directory_path, native_pdb)
    traj_xtc_path = os.path.join(directory_path, traj_xtc)
    print(f"Native PDB: {native_pdb_path}")
    print(f"Trajectory XTC: {traj_xtc_path}")

    # Check if topology needs conversion to PDB format (for retrieve results)
    original_topo_path = native_pdb_path
    native_pdb_path = convert_topology_to_pdb(native_pdb_path, traj_xtc_path, session_id)

    if native_pdb_path != original_topo_path:
        converted_filename = os.path.basename(native_pdb_path)
        session_data['files']['nativePdb'] = converted_filename
        native_pdb = converted_filename
        with open(os.path.join(directory_path, "session_data.json"), "w") as file:
            json.dump(session_data, file, indent=4)
        logger.info(f"Updated retrieve results session data with converted topology: {converted_filename}")

    # Check if trajectory needs conversion to XTC format (for retrieve results)
    original_traj_path = traj_xtc_path
    traj_xtc_path = convert_trajectory_to_xtc(native_pdb_path, traj_xtc_path, session_id)

    # If conversion happened, update the session data
    if traj_xtc_path != original_traj_path:
        converted_filename = os.path.basename(traj_xtc_path)
        session_data['files']['trajXtc'] = converted_filename
        session_data['trajectory_converted'] = True  # Flag to bypass cache
        traj_xtc = converted_filename
        # Update session data file
        with open(os.path.join(directory_path, "session_data.json"), "w") as file:
            json.dump(session_data, file, indent=4)
        logger.info(f"Updated retrieve results session data with converted trajectory: {converted_filename}")

    u = mda.Universe(native_pdb_path, traj_xtc_path)

    if not os.path.exists(pickle_file_path):
        return "Session results not found", 404

    with open(pickle_file_path, "rb") as f:
        plot_data = pickle.load(f)

    with open(os.path.join(app.static_folder,'explanations.json')) as f:
        explanations = json.load(f)

    print(explanations)

    return render_template(
        "view_trajectory.html",
        session_id=session_id,
        native_pdb=native_pdb,
        traj_xtc=traj_xtc,
        plot_data=plot_data,
        trajectory_length=len(u.trajectory),
        explainations=explanations,
        trajectory_converted=session_data.get('trajectory_converted', False)
    )


@app.route("/download/all/<session_id>")
def download_all(session_id):
    """Bundle every data file and plot for a session into a single zip."""
    directory_path = os.path.join(uploads_dir, session_id)
    if not os.path.isdir(directory_path):
        return jsonify({"error": "Session not found"}), 404

    memory_file = io.BytesIO()
    added = 0

    with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as zf:
        # Data files  →  data/<PLOT>/<filename>
        data_root = os.path.join(directory_path, "download_data")
        if os.path.isdir(data_root):
            for plot_dir in sorted(os.listdir(data_root)):
                plot_path = os.path.join(data_root, plot_dir)
                if not os.path.isdir(plot_path):
                    continue
                for fname in sorted(os.listdir(plot_path)):
                    fpath = os.path.join(plot_path, fname)
                    if os.path.isfile(fpath):
                        zf.write(fpath, arcname=os.path.join("data", plot_dir, fname))
                        added += 1

        # Plot files  →  plots/<PLOT>/<filename>
        plot_root = os.path.join(directory_path, "download_plot")
        if os.path.isdir(plot_root):
            for plot_dir in sorted(os.listdir(plot_root)):
                plot_path = os.path.join(plot_root, plot_dir)
                if not os.path.isdir(plot_path):
                    continue
                for fname in sorted(os.listdir(plot_path)):
                    fpath = os.path.join(plot_path, fname)
                    if os.path.isfile(fpath):
                        zf.write(fpath, arcname=os.path.join("plots", plot_dir, fname))
                        added += 1

    if added == 0:
        return jsonify({"error": "No results available yet for this session"}), 404

    memory_file.seek(0)
    download_name = f"arny_results_{session_id[:12]}.zip"
    return send_file(memory_file, download_name=download_name, as_attachment=True,
                     mimetype="application/zip")


@app.route('/status/<session_id>')
def session_status_page(session_id):
    """Renders the status dashboard for a session (opens in a new tab)."""
    return render_template('session_status.html', session_id=session_id)


@app.route('/api/session-status/<session_id>')
def session_status_api(session_id):
    """Returns JSON status of all calculations for a session."""
    directory_path = os.path.join(uploads_dir, session_id)

    if not os.path.isdir(directory_path):
        return jsonify({'error': 'Session not found'}), 404

    # Load session data
    session_data = {}
    session_data_path = os.path.join(directory_path, 'session_data.json')
    if os.path.exists(session_data_path):
        try:
            with open(session_data_path) as f:
                session_data = json.load(f)
        except Exception:
            pass

    selected_plots = session_data.get('selected_plots', [])
    overall_done = os.path.exists(os.path.join(directory_path, 'plot_data.pkl'))

    # Determine per-plot completion by checking output files
    download_data_dir = os.path.join(directory_path, 'download_data')
    plots_status = []
    for plot in selected_plots:
        plot_dir = os.path.join(download_data_dir, plot)
        if overall_done or (os.path.isdir(plot_dir) and glob.glob(os.path.join(plot_dir, '*'))):
            status = 'done'
        else:
            status = 'processing'
        plots_status.append({'name': plot, 'status': status})

    # Read the last 200 lines of the session log
    log_lines = []
    log_path = os.path.join(directory_path, 'session.log')
    if os.path.exists(log_path):
        try:
            with open(log_path) as f:
                log_lines = f.readlines()[-200:]
        except Exception:
            pass

    return jsonify({
        'session_id': session_id,
        'overall_done': overall_done,
        'plots': plots_status,
        'log': ''.join(log_lines),
        'n_frames': session_data.get('n_frames', 0),
        'frame_range': session_data.get('frame_range', 'all'),
    })


@app.route("/download/plot_data/<session_id>/<plot_id>")
def download_plot_data(plot_id, session_id):
    directory_path = os.path.join(uploads_dir, session_id)
    download_path = os.path.join(directory_path, "download_data")
    files_path = os.path.join(download_path, plot_id)
    print(f"Download path: {files_path}")
    file_paths = glob.glob(f"{files_path}/*")
    print(file_paths)

    if len(file_paths) > 1:
        memory_file = io.BytesIO()
        download_filename = f"{plot_id}_data.zip"
        with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in file_paths:
                with open(file_path, "rb") as f:
                    # Use the file name as the arcname
                    zf.writestr(file_path.split("/")[-1], f.read())
    else:
        memory_file = io.BytesIO()
        with open(file_paths[0], "rb") as f:
            memory_file.write(f.read())
        download_filename = os.path.basename(file_paths[0])

    memory_file.seek(0)

    # Send the zip file as a response
    return send_file(memory_file, download_name=download_filename, as_attachment=True)


@app.route("/download/plot/<session_id>/<plot_id>")
def download_plot(plot_id, session_id):
    directory_path = os.path.join(uploads_dir, session_id)
    download_path = os.path.join(directory_path, "download_plot")
    files_path = os.path.join(download_path, plot_id)
    print(f"Download path: {files_path}")
    file_paths = glob.glob(f"{files_path}/*")
    print(file_paths)

    if len(file_paths) > 1:
        memory_file = io.BytesIO()
        download_filename = f"{plot_id}_plots.zip"
        with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in file_paths:
                with open(file_path, "rb") as f:
                    # Use the file name as the arcname
                    zf.writestr(file_path.split("/")[-1], f.read())
    else:
        memory_file = io.BytesIO()
        with open(file_paths[0], "rb") as f:
            memory_file.write(f.read())
        download_filename = os.path.basename(file_paths[0])

    memory_file.seek(0)

    # Send the zip file as a response
    return send_file(memory_file, download_name=download_filename, as_attachment=True)


@app.route("/download/frame/<session_id>/<int:frame_number>", methods=["GET"])
def download_frame(session_id, frame_number):
    """
    Extract and download a specific frame from the trajectory as a PDB file.

    Args:
        session_id: The session ID
        frame_number: The frame index to extract (0-based)

    Returns:
        PDB file as download
    """
    from src.core.tasks import extract_frame_as_pdb

    logger.info(f"Download frame request: session={session_id}, frame={frame_number}")

    # Get session data to find trajectory files
    directory_path = os.path.join(uploads_dir, session_id)
    session_data_path = os.path.join(directory_path, "session_data.json")

    if not os.path.exists(session_data_path):
        emit_error(session_id, f"Session data not found for session {session_id}", 'error')
        return jsonify({"error": "Session not found"}), 404

    try:
        with open(session_data_path, "r") as f:
            session_data = json.load(f)

        native_pdb = session_data['files']['nativePdb']
        traj_xtc = session_data['files']['trajXtc']

        native_pdb_path = os.path.join(directory_path, native_pdb)
        traj_xtc_path = os.path.join(directory_path, traj_xtc)

        # Check if files exist
        if not os.path.exists(native_pdb_path) or not os.path.exists(traj_xtc_path):
            emit_error(session_id, "Trajectory files not found", 'error')
            return jsonify({"error": "Trajectory files not found"}), 404

        # Run the Celery task synchronously (blocking)
        result = extract_frame_as_pdb.apply_async(
            args=[native_pdb_path, traj_xtc_path, frame_number, session_id]
        )

        # Wait for result with timeout
        try:
            task_result = result.get(timeout=30)
        except Exception as e:
            logger.error(f"Frame extraction task failed: {e}")
            emit_error(session_id, f"Failed to extract frame: {str(e)}", 'processing')
            return jsonify({"error": f"Failed to extract frame: {str(e)}"}), 500

        if task_result.get("status") == "success":
            pdb_path = "../../"+task_result["path"]
            filename = task_result["filename"]
            # if os.path.exists(pdb_path):
            return send_file(
                pdb_path,
                download_name=filename,
                as_attachment=True,
                mimetype='chemical/x-pdb'
            )
            # else:
                # emit_error(session_id, "Generated PDB file not found", 'error')
                # return jsonify({"error": "Generated PDB file not found"}), 500
        else:
            emit_error(session_id, "Frame extraction failed", 'processing')
            return jsonify({"error": "Frame extraction failed"}), 500

    except Exception as e:
        logger.error(f"Error downloading frame: {e}")
        emit_error(session_id, f"Error downloading frame: {str(e)}", 'error')
        return jsonify({"error": str(e)}), 500


@app.route('/view-trajectory/<session_id>')
def view_trajectory(session_id):
    start_time = time.time()
    session_log_handler = setup_session_logger(session_id)
    try:
        return _view_trajectory_impl(session_id, start_time)
    finally:
        remove_session_logger(session_log_handler)


def _view_trajectory_impl(session_id, start_time):
    # Check if results already exist (from a previous run)
    directory_path = os.path.join(uploads_dir, session_id)
    pickle_file_path = os.path.join(directory_path, "plot_data.pkl")

    if os.path.exists(pickle_file_path):
        logger.info(f"Results already exist for session {session_id}, loading from cache")
        # Results exist, just load and display them
        with open(os.path.join(directory_path, "session_data.json"), "r") as file:
            session_data = json.load(file)

        native_pdb_cached = session_data['files']['nativePdb']
        traj_xtc_cached = session_data['files']['trajXtc']

        native_pdb_path = os.path.join(directory_path, native_pdb_cached)
        traj_xtc_path = os.path.join(directory_path, traj_xtc_cached)

        # Check if topology needs conversion to PDB format (even for cached results)
        original_topo_path = native_pdb_path
        native_pdb_path = convert_topology_to_pdb(native_pdb_path, traj_xtc_path, session_id)

        if native_pdb_path != original_topo_path:
            converted_filename = os.path.basename(native_pdb_path)
            session_data['files']['nativePdb'] = converted_filename
            native_pdb_cached = converted_filename
            with open(os.path.join(directory_path, "session_data.json"), "w") as file:
                json.dump(session_data, file, indent=4)
            logger.info(f"Updated cached session data with converted topology: {converted_filename}")

        # Check if trajectory needs conversion to XTC format (even for cached results)
        original_traj_path = traj_xtc_path
        traj_xtc_path = convert_trajectory_to_xtc(native_pdb_path, traj_xtc_path, session_id)

        # If conversion happened, update the session data
        if traj_xtc_path != original_traj_path:
            converted_filename = os.path.basename(traj_xtc_path)
            session_data['files']['trajXtc'] = converted_filename
            session_data['trajectory_converted'] = True  # Flag to bypass cache
            traj_xtc_cached = converted_filename
            # Update session data file
            with open(os.path.join(directory_path, "session_data.json"), "w") as file:
                json.dump(session_data, file, indent=4)
            logger.info(f"Updated cached session data with converted trajectory: {converted_filename}")

        u = mda.Universe(native_pdb_path, traj_xtc_path)

        with open(pickle_file_path, "rb") as f:
            plot_data = pickle.load(f)

        with open(os.path.join(app.static_folder, 'explanations.json')) as f:
            explanations = json.load(f)

        return render_template(
            "view_trajectory.html",
            session_id=session_id,
            native_pdb=native_pdb_cached,
            traj_xtc=traj_xtc_cached,
            plot_data=plot_data,
            trajectory_length=len(u.trajectory),
            explainations=explanations,
            trajectory_converted=session_data.get('trajectory_converted', False)
        )

    # Results don't exist, need to process
    socketio.emit('update_progress', {"progress": 0, "message": "Initializing trajectory analysis..."}, to=session_id)
    socketio.sleep(0.01)  # Minimal delay for message sending

    # Setup paths and load explanations
    directory_path = os.path.join(uploads_dir, session_id)
    download_path = os.path.join(directory_path, "download_data")
    download_plot = os.path.join(directory_path, "download_plot")
    generate_data_path = os.path.join(directory_path, "generated_data")

    # Create directories if they don't exist
    os.makedirs(download_path, exist_ok=True)
    os.makedirs(download_plot, exist_ok=True)
    os.makedirs(generate_data_path, exist_ok=True)

    with open(os.path.join(app.static_folder, 'explanations.json')) as f:
        explanations = json.load(f)

    socketio.emit('update_progress', {"progress": 10, "message": "Paths set up and explanations loaded."}, to=session_id)
    socketio.sleep(0.01)  # Minimal delay for message sending

    # Load session data to get file names
    try:
        with open(os.path.join(directory_path, "session_data.json"), "r") as file:
            session_data_json = json.load(file)

        native_pdb = session_data_json['files']['nativePdb']
        traj_xtc = session_data_json['files']['trajXtc']
    except FileNotFoundError as e:
        error_msg = f"Session data file not found. Please re-upload your files."
        emit_error(session_id, error_msg, 'error')
        socketio.emit('update_progress', {"progress": 100, "message": error_msg}, to=session_id)
        flash(error_msg, 'error')
        socketio.sleep(3)
        return redirect(url_for('index'))
    except KeyError as e:
        error_msg = f"Session data is corrupted or incomplete. Missing key: {e}"
        emit_error(session_id, error_msg, 'error')
        socketio.emit('update_progress', {"progress": 100, "message": error_msg}, to=session_id)
        flash(error_msg, 'error')
        socketio.sleep(3)
        return redirect(url_for('index'))
    except json.JSONDecodeError as e:
        error_msg = f"Session data file is corrupted: {e}"
        emit_error(session_id, error_msg, 'error')
        socketio.emit('update_progress', {"progress": 100, "message": error_msg}, to=session_id)
        flash(error_msg, 'error')
        socketio.sleep(3)
        return redirect(url_for('index'))

    # Validate paths and session data
    native_pdb_path = os.path.join(directory_path, native_pdb)
    traj_xtc_path = os.path.join(directory_path, traj_xtc)
    if not os.path.exists(native_pdb_path):
        error_msg = f"Topology file not found: {native_pdb}. Please re-upload your files."
        emit_error(session_id, error_msg, 'error')
        socketio.emit('update_progress', {"progress": 100, "message": error_msg}, to=session_id)
        flash(error_msg, 'error')
        socketio.sleep(3)
        return redirect(url_for('index'))
    if not os.path.exists(traj_xtc_path):
        error_msg = f"Trajectory file not found: {traj_xtc}. Please re-upload your files."
        emit_error(session_id, error_msg, 'error')
        socketio.emit('update_progress', {"progress": 100, "message": error_msg}, to=session_id)
        flash(error_msg, 'error')
        socketio.sleep(3)
        return redirect(url_for('index'))

    # Check if topology/trajectory need format conversion
    socketio.emit('update_progress', {"progress": 15, "message": "Checking file formats..."}, to=session_id)

    original_topo_path = native_pdb_path
    native_pdb_path = convert_topology_to_pdb(native_pdb_path, traj_xtc_path, session_id)

    if native_pdb_path != original_topo_path:
        converted_filename = os.path.basename(native_pdb_path)
        session_data_json['files']['nativePdb'] = converted_filename
        with open(os.path.join(directory_path, "session_data.json"), "w") as file:
            json.dump(session_data_json, file, indent=4)
        logger.info(f"Updated session data with converted topology: {converted_filename}")
        socketio.emit('update_progress', {"progress": 16, "message": "Topology converted to PDB format."}, to=session_id)

    original_traj_path = traj_xtc_path
    traj_xtc_path = convert_trajectory_to_xtc(native_pdb_path, traj_xtc_path, session_id)

    # If conversion happened, update the session data
    if traj_xtc_path != original_traj_path:
        converted_filename = os.path.basename(traj_xtc_path)
        session_data_json['files']['trajXtc'] = converted_filename
        session_data_json['trajectory_converted'] = True  # Flag to bypass cache
        # Update session data file
        with open(os.path.join(directory_path, "session_data.json"), "w") as file:
            json.dump(session_data_json, file, indent=4)
        logger.info(f"Updated session data with converted trajectory: {converted_filename}")
        socketio.emit('update_progress', {"progress": 18, "message": "Trajectory converted to XTC format."}, to=session_id)
    
    selected_plots = session.get("selected_plots", [])
    n_frames = session.get("n_frames", 1)
    frame_range = session.get("frame_range", "all")
    plot_settings = session_data_json.get("plot_settings", {})

    socketio.emit('update_progress', {"progress": 20, "message": "Session data validated and trajectory format checked."}, to=session_id)
    socketio.sleep(0.01)

    # Apply atom selection filter (remove waters / heterogens / custom MDAnalysis string)
    atom_selection_mode = session_data_json.get("atom_selection_mode", "none")
    custom_selection    = session_data_json.get("custom_selection", "")

    SELECTION_STRINGS = {
        "remove_waters":     "not (resname WAT SOL HOH TIP3 TIP3P TIP4P SPC SPCE)",
        "remove_heterogens": "protein or nucleic",
        "remove_waters_and_ions": "not (resname WAT SOL HOH TIP3 TIP3P TIP4P SPC SPCE or resname NA CL K CA MG ZN CU FE MN)",
        "nucleic_only": "nucleic",
    }

    mda_selection = None
    if atom_selection_mode == "remove_waters":
        mda_selection = SELECTION_STRINGS["remove_waters"]
    elif atom_selection_mode == "remove_heterogens":
        mda_selection = SELECTION_STRINGS["remove_heterogens"]
    elif atom_selection_mode == "remove_waters_and_ions":
        mda_selection = SELECTION_STRINGS["remove_waters_and_ions"]
    elif atom_selection_mode == "nucleic_only":
        mda_selection = SELECTION_STRINGS["nucleic_only"]
    elif atom_selection_mode == "custom" and custom_selection:
        mda_selection = custom_selection

    if mda_selection:
        socketio.emit('update_progress', {"progress": 22, "message": f"Applying atom selection: {mda_selection}"}, to=session_id)
        try:
            u_raw = mda.Universe(native_pdb_path, traj_xtc_path)
            ag = u_raw.select_atoms(mda_selection)
            if len(ag) == 0:
                emit_error(session_id,
                           f"Atom selection '{mda_selection}' matched 0 atoms — using full structure.",
                           'warning')
            else:
                filtered_pdb  = os.path.join(directory_path, "filtered_topology.pdb")
                filtered_xtc  = os.path.join(directory_path, "filtered_trajectory.xtc")

                # Write filtered topology (first frame as PDB)
                with mda.Writer(filtered_pdb, ag.n_atoms) as W:
                    u_raw.trajectory[0]
                    W.write(ag)

                # Write filtered trajectory
                with mda.Writer(filtered_xtc, ag.n_atoms) as W:
                    for ts in u_raw.trajectory:
                        W.write(ag)

                native_pdb_path = filtered_pdb
                traj_xtc_path   = filtered_xtc

                session_data_json['files']['nativePdb'] = "filtered_topology.pdb"
                session_data_json['files']['trajXtc']   = "filtered_trajectory.xtc"
                with open(os.path.join(directory_path, "session_data.json"), "w") as file:
                    json.dump(session_data_json, file, indent=4)

                logger.info(f"Atom selection applied: {len(ag)} atoms kept from {len(u_raw.atoms)}")
                socketio.emit('update_progress', {
                    "progress": 23,
                    "message": f"Selection applied: {len(ag)} atoms kept (from {len(u_raw.atoms)} total)."
                }, to=session_id)
        except Exception as e:
            emit_error(session_id, f"Atom selection failed: {e}. Using full structure.", 'warning')
            logger.warning(f"Atom selection error: {e}")

    # Optimized trajectory loading with parallel preprocessing
    socketio.emit('update_progress', {"progress": 25, "message": "Loading trajectory..."}, to=session_id)
    
    def load_and_preprocess_trajectory():
        """Load trajectory with parallel optimization"""
        u = mda.Universe(native_pdb_path, traj_xtc_path)
        # Load ref with trajectory so topology-only formats (parm7, psf, etc.) get coordinates
        ref = mda.Universe(native_pdb_path, traj_xtc_path)
        ref.trajectory[0]  # Snap to first frame as reference
        
        # Parallel computation of trajectory properties
        def compute_alignment():
            nucleic_backbone = u.select_atoms('nucleicbackbone').positions
            ref_backbone = ref.select_atoms('nucleicbackbone').positions
            return mda.analysis.align.rotation_matrix(nucleic_backbone, ref_backbone)[0]
        
        def extract_residue_info():
            return [residue.resname for residue in u.residues]
        
        # Execute in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            r_matrix_future = executor.submit(compute_alignment)
            residue_future = executor.submit(extract_residue_info)
            
            r_matrix = r_matrix_future.result()
            residue_names = residue_future.result()
        
        return u, ref, r_matrix, residue_names
    
    # Load with optimization
    try:
        u, ref, r_matrix, residue_names = load_and_preprocess_trajectory()
        logger.info(f"Computed the rotation matrix: {r_matrix}")
    except Exception as e:
        error_msg = f"Failed to load trajectory files. "
        if "coordinate" in str(e).lower():
            error_msg += f"Bad coordinates detected: {e}"
            emit_error(session_id, error_msg, 'processing')
        elif "atom" in str(e).lower() or "residue" in str(e).lower():
            error_msg += f"Topology/trajectory mismatch: {e}"
            emit_error(session_id, error_msg, 'processing')
        elif "file" in str(e).lower() or "read" in str(e).lower():
            error_msg += f"File reading error: {e}"
            emit_error(session_id, error_msg, 'error')
        else:
            error_msg += f"Error: {e}"
            emit_error(session_id, error_msg, 'processing')
        socketio.emit('update_progress', {"progress": 100, "message": error_msg}, to=session_id)
        logger.error(f"Trajectory loading failed: {e}")
        flash(error_msg, 'error')
        socketio.sleep(3)
        return redirect(url_for('index'))

    file_name, file_extension = os.path.splitext(traj_xtc_path)
    try:
        r_matrix_str = str(r_matrix.tolist())
    except Exception as e:
        logger.warning(f"Got an issue with r_matrix computation: {e}")
        emit_error(session_id, f"Warning: Could not compute rotation matrix: {e}", 'warning')
        r_matrix_str = "[[1,0,0],[0,1,0],[0,0,1]]"  # Identity matrix fallback     
    logger.info(f"Loaded trajectory: {residue_names}")
    logger.info(f"Nucleic backbone atoms: {len(u.select_atoms('nucleicbackbone').positions)}")
    logger.info(f"Reference backbone atoms: {len(ref.select_atoms('nucleicbackbone').positions)}")
    
    file_name, file_extension = os.path.splitext(traj_xtc_path)
    # Optimized trajectory writing with parallel processing
    def write_trajectory_range():
        if frame_range != "all":
            start, end_stride = frame_range.split("-")
            end, stride = end_stride.split(":")
            start, end, stride = int(start), int(end), int(stride)
            u.trajectory[start:end:stride]
            output_path = os.path.join(directory_path, f"traj_{start}_{end}{file_extension}")
        else:
            output_path = os.path.join(directory_path, f"traj_uploaded{file_extension}")
            start, end, stride = 0, len(u.trajectory), 1
        
        # Parallel trajectory writing
        if frame_range != "all":
            with mda.Writer(output_path, n_atoms=u.atoms.n_atoms) as W:
                for ts in u.trajectory[start:end:stride]:
                    W.write(u)
        else:
            # If frame_range is "all", don't rewrite - just move/rename if needed
            if traj_xtc_path != output_path:
                #import shutil
                # trying symlink
                if os.path.exists(output_path):
                    logger.info(f"Output path {output_path} already exists, using it")
                else:
                    os.symlink(traj_xtc_path, output_path)
                #shutil.move(traj_xtc_path, output_path)

        return output_path

    # Use ThreadPoolExecutor for non-blocking trajectory writing
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            traj_future = executor.submit(write_trajectory_range)
            traj_xtc_path = traj_future.result()
    except Exception as e:
        logger.error(f"Error in trajectory writing: {e}")
        emit_error(session_id, f"Warning: Could not write trajectory subset: {e}. Using original file.", 'warning')
        # Fallback: construct the expected path
        if frame_range != "all":
            start, end_stride = frame_range.split("-")
            end, stride = end_stride.split(":")
            traj_xtc_path = os.path.join(directory_path, f"traj_{start}_{end}{file_extension}")
        else:
            traj_xtc_path = os.path.join(directory_path, f"traj_uploaded{file_extension}")

    socketio.emit('update_progress', {"progress": 40, "message": "Trajectory loaded."}, to=session_id)
    socketio.sleep(0.1)

    # Build execution tree and identify shared computations
    planner = CalculationPlanner(session_id)
    shared_computations = planner.get_shared_computations(selected_plots)
    execution_tree = planner.build_execution_tree(selected_plots)

    # Log the execution tree
    logger.info("=== EXECUTION TREE ===")
    logger.info("Parallel Phase - Required Metrics & Independent Plots (run simultaneously):")
    if execution_tree['phase_1_metrics']:
        logger.info("  Required Metrics:")
        for metric in execution_tree['phase_1_metrics']:
            logger.info(f"    └── {metric}")
    
    if execution_tree['phase_2_independent']:
        logger.info("  Independent Plots:")
        for plot in execution_tree['phase_2_independent']:
            logger.info(f"    └── {plot}")
    
    if execution_tree['phase_3_dependent']:
        logger.info("Sequential Phase - Dependent Plots (wait for required metrics):")
        for plot in execution_tree['phase_3_dependent']:
            deps = planner.get_plot_dependencies().get(plot, [])
            logger.info(f"  └── {plot} (depends on: {deps})")
    
    if shared_computations:
        logger.info("Shared computations identified:")
        for comp_type, plots in shared_computations.items():
            logger.info(f"  └── {comp_type}: {plots}")

    socketio.emit('update_progress', {"progress": 45, "message": "Execution tree built and shared computations identified."}, to=session_id)

    # Phase 1: Compute metrics needed by plots
    metrics_needed = []
    if "RMSD" in selected_plots:
        metrics_needed.append("rmsd")
    if "ERMSD" in selected_plots:
        metrics_needed.append("ermsd")
    if "ANNOTATE" in selected_plots:
        metrics_needed.append("annotate")
    if "RADIUS_OF_GYRATION" in selected_plots:
        metrics_needed.append("radius_of_gyration")
    if "END_TO_END_DISTANCE" in selected_plots:
        metrics_needed.append("end_to_end_distance")
    if any(plot in selected_plots for plot in ["PCA", "UMAP", "TSNE"]):
        metrics_needed.append("dimensionality_reduction")
    
    # Check if landscape plot needs additional metrics based on user selection
    if "LANDSCAPE" in selected_plots:
        landscape_first_component = session.get("landscape_first_component", 1)
        landscape_second_component = session.get("landscape_second_component", 2)
        
        # Add RMSD if component 1 is selected
        if (landscape_first_component == 1 or landscape_second_component == 1) and "rmsd" not in metrics_needed:
            metrics_needed.append("rmsd")
        
        # Add eRMSD if component 2 is selected
        if (landscape_first_component == 2 or landscape_second_component == 2) and "ermsd" not in metrics_needed:
            metrics_needed.append("ermsd")
        
        # Add Q-value if component 3 or 4 is selected
        if (landscape_first_component in [3, 4] or landscape_second_component in [3, 4]) and "q_value" not in metrics_needed:
            metrics_needed.append("q_value")
    
    if metrics_needed:
        socketio.emit('update_progress', {"progress": 50, "message": "Computing metrics..."}, to=session_id)
        
        # Create Celery chain for metrics computation
        metrics_jobs = []
        
        if "rmsd" in metrics_needed:
            metrics_jobs.append(compute_rmsd.s(native_pdb_path, traj_xtc_path, session_id))
        if "ermsd" in metrics_needed:
            metrics_jobs.append(compute_ermsd.s(native_pdb_path, traj_xtc_path, session_id))
        if "annotate" in metrics_needed:
            metrics_jobs.append(compute_annotate.s(native_pdb_path, traj_xtc_path, session_id))
        if "q_value" in metrics_needed:
            metrics_jobs.append(compute_q_value.s(native_pdb_path, traj_xtc_path, session_id))
        if "radius_of_gyration" in metrics_needed:
            metrics_jobs.append(compute_radius_of_gyration.s(native_pdb_path, traj_xtc_path, session_id))
        if "end_to_end_distance" in metrics_needed:
            # Get plot settings for end_to_end_distance
            e2e_settings = session.get("plot_settings", {}).get("end_to_end_distance", {})
            metrics_jobs.append(compute_end_to_end_distance.s(native_pdb_path, traj_xtc_path, session_id, e2e_settings))
        if "dimensionality_reduction" in metrics_needed:
            metrics_jobs.append(compute_dimensionality_reduction.s(native_pdb_path, traj_xtc_path, session_id))
        
        # Execute metrics computation in parallel using group
        if metrics_jobs:
            from celery import group
            metrics_group = group(metrics_jobs)
            metrics_result = metrics_group.apply_async()
            
            # Wait for metrics computation to complete
            while not metrics_result.ready():
                socketio.sleep(0.1)
            
            if metrics_result.successful():
                logger.info("All metrics computed successfully in parallel")
                socketio.emit('update_progress', {"progress": 55, "message": "Metrics computed in parallel."}, to=session_id)
            else:
                logger.error(f"Some metrics computation failed: {metrics_result.results}")
                # Try to get partial results and continue
                successful_results = []
                failed_results = []
                failed_metric_names = []
                metric_names = metrics_needed[:len(metrics_result.results)]  # Map indices to names
                for i, task_result in enumerate(metrics_result.results):
                    try:
                        if task_result.successful():
                            successful_results.append(task_result.get())
                        else:
                            error_detail = str(task_result.result) if hasattr(task_result, 'result') else str(task_result.results)
                            failed_results.append(error_detail)
                            if i < len(metric_names):
                                failed_metric_names.append(metric_names[i])
                    except Exception as e:
                        failed_results.append(str(e))
                        if i < len(metric_names):
                            failed_metric_names.append(metric_names[i])

                # Emit detailed error for failed metrics
                if failed_metric_names:
                    error_msg = f"Failed metrics: {', '.join(failed_metric_names)}. {'; '.join(failed_results[:3])}"
                    emit_error(session_id, error_msg, 'processing')

                logger.info(f"Metrics: {len(successful_results)} successful, {len(failed_results)} failed")
                socketio.emit('update_progress', {"progress": 55, "message": f"Metrics computed: {len(successful_results)} successful, {len(failed_results)} failed"}, to=session_id)

    # Phase 2: Generate plots using execution tree with chord structure
    socketio.emit('update_progress', {"progress": 60, "message": "Generating plots using dependency tree..."}, to=session_id)
    plot_data = []
    
    def create_plot_job(plot):
        """Create a plot job signature"""
        files_path = os.path.join(download_path, plot)
        plot_dir = os.path.join(download_plot, plot)
        os.makedirs(files_path, exist_ok=True)
        os.makedirs(plot_dir, exist_ok=True)

        plot_style = "default"

        # Get plot-specific settings
        plot_settings_for_task = plot_settings.get(plot.lower(), {})
        
        if plot == "RMSD":
            job_sig = generate_rmsd_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, plot_settings_for_task)
            plot_style = "scatter2"
        elif plot == "ERMSD":
            job_sig = generate_ermsd_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, plot_settings_for_task)
            plot_style = "scatter"
        elif plot == "TORSION":
            torsion_residue = session.get("torsionResidue", 0)
            job_sig = generate_torsion_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, torsion_residue, plot_settings_for_task)
            plot_style = "torsion"
        elif plot == "SEC_STRUCTURE":
            job_sig = generate_sec_structure_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, plot_settings_for_task)
            plot_style = "bar"
        elif plot == "DOTBRACKET":
            job_sig = generate_dotbracket_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, plot_settings_for_task)
            plot_style = "dotbracket"
        elif plot == "ARC":
            job_sig = generate_arc_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, plot_settings_for_task)
            plot_style = "arc"
        elif plot == "CONTACT_MAPS":
            job_sig = generate_contact_map_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, generate_data_path, session_id, plot_settings_for_task)
            plot_style = "CONTACT_MAPS"
        elif plot == "ANNOTATE":
            job_sig = generate_annotate_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, plot_settings_for_task)
            plot_style = "annotate"
        elif plot == "DS_MOTIF":
            job_sig = generate_ds_motif_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, plot_settings_for_task)
            plot_style = "motif"
        elif plot == "SS_MOTIF":
            job_sig = generate_ss_motif_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, plot_settings_for_task)
            plot_style = "motif"
        elif plot == "JCOUPLING":
            job_sig = generate_jcoupling_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, plot_settings_for_task)
            plot_style = "s"
        elif plot == "ESCORE":
            job_sig = generate_escore_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, plot_settings_for_task)
            plot_style = "scatter"
        elif plot == "LANDSCAPE":
            landscape_params = [
                session.get("landscape_stride", 1),
                session.get("landscape_first_component", 1),
                session.get("landscape_second_component", 1)
            ]
            job_sig = generate_landscape_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, landscape_params, generate_data_path, plot_settings_for_task)
            plot_style = "surface"
        elif plot == "BASE_PAIRING":
            job_sig = generate_2Dpairing_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, plot_settings_for_task)
            plot_style = "2Dpairing"
        elif plot == "RADIUS_OF_GYRATION":
            job_sig = generate_radius_of_gyration_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, plot_settings_for_task)
            plot_style = "scatter_rog"
        elif plot == "END_TO_END_DISTANCE":
            job_sig = generate_end_to_end_distance_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, plot_settings_for_task)
            plot_style = "scatter_e2e"
        elif plot == "PCA":
            job_sig = generate_dimensionality_reduction_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, 'pca', plot_settings_for_task)
            plot_style = "scatter_PCA"
        elif plot == "UMAP":
            job_sig = generate_dimensionality_reduction_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, 'umap', plot_settings_for_task)
            plot_style = "scatter_UMAP"
        elif plot == "TSNE":
            job_sig = generate_dimensionality_reduction_plot.s(native_pdb_path, traj_xtc_path, files_path, plot_dir, session_id, 'tsne', plot_settings_for_task)
            plot_style = "scatter_TSNE"
        else:
            logger.warning(f"Unknown plot type: {plot}")
            return None

        return job_sig, plot_style
    
    # Execute using optimized parallel structure
    from celery import chord, group
    
    # Phase 1: Execute required metrics AND independent plots in parallel
    phase1_jobs = []  # Required metrics
    phase2_jobs = []  # Independent plots
    
    if execution_tree['phase_1_metrics']:
        logger.info("Phase 1 - Required Metrics (will run in parallel with independent plots)")
        for plot in execution_tree['phase_1_metrics']:
            job_sig, plot_style = create_plot_job(plot)
            if job_sig:
                phase1_jobs.append(job_sig)
                plot_data.append([plot, plot_style, None])
                logger.info(f"  └── {plot} (required metric)")
    
    if execution_tree['phase_2_independent']:
        logger.info("Phase 2 - Independent Plots (will run in parallel with required metrics)")
        for plot in execution_tree['phase_2_independent']:
            job_sig, plot_style = create_plot_job(plot)
            if job_sig:
                phase2_jobs.append(job_sig)
                plot_data.append([plot, plot_style, None])
                logger.info(f"  └── {plot} (independent)")
    
    # Start both required metrics and independent plots at the same time
    phase1_result = None
    phase2_result = None
    
    if phase1_jobs:
        phase1_group = group(phase1_jobs)
        phase1_result = phase1_group.apply_async()
        logger.info("Started required metrics computation")
    
    if phase2_jobs:
        phase2_group = group(phase2_jobs)
        phase2_result = phase2_group.apply_async()
        logger.info("Started independent plots computation")
    
    socketio.emit('update_progress', {"progress": 65, "message": "Computing metrics and independent plots in parallel..."}, to=session_id)
    
    # Wait for required metrics to complete (needed for dependent plots)
    if phase1_result:
        # Use timeout to avoid hanging indefinitely
        try:
            phase1_result.get(timeout=300)  # 5 minute timeout
            logger.info("Required metrics completed")
        except Exception as e:
            logger.error(f"Required metrics failed or timed out: {e}")
            socketio.emit('update_progress', {"progress": 100, "message": "Error: Required metrics failed"}, to=session_id)
            return
    
    # Phase 3: Execute dependent plots (these depend on Phase 1 metrics)
    if execution_tree['phase_3_dependent']:
        logger.info("Phase 3 - Dependent Plots (starting after required metrics)")
        phase3_jobs = []
        for plot in execution_tree['phase_3_dependent']:
            job_sig, plot_style = create_plot_job(plot)
            if job_sig:
                phase3_jobs.append(job_sig)
                plot_data.append([plot, plot_style, None])
                deps = planner.get_plot_dependencies().get(plot, [])
                logger.info(f"  └── {plot} (depends on: {deps})")
        
        if phase3_jobs:
            phase3_group = group(phase3_jobs)
            phase3_result = phase3_group.apply_async()
            logger.info("Started dependent plots computation")
    
    # Wait for all remaining phases to complete with error handling
    logger.info("Waiting for all plot jobs to complete...")
    socketio.emit('update_progress', {"progress": 70, "message": "Waiting for all plot jobs to complete..."}, to=session_id)
    
    phase2_results = None
    phase3_results = None
    
    # Handle Phase 2 (Independent plots) with error handling
    if phase2_result:
        try:
            phase2_results = phase2_result.get()
            logger.info("Independent plots completed successfully")
        except Exception as e:
            logger.error(f"Some independent plots failed: {str(e)}")
            emit_error(session_id, f"Some independent plots failed: {str(e)}", 'processing')
            socketio.emit('update_progress', {"progress": 75, "message": "Some independent plots failed, continuing..."}, to=session_id)
            # Try to get partial results
            try:
                phase2_results = []
                failed_plots = []
                for i, task_result in enumerate(phase2_result.results if hasattr(phase2_result, 'results') else []):
                    try:
                        if task_result.ready():
                            if task_result.successful():
                                phase2_results.append(task_result.get())
                            else:
                                plot_name = execution_tree['phase_2_independent'][i] if i < len(execution_tree['phase_2_independent']) else f"Plot {i}"
                                error_detail = str(task_result.result) if hasattr(task_result, 'result') else "Unknown error"
                                failed_plots.append(f"{plot_name}: {error_detail}")
                                logger.error(f"Independent plot {plot_name} failed: {task_result.result}")
                                phase2_results.append(None)  # Placeholder for failed task
                        else:
                            phase2_results.append(None)
                    except Exception as task_err:
                        plot_name = execution_tree['phase_2_independent'][i] if i < len(execution_tree['phase_2_independent']) else f"Plot {i}"
                        failed_plots.append(f"{plot_name}: {task_err}")
                        logger.error(f"Error getting result for independent plot {plot_name}: {task_err}")
                        phase2_results.append(None)
                if failed_plots:
                    emit_error(session_id, f"Failed plots: {'; '.join(failed_plots[:3])}", 'processing')
            except Exception:
                phase2_results = None
    
    # Handle Phase 3 (Dependent plots) with error handling
    if 'phase3_result' in locals():
        try:
            phase3_results = phase3_result.get()
            logger.info("Dependent plots completed successfully")
        except Exception as e:
            logger.error(f"Some dependent plots failed: {str(e)}")
            emit_error(session_id, f"Some dependent plots failed: {str(e)}", 'processing')
            socketio.emit('update_progress', {"progress": 80, "message": "Some dependent plots failed, continuing..."}, to=session_id)
            # Try to get partial results
            try:
                phase3_results = []
                failed_plots = []
                for i, task_result in enumerate(phase3_result.results if hasattr(phase3_result, 'results') else []):
                    try:
                        if task_result.ready():
                            if task_result.successful():
                                phase3_results.append(task_result.get())
                            else:
                                plot_name = execution_tree['phase_3_dependent'][i] if i < len(execution_tree['phase_3_dependent']) else f"Plot {i}"
                                error_detail = str(task_result.result) if hasattr(task_result, 'result') else "Unknown error"
                                failed_plots.append(f"{plot_name}: {error_detail}")
                                logger.error(f"Dependent plot {plot_name} failed: {task_result.result}")
                                phase3_results.append(None)  # Placeholder for failed task
                        else:
                            phase3_results.append(None)
                    except Exception as task_err:
                        plot_name = execution_tree['phase_3_dependent'][i] if i < len(execution_tree['phase_3_dependent']) else f"Plot {i}"
                        failed_plots.append(f"{plot_name}: {task_err}")
                        logger.error(f"Error getting result for dependent plot {plot_name}: {task_err}")
                        phase3_results.append(None)
                if failed_plots:
                    emit_error(session_id, f"Failed dependent plots: {'; '.join(failed_plots[:3])}", 'processing')
            except Exception:
                phase3_results = None

    # Process results - since we waited for all groups to complete, we can collect results
    completed_plot_data = []
    
    # Collect results from all phases
    try:
        logger.info("Collecting results from all completed phases...")
        
        # Get results from phase 1 (metrics)
        if 'phase1_result' in locals() and execution_tree['phase_1_metrics']:
            phase1_results = phase1_result.get()
            for i, plot in enumerate(execution_tree['phase_1_metrics']):
                # Find the corresponding plot_data entry
                for plot_entry in plot_data:
                    if plot_entry[0] == plot:
                        completed_plot_data.append([plot, plot_entry[1], phase1_results[i]])
                        logger.info(f"Collected result for {plot} (Phase 1)")
                        break
        
        # Get results from phase 2 (independent)
        if phase2_results is not None and execution_tree['phase_2_independent']:
            for i, plot in enumerate(execution_tree['phase_2_independent']):
                if i < len(phase2_results) and phase2_results[i] is not None:
                    for plot_entry in plot_data:
                        if plot_entry[0] == plot:
                            completed_plot_data.append([plot, plot_entry[1], phase2_results[i]])
                            logger.info(f"Collected result for {plot} (Phase 2)")
                            break
                else:
                    logger.warning(f"Skipping failed plot {plot} (Phase 2)")
        
        # Get results from phase 3 (dependent)
        if phase3_results is not None and execution_tree['phase_3_dependent']:
            for i, plot in enumerate(execution_tree['phase_3_dependent']):
                if i < len(phase3_results) and phase3_results[i] is not None:
                    for plot_entry in plot_data:
                        if plot_entry[0] == plot:
                            completed_plot_data.append([plot, plot_entry[1], phase3_results[i]])
                            logger.info(f"Collected result for {plot} (Phase 3)")
                            break
                else:
                    logger.warning(f"Skipping failed plot {plot} (Phase 3)")
                        
        logger.info(f"Successfully collected {len(completed_plot_data)} plot results")
        
    except Exception as e:
        logger.error(f"Error collecting results: {str(e)}")
        socketio.emit('update_progress', {"progress": 90, "message": f"Error collecting results: {str(e)}"}, to=session_id)

    pickle_file_path = os.path.join(directory_path, "plot_data.pkl")
    print(f"Dir Path = {directory_path}")
    print(f"Completed Plot Data: {completed_plot_data}")  # Debugging line to check the data
    with open(pickle_file_path, "wb") as f:
        pickle.dump(completed_plot_data, f, protocol=pickle.HIGHEST_PROTOCOL)

    # Final progress message
    total_requested = len(selected_plots)
    total_completed = len(completed_plot_data)
    
    if total_completed == total_requested:
        final_message = "Trajectory analysis complete - all plots generated successfully."
    else:
        final_message = f"Trajectory analysis complete - {total_completed}/{total_requested} plots generated successfully."
    
    socketio.emit('update_progress', {"progress": 100, "message": final_message}, to=session_id)

    # Schedule automatic cleanup of session folder after one month
    schedule_session_cleanup(session_id)

    return render_template(
            "view_trajectory.html",
            session_id=session_id,
            native_pdb=native_pdb,
            traj_xtc=os.path.basename(traj_xtc_path),
            plot_data=completed_plot_data,
            trajectory_length=len(u.trajectory),
            explainations=explanations,
            rotation_matrix=r_matrix_str,
            trajectory_converted=session_data_json.get('trajectory_converted', False)
        )



@app.route("/plot-biased", methods=["POST"])
def plot_biased():
    directory = request.form["directory"]
    df = read_biased(os.path.join(directory, "ratchet.out"))
    plot_filenames = save_all_plots(df, directory)
    return render_template("biased_plots.html", plot_filenames=plot_filenames)


def read_biased(file_path):
    df = pd.read_csv(file_path, sep="\t", header=None)
    return df


@socketio.on('update_frame_displayed')
def seek_right_frame_landscape(data, second_arg):
    #As for input coordinates on the energy landscape, and return the correspondin closest frame
    print(f"Data received from update frame displayed: {data}")
    session = data['session_id']
    directory_path = os.path.join(uploads_dir, session)
    generate_data_path = os.path.join(directory_path, "generated_data")
    coordinates = {
        'Q': float(data['value'][0]),  # Convert to float to ensure JSON serialization
        'RMSD': float(data['value'][1])  # Convert to float to ensure JSON serialization
    }
    print(f"Coordinates received: Q={coordinates['Q']}, RMSD={coordinates['RMSD']}")
    job = update_landscape_frame.apply_async(args=[generate_data_path, coordinates])
    result = job.get()
    print(result)
    emit('update_frame_landscape_click', {'frame': int(result)}, room=session)  # Convert result to int for JSON serialization



@socketio.on('slider_value')
def handle_slider_value(data):
    slider_value = int(data['value'])
    traj = data['traj']
    native_state = data['native_state']
    session = data['session_id']
    print(f"slider value = {slider_value}")
    path_contactmap_figure = create_contact_maps(traj, native_state, slider_value, session)


    path_save_2 = 'contact_map_plotly.png'

    emit('image_update', {'image_url': path_save_2})

@socketio.on('update_contact_map')
def update_contact_map_to_right_frame(data, second_arg):
    #print(second_arg)
    print('Trying to update contact map')
    session = data['session_id']
    slider_value = int(data['value'])
    directory_path = os.path.join(uploads_dir, session)
    generate_data_path = os.path.join(directory_path, "generated_data")
    download_plot = os.path.join(directory_path, "download_plot", "CONTACT_MAPS")
    print(f"SESSION = {session}")
    job = update_contact_map_plot.apply_async(args=[generate_data_path, download_plot, slider_value, session])
    result = job.get()
    emit('contact_map_plot_update', {'plotData': result}, room=session)

@socketio.on('update_stacking_map')
def update_stacking_map_to_right_frame(data, second_arg):
    print('Trying to update stacking map')
    session = data['session_id']
    slider_value = int(data['value'])
    directory_path = os.path.join(uploads_dir, session)
    generate_data_path = os.path.join(directory_path, "generated_data")
    download_plot = os.path.join(directory_path, "download_plot", "STACKING_MAPS")
    print(f"SESSION = {session}")
    job = update_stacking_map_plot.apply_async(args=[generate_data_path, download_plot, slider_value, session])
    result = job.get()
    emit('stacking_map_plot_update', {'plotData': result}, room=session)

def generate_image(value):
    # Create a simple image based on the slider value
    print("generating image")
    width, height = 200, 100
    image = Image.new('RGB', (width, height), color=(255, 255, 255))
    draw = ImageDraw.Draw(image)
    draw.rectangle([10, 10, 10 + value, 90], fill=(255, 0, 0))
    return image


def save_plot(df, time_column, column_to_plot, output_folder):
    plt.figure(figsize=(10, 6))
    plt.plot(df[time_column], df[column_to_plot])
    plt.title(f"{column_to_plot} over time")
    plt.xlabel("Time")
    plt.ylabel(column_to_plot)
    plot_filename = f"{column_to_plot}_over_time.png"
    plt.savefig(os.path.join(output_folder, plot_filename))
    plt.close()
    return plot_filename


def save_all_plots(df, directory):
    output_folder = os.path.join(directory, "plots")
    os.makedirs(output_folder, exist_ok=True)
    time_column = df.columns[0]
    plot_filenames = []
    for column_to_plot in df.columns[1:]:
        plot_filename = save_plot(df, time_column, column_to_plot, output_folder)
        plot_filenames.append(plot_filename)
    return plot_filenames

BUG_REPORTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data", "bug_reports"
)


@app.route("/bug-report", methods=["GET"])
def bug_report_page():
    session_id = request.args.get("session_id", "")
    return render_template("bug_report.html", session_id=session_id)


@app.route("/bug-report", methods=["POST"])
def bug_report_submit():
    session_id  = request.form.get("session_id", "").strip()
    description = request.form.get("description", "").strip()
    email       = request.form.get("email", "").strip()

    if not description:
        return render_template(
            "bug_report.html",
            session_id=session_id,
            error="Please provide a description of the bug.",
        )

    os.makedirs(BUG_REPORTS_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    slug      = datetime.now().strftime("%Y%m%d_%H%M%S")
    sid_tag   = session_id[:12] if session_id else "no_session"
    filename  = f"{slug}_{sid_tag}.txt"

    lines = [
        f"Timestamp : {timestamp}",
        f"Session ID: {session_id if session_id else '(none)'}",
        f"Email     : {email if email else '(not provided)'}",
        "",
        "--- Description ---",
        description,
        "",
    ]

    report_path = os.path.join(BUG_REPORTS_DIR, filename)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    logger.info(f"Bug report saved: {filename}")
    return render_template("bug_report.html", session_id=session_id, submitted=True)


if __name__ == "__main__":
    # eventlet.monkey_patch() already done at top
    with app.app_context():
        # app.run(debug=True)
        socketio.run(app)
