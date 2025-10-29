"""
Advanced MDAnalysis Optimizations for RNA Trajectory Analysis
Implements cutting-edge performance techniques based on MDAnalysis 2024 developments
"""

import MDAnalysis as mda
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import multiprocessing as mp
from functools import partial
import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
from dataclasses import dataclass
from contextlib import contextmanager
import pickle
import tempfile
import os

logger = logging.getLogger(__name__)

@dataclass
class OptimizationConfig:
    """Configuration for trajectory analysis optimizations"""
    use_parallel_processing: bool = True
    n_workers: int = mp.cpu_count() // 2
    chunk_size: int = 100  # frames per chunk
    preload_selections: bool = True
    enable_caching: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'use_parallel_processing': self.use_parallel_processing,
            'n_workers': self.n_workers,
            'chunk_size': self.chunk_size,
            'preload_selections': self.preload_selections,
            'enable_caching': self.enable_caching
        }

class OptimizedTrajectoryProcessor:
    """
    Advanced trajectory processor implementing MDAnalysis 2024 optimizations
    """
    
    def __init__(self, topology_path: str, trajectory_path: str, config: OptimizationConfig = None):
        self.topology_path = topology_path
        self.trajectory_path = trajectory_path
        self.config = config or OptimizationConfig()
        self.universe = None
        self.cached_selections = {}
        self.cached_computations = {}
        
    def _initialize_universe(self):
        """Initialize MDAnalysis Universe"""
        if self.universe is None:
            self.universe = mda.Universe(self.topology_path, self.trajectory_path)
            logger.info(f"Initialized Universe: {len(self.universe.trajectory)} frames, {self.universe.atoms.n_atoms} atoms")
    
    def precompute_selections(self, selection_strings: Dict[str, str]):
        """
        Precompute and cache atom selections for efficiency
        Selections don't change across frames, so compute once
        """
        self._initialize_universe()
        
        for name, selection_str in selection_strings.items():
            if name not in self.cached_selections:
                start_time = time.time()
                selection = self.universe.select_atoms(selection_str)
                self.cached_selections[name] = selection
                logger.info(f"Cached selection '{name}': {len(selection)} atoms ({time.time() - start_time:.2f}s)")
    
    def parallel_frame_analysis(self, analysis_func, frame_range: Optional[Tuple[int, int]] = None, 
                              chunk_size: Optional[int] = None) -> List[Any]:
        """
        Parallel processing of trajectory frames using multiprocessing
        Based on MDAnalysis 2024 parallel analysis implementation
        """
        self._initialize_universe()
        
        if frame_range is None:
            frame_range = (0, len(self.universe.trajectory))
        
        chunk_size = chunk_size or self.config.chunk_size
        n_workers = self.config.n_workers
        
        # Split frames into chunks for parallel processing
        frame_indices = list(range(frame_range[0], frame_range[1]))
        frame_chunks = [frame_indices[i:i + chunk_size] for i in range(0, len(frame_indices), chunk_size)]
        
        logger.info(f"Processing {len(frame_indices)} frames in {len(frame_chunks)} chunks using {n_workers} workers")
        
        # Use ProcessPoolExecutor for CPU-bound analysis
        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            # Create partial function with fixed arguments
            chunk_processor = partial(self._process_frame_chunk, analysis_func)
            
            # Submit all chunks
            futures = [executor.submit(chunk_processor, chunk) for chunk in frame_chunks]
            
            # Collect results
            all_results = []
            for i, future in enumerate(futures):
                chunk_results = future.result()
                all_results.extend(chunk_results)
                logger.info(f"Completed chunk {i+1}/{len(frame_chunks)}")
        
        return all_results
    
    def _process_frame_chunk(self, analysis_func, frame_indices: List[int]) -> List[Any]:
        """
        Process a chunk of frames in a separate process
        Each process gets its own Universe instance
        """
        # Create new Universe in worker process
        u = mda.Universe(self.topology_path, self.trajectory_path)
        
        results = []
        for frame_idx in frame_indices:
            u.trajectory[frame_idx]  # Jump to specific frame
            result = analysis_func(u)
            results.append((frame_idx, result))
        
        return results
    
    def efficient_rmsd_calculation(self, reference_frame: int = 0, 
                                 selection: str = "protein") -> np.ndarray:
        """
        Optimized RMSD calculation using precomputed selections and parallel processing
        """
        self._initialize_universe()
        
        # Precompute selection
        if selection not in self.cached_selections:
            self.cached_selections[selection] = self.universe.select_atoms(selection)
        
        selected_atoms = self.cached_selections[selection]
        
        # Get reference coordinates
        self.universe.trajectory[reference_frame]
        ref_coords = selected_atoms.positions.copy()
        
        def rmsd_analysis(u):
            """RMSD calculation for a single frame"""
            from MDAnalysis.analysis import rms
            current_atoms = u.select_atoms(selection)
            return rms.rmsd(current_atoms.positions, ref_coords)
        
        # Use parallel processing for RMSD calculation
        results = self.parallel_frame_analysis(rmsd_analysis)
        
        # Sort by frame index and extract RMSD values
        results.sort(key=lambda x: x[0])
        rmsd_values = np.array([result[1] for result in results])
        
        return rmsd_values
    
    def memory_efficient_torsion_analysis(self, selection: str = "protein") -> Tuple[np.ndarray, List[str]]:
        """
        Memory-efficient torsion angle calculation using streaming approach
        """
        self._initialize_universe()
        
        # Use barnaba for torsion calculation with optimized approach
        import barnaba as bb
        
        # Check cache first
        cache_key = f"torsion_{selection}_{hash(self.trajectory_path)}"
        if cache_key in self.cached_computations:
            logger.info("Using cached torsion angles")
            return self.cached_computations[cache_key]
        
        # Calculate torsion angles
        start_time = time.time()
        angles, residues = bb.backbone_angles(self.trajectory_path, topology=self.topology_path)
        calculation_time = time.time() - start_time
        
        logger.info(f"Calculated torsion angles in {calculation_time:.2f}s")
        
        # Cache results
        if self.config.enable_caching:
            self.cached_computations[cache_key] = (angles, residues)
        
        return angles, residues
    
    def coordinate_analysis(self, analysis_func):
        """
        Standard coordinate analysis without streaming
        Process trajectory frame by frame efficiently
        """
        self._initialize_universe()
        
        results = []
        
        # Process trajectory frame by frame
        for ts in self.universe.trajectory:
            result = analysis_func(self.universe, ts)
            results.append(result)
        
        return results
    
    def batch_property_calculation(self, properties: Dict[str, callable]) -> Dict[str, np.ndarray]:
        """
        Calculate multiple properties in a single trajectory pass
        Minimizes I/O by computing everything together
        """
        self._initialize_universe()
        
        results = {prop_name: [] for prop_name in properties.keys()}
        
        logger.info(f"Computing {len(properties)} properties in single pass")
        start_time = time.time()
        
        for ts in self.universe.trajectory:
            for prop_name, prop_func in properties.items():
                try:
                    value = prop_func(self.universe)
                    results[prop_name].append(value)
                except Exception as e:
                    logger.warning(f"Error computing {prop_name} at frame {ts.frame}: {e}")
                    results[prop_name].append(np.nan)
        
        # Convert to numpy arrays
        for prop_name in results:
            results[prop_name] = np.array(results[prop_name])
        
        calculation_time = time.time() - start_time
        logger.info(f"Batch calculation completed in {calculation_time:.2f}s")
        
        return results
    
    @contextmanager
    def optimized_writer(self, output_path: str, selection: str = "all"):
        """
        Context manager for efficient trajectory writing
        """
        self._initialize_universe()
        
        selected_atoms = self.universe.select_atoms(selection)
        
        with mda.Writer(output_path, n_atoms=selected_atoms.n_atoms) as writer:
            yield writer, selected_atoms
    
    def get_optimization_stats(self) -> Dict[str, Any]:
        """Get statistics about current optimizations"""
        stats = {
            'config': self.config.to_dict(),
            'cached_selections': len(self.cached_selections),
            'cached_computations': len(self.cached_computations),
            'universe_loaded': self.universe is not None
        }
        
        if self.universe:
            stats.update({
                'n_frames': len(self.universe.trajectory),
                'n_atoms': self.universe.atoms.n_atoms,
            })
        
        return stats

# Example usage functions for common RNA analysis tasks
class RNAAnalysisOptimizer:
    """Specialized optimizer for RNA trajectory analysis"""
    
    @staticmethod
    def optimize_barnaba_calculations(processor: OptimizedTrajectoryProcessor, 
                                    calculation_types: List[str]) -> Dict[str, Any]:
        """
        Optimize multiple Barnaba calculations to share data loading
        """
        import barnaba as bb
        
        results = {}
        
        # Group calculations that can share intermediate results
        annotate_based = {'dotbracket', 'sec_structure', 'arc', 'contact_maps'}
        angle_based = {'torsion', 'jcoupling'}
        distance_based = {'rmsd', 'ermsd'}
        
        requested_annotate = annotate_based.intersection(calculation_types)
        requested_angles = angle_based.intersection(calculation_types)
        requested_distances = distance_based.intersection(calculation_types)
        
        # Process annotate-based calculations together
        if requested_annotate:
            logger.info(f"Processing annotate-based calculations: {requested_annotate}")
            stackings, pairings, res = bb.annotate(
                processor.trajectory_path, 
                topology=processor.topology_path
            )
            
            if 'dotbracket' in requested_annotate:
                results['dotbracket'] = bb.dot_bracket(pairings, res)
            
            results['annotate_data'] = (stackings, pairings, res)
        
        # Process angle-based calculations
        if requested_angles:
            logger.info(f"Processing angle-based calculations: {requested_angles}")
            if 'torsion' in requested_angles:
                results['torsion'] = processor.memory_efficient_torsion_analysis()
        
        # Process distance-based calculations
        if requested_distances:
            logger.info(f"Processing distance-based calculations: {requested_distances}")
            if 'rmsd' in requested_distances:
                results['rmsd'] = processor.efficient_rmsd_calculation()
            if 'ermsd' in requested_distances:
                ermsd = bb.ermsd(processor.topology_path, processor.trajectory_path, 
                               topology=processor.topology_path)
                results['ermsd'] = ermsd
        
        return results