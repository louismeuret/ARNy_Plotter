"""
Computation Cache System for RNA Trajectory Analysis
Provides shared caching of expensive computations across Celery workers
"""

import os
import pickle
import hashlib
import time
import logging
from typing import Dict, Any, Optional, Tuple
from functools import wraps
import redis
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

class ComputationCache:
    """
    Redis-backed computation cache for expensive trajectory analysis operations
    Handles serialization of numpy arrays and complex data structures
    """
    
    def __init__(self, redis_url='redis://localhost:6379/1', ttl=3600):
        """
        Initialize computation cache
        
        Args:
            redis_url: Redis connection URL (use different DB than Celery)
            ttl: Time to live for cache entries in seconds (1 hour default)
        """
        self.redis_client = redis.from_url(redis_url)
        self.ttl = ttl
        self.cache_prefix = "rna_computation:"
        
        # Test Redis connection
        try:
            self.redis_client.ping()
            logger.info("ComputationCache: Redis connection established")
        except Exception as e:
            logger.error(f"ComputationCache: Redis connection failed: {e}")
            raise
    
    def _generate_cache_key(self, session_id: str, computation_type: str, file_paths: tuple, params: dict = None) -> str:
        """Generate unique cache key based on inputs"""
        # Create hash from file paths and modification times
        file_hash_parts = []
        for file_path in file_paths:
            if os.path.exists(file_path):
                mtime = os.path.getmtime(file_path)
                size = os.path.getsize(file_path)
                file_hash_parts.append(f"{file_path}:{mtime}:{size}")
        
        # Include parameters in hash
        params_str = str(sorted(params.items())) if params else ""
        
        # Create composite hash
        hash_input = f"{session_id}:{computation_type}:{':'.join(file_hash_parts)}:{params_str}"
        cache_hash = hashlib.md5(hash_input.encode()).hexdigest()
        
        return f"{self.cache_prefix}{computation_type}:{cache_hash}"
    
    def _serialize_data(self, data: Any) -> bytes:
        """Serialize complex data structures including numpy arrays"""
        return pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
    
    def _deserialize_data(self, data: bytes) -> Any:
        """Deserialize data back to original format"""
        return pickle.loads(data)
    
    def get(self, session_id: str, computation_type: str, file_paths: tuple, params: dict = None) -> Optional[Any]:
        """
        Retrieve cached computation result
        
        Args:
            session_id: Session identifier
            computation_type: Type of computation (e.g., 'bb_annotate', 'bb_rmsd')
            file_paths: Tuple of file paths used in computation
            params: Additional parameters that affect the computation
            
        Returns:
            Cached result or None if not found
        """
        cache_key = self._generate_cache_key(session_id, computation_type, file_paths, params)
        
        try:
            cached_data = self.redis_client.get(cache_key)
            if cached_data:
                result = self._deserialize_data(cached_data)
                logger.info(f"ComputationCache: Cache hit for {computation_type} (session: {session_id})")
                return result
            else:
                logger.info(f"ComputationCache: Cache miss for {computation_type} (session: {session_id})")
                return None
        except Exception as e:
            logger.error(f"ComputationCache: Error retrieving from cache: {e}")
            return None
    
    def set(self, session_id: str, computation_type: str, file_paths: tuple, data: Any, params: dict = None) -> bool:
        """
        Store computation result in cache
        
        Args:
            session_id: Session identifier
            computation_type: Type of computation
            file_paths: Tuple of file paths used in computation
            data: Result data to cache
            params: Additional parameters that affect the computation
            
        Returns:
            True if successful, False otherwise
        """
        cache_key = self._generate_cache_key(session_id, computation_type, file_paths, params)
        
        try:
            serialized_data = self._serialize_data(data)
            success = self.redis_client.setex(cache_key, self.ttl, serialized_data)
            
            if success:
                logger.info(f"ComputationCache: Cached {computation_type} for session {session_id}")
                return True
            else:
                logger.error(f"ComputationCache: Failed to cache {computation_type}")
                return False
        except Exception as e:
            logger.error(f"ComputationCache: Error storing in cache: {e}")
            return False
    
    def invalidate_session(self, session_id: str) -> int:
        """
        Invalidate all cache entries for a session
        
        Args:
            session_id: Session identifier
            
        Returns:
            Number of keys deleted
        """
        try:
            pattern = f"{self.cache_prefix}*{session_id}*"
            keys = self.redis_client.keys(pattern)
            if keys:
                deleted = self.redis_client.delete(*keys)
                logger.info(f"ComputationCache: Invalidated {deleted} cache entries for session {session_id}")
                return deleted
            return 0
        except Exception as e:
            logger.error(f"ComputationCache: Error invalidating session cache: {e}")
            return 0
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        try:
            info = self.redis_client.info()
            pattern = f"{self.cache_prefix}*"
            keys = self.redis_client.keys(pattern)
            
            return {
                'total_keys': len(keys),
                'memory_used_mb': info.get('used_memory', 0) / 1024 / 1024,
                'connected_clients': info.get('connected_clients', 0),
                'cache_hit_ratio': info.get('keyspace_hits', 0) / max(info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0), 1)
            }
        except Exception as e:
            logger.error(f"ComputationCache: Error getting cache stats: {e}")
            return {}

# Global cache instance
computation_cache = ComputationCache()

def cached_computation(computation_type: str, file_path_args: list = None, param_args: list = None):
    """
    Decorator for caching expensive computations
    
    Args:
        computation_type: Type of computation for cache key
        file_path_args: List of argument indices that contain file paths
        param_args: List of argument indices that contain parameters affecting the computation
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Extract session_id (should be last positional argument)
            session_id = args[-1] if args else kwargs.get('session_id')
            if not session_id:
                logger.warning(f"cached_computation: No session_id found, skipping cache for {func.__name__}")
                return func(*args, **kwargs)
            
            # Extract file paths for cache key
            file_paths = []
            if file_path_args:
                for idx in file_path_args:
                    if idx < len(args):
                        file_paths.append(args[idx])
            file_paths = tuple(file_paths)
            
            # Extract parameters for cache key
            params = {}
            if param_args:
                for idx in param_args:
                    if idx < len(args):
                        params[f'arg_{idx}'] = args[idx]
            
            # Add relevant kwargs to params
            for key, value in kwargs.items():
                if key != 'session_id':
                    params[key] = value
            
            # Try to get from cache
            cached_result = computation_cache.get(session_id, computation_type, file_paths, params)
            if cached_result is not None:
                return cached_result
            
            # Compute and cache result
            start_time = time.time()
            result = func(*args, **kwargs)
            computation_time = time.time() - start_time
            
            logger.info(f"cached_computation: {func.__name__} took {computation_time:.2f}s")
            
            # Cache the result
            computation_cache.set(session_id, computation_type, file_paths, result, params)
            
            return result
        return wrapper
    return decorator

class BatchComputationManager:
    """
    Manages batch processing of related computations to minimize file I/O
    """
    
    @staticmethod
    def compute_barnaba_batch(native_pdb_path: str, traj_xtc_path: str, session_id: str, 
                            required_computations: list) -> Dict[str, Any]:
        """
        Perform batch computation of multiple Barnaba operations
        
        Args:
            native_pdb_path: Path to native structure
            traj_xtc_path: Path to trajectory
            session_id: Session identifier
            required_computations: List of computation types needed
            
        Returns:
            Dictionary containing all computed results
        """
        results = {}
        
        # Check cache for each computation first
        cached_results = {}
        remaining_computations = []
        
        for comp_type in required_computations:
            cached = computation_cache.get(session_id, comp_type, (native_pdb_path, traj_xtc_path))
            if cached:
                cached_results[comp_type] = cached
                logger.info(f"BatchComputationManager: Using cached result for {comp_type}")
            else:
                remaining_computations.append(comp_type)
        
        # If all results are cached, return them
        if not remaining_computations:
            return cached_results
        
        # Perform remaining computations
        import barnaba as bb
        
        logger.info(f"BatchComputationManager: Computing {remaining_computations} for session {session_id}")
        
        # Group computations that can share intermediate results
        if any(comp in remaining_computations for comp in ['bb_annotate', 'dotbracket', 'sec_structure', 'arc']):
            logger.info("BatchComputationManager: Computing bb.annotate for multiple plots")
            stackings, pairings, res = bb.annotate(traj_xtc_path, topology=native_pdb_path)
            
            # Cache annotate results
            annotate_result = (stackings, pairings, res)
            computation_cache.set(session_id, 'bb_annotate', (native_pdb_path, traj_xtc_path), annotate_result)
            results['bb_annotate'] = annotate_result
            
            # Compute dot_bracket from pairings if needed
            if 'dotbracket' in remaining_computations or 'arc' in remaining_computations:
                dotbracket_data = bb.dot_bracket(pairings, res)[0]
                computation_cache.set(session_id, 'dotbracket', (native_pdb_path, traj_xtc_path), dotbracket_data)
                results['dotbracket'] = dotbracket_data
        
        # Individual computations
        for comp_type in remaining_computations:
            if comp_type == 'bb_rmsd':
                result = bb.rmsd(native_pdb_path, traj_xtc_path, topology=native_pdb_path, heavy_atom=True)
                computation_cache.set(session_id, comp_type, (native_pdb_path, traj_xtc_path), result)
                results[comp_type] = result
            
            elif comp_type == 'bb_ermsd':
                result = bb.ermsd(native_pdb_path, traj_xtc_path, topology=native_pdb_path)
                computation_cache.set(session_id, comp_type, (native_pdb_path, traj_xtc_path), result)
                results[comp_type] = result
            
            elif comp_type == 'bb_backbone_angles':
                result = bb.backbone_angles(traj_xtc_path, topology=native_pdb_path)
                computation_cache.set(session_id, comp_type, (native_pdb_path, traj_xtc_path), result)
                results[comp_type] = result
        
        # Merge cached and computed results
        results.update(cached_results)
        
        return results

# Global batch manager instance
batch_manager = BatchComputationManager()