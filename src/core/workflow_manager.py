"""
Advanced Workflow Manager for RNA Trajectory Analysis
Implements RMSD-first computation chains with intelligent dependency management
"""

import time
import logging
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass
from enum import Enum
from celery import group, chain, chord
from celery.result import GroupResult
import json
from computation_cache import computation_cache, BatchComputationManager

logger = logging.getLogger(__name__)

class ComputationType(Enum):
    """Types of computations with their dependencies"""
    RMSD = "rmsd"
    ERMSD = "ermsd"
    ANNOTATE = "annotate"
    TORSION = "torsion"
    LANDSCAPE = "landscape"
    CONTACT_MAPS = "contact_maps"
    BASE_PAIRING = "base_pairing"
    RADIUS_OF_GYRATION = "radius_of_gyration"
    END_TO_END_DISTANCE = "end_to_end_distance"
    PCA = "pca"
    UMAP = "umap"
    TSNE = "tsne"

@dataclass
class ComputationTask:
    """Represents a computation task with its dependencies and metadata"""
    task_id: str
    computation_type: ComputationType
    dependencies: Set[ComputationType]
    estimated_time: float
    priority: int  # Lower number = higher priority
    is_shared: bool  # True if result can be shared with other tasks
    
    @property
    def cache_key(self) -> str:
        return f"{self.computation_type.value}_{self.task_id}"

class WorkflowOrchestrator:
    """
    Orchestrates RNA analysis workflows with dependency management and RMSD-first optimization
    """
    
    # Dependency mapping - defines which computations depend on others
    DEPENDENCIES = {
        ComputationType.RMSD: set(),
        ComputationType.ERMSD: set(),
        ComputationType.ANNOTATE: set(),
        ComputationType.TORSION: set(),
        ComputationType.LANDSCAPE: {ComputationType.RMSD, ComputationType.ERMSD},
        ComputationType.CONTACT_MAPS: {ComputationType.ANNOTATE},
        ComputationType.BASE_PAIRING: {ComputationType.ANNOTATE},
        ComputationType.RADIUS_OF_GYRATION: set(),
        ComputationType.END_TO_END_DISTANCE: set(),
        ComputationType.PCA: set(),
        ComputationType.UMAP: set(),
        ComputationType.TSNE: set(),
    }
    
    # Task priorities (lower = higher priority, RMSD first)
    PRIORITIES = {
        ComputationType.RMSD: 1,
        ComputationType.ERMSD: 2,
        ComputationType.ANNOTATE: 3,
        ComputationType.TORSION: 4,
        ComputationType.CONTACT_MAPS: 5,
        ComputationType.BASE_PAIRING: 6,
        ComputationType.RADIUS_OF_GYRATION: 7,
        ComputationType.END_TO_END_DISTANCE: 8,
        ComputationType.PCA: 9,
        ComputationType.UMAP: 10,
        ComputationType.TSNE: 11,
        ComputationType.LANDSCAPE: 12,  # Depends on RMSD/ERMSD, so runs later
    }
    
    # Estimated computation times (seconds)
    ESTIMATED_TIMES = {
        ComputationType.RMSD: 30.0,
        ComputationType.ERMSD: 45.0,
        ComputationType.ANNOTATE: 120.0,
        ComputationType.TORSION: 60.0,
        ComputationType.CONTACT_MAPS: 180.0,
        ComputationType.BASE_PAIRING: 150.0,
        ComputationType.RADIUS_OF_GYRATION: 40.0,
        ComputationType.END_TO_END_DISTANCE: 35.0,
        ComputationType.PCA: 80.0,
        ComputationType.UMAP: 120.0,
        ComputationType.TSNE: 150.0,
        ComputationType.LANDSCAPE: 300.0,
    }

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.batch_manager = BatchComputationManager()
        self.computation_cache = computation_cache
        
    def create_workflow(self, selected_plots: List[str], 
                       native_pdb_path: str, traj_xtc_path: str,
                       session_params: Dict[str, Any]) -> 'WorkflowPlan':
        """
        Create an optimized workflow plan with RMSD-first computation chains
        
        Args:
            selected_plots: List of requested plot types
            native_pdb_path: Path to native structure
            traj_xtc_path: Path to trajectory file
            session_params: Additional parameters for computations
            
        Returns:
            WorkflowPlan with optimized execution strategy
        """
        # Map plot names to computation types
        plot_to_computation = {
            'RMSD': ComputationType.RMSD,
            'ERMSD': ComputationType.ERMSD,
            'TORSION': ComputationType.TORSION,
            'ANNOTATE': ComputationType.ANNOTATE,
            'DOTBRACKET': ComputationType.ANNOTATE,
            'ARC': ComputationType.ANNOTATE,
            'SEC_STRUCTURE': ComputationType.ANNOTATE,
            'CONTACT_MAPS': ComputationType.CONTACT_MAPS,
            'BASE_PAIRING': ComputationType.BASE_PAIRING,
            'LANDSCAPE': ComputationType.LANDSCAPE,
            'RADIUS_OF_GYRATION': ComputationType.RADIUS_OF_GYRATION,
            'END_TO_END_DISTANCE': ComputationType.END_TO_END_DISTANCE,
            'PCA': ComputationType.PCA,
            'UMAP': ComputationType.UMAP,
            'TSNE': ComputationType.TSNE,
        }
        
        # Create tasks for selected plots
        required_computations = set()
        for plot in selected_plots:
            if plot in plot_to_computation:
                comp_type = plot_to_computation[plot]
                required_computations.add(comp_type)
                # Add dependencies
                required_computations.update(self.DEPENDENCIES[comp_type])
        
        # Create computation tasks
        tasks = {}
        for comp_type in required_computations:
            task = ComputationTask(
                task_id=f"{self.session_id}_{comp_type.value}",
                computation_type=comp_type,
                dependencies=self.DEPENDENCIES[comp_type],
                estimated_time=self.ESTIMATED_TIMES[comp_type],
                priority=self.PRIORITIES[comp_type],
                is_shared=comp_type in [ComputationType.RMSD, ComputationType.ERMSD, ComputationType.ANNOTATE]
            )
            tasks[comp_type] = task
        
        return WorkflowPlan(
            session_id=self.session_id,
            tasks=tasks,
            native_pdb_path=native_pdb_path,
            traj_xtc_path=traj_xtc_path,
            session_params=session_params
        )

class WorkflowPlan:
    """
    Represents an execution plan for RNA analysis workflow
    """
    
    def __init__(self, session_id: str, tasks: Dict[ComputationType, ComputationTask],
                 native_pdb_path: str, traj_xtc_path: str, session_params: Dict[str, Any]):
        self.session_id = session_id
        self.tasks = tasks
        self.native_pdb_path = native_pdb_path
        self.traj_xtc_path = traj_xtc_path
        self.session_params = session_params
        
    def get_execution_stages(self) -> List[List[ComputationTask]]:
        """
        Get tasks organized into execution stages based on dependencies
        Tasks in the same stage can run in parallel
        """
        # Topological sort to determine execution order
        stages = []
        remaining_tasks = set(self.tasks.values())
        completed = set()
        
        while remaining_tasks:
            # Find tasks with no unmet dependencies
            ready_tasks = []
            for task in remaining_tasks:
                if task.dependencies.issubset(completed):
                    ready_tasks.append(task)
            
            if not ready_tasks:
                # Shouldn't happen with proper dependencies
                logger.error("Circular dependency detected in workflow!")
                break
                
            # Sort by priority within the stage
            ready_tasks.sort(key=lambda t: t.priority)
            stages.append(ready_tasks)
            
            # Mark as completed and remove from remaining
            for task in ready_tasks:
                completed.add(task.computation_type)
                remaining_tasks.remove(task)
        
        return stages
    
    def get_optimization_summary(self) -> Dict[str, Any]:
        """Get workflow optimization statistics"""
        total_time_unoptimized = sum(task.estimated_time for task in self.tasks.values())
        
        stages = self.get_execution_stages()
        total_time_optimized = sum(max(task.estimated_time for task in stage) for stage in stages)
        
        shared_computations = [task for task in self.tasks.values() if task.is_shared]
        
        return {
            'total_tasks': len(self.tasks),
            'execution_stages': len(stages),
            'total_time_unoptimized': total_time_unoptimized,
            'total_time_optimized': total_time_optimized,
            'time_savings': total_time_unoptimized - total_time_optimized,
            'efficiency_gain': (total_time_unoptimized - total_time_optimized) / total_time_unoptimized * 100,
            'shared_computations': len(shared_computations),
            'parallelization_factor': total_time_unoptimized / total_time_optimized
        }

class WorkflowExecutor:
    """
    Executes workflow plans using Celery task chains and groups
    """
    
    def __init__(self, app2):
        self.celery_app = app2
        self.computation_cache = computation_cache
        
    async def execute_workflow(self, workflow_plan: WorkflowPlan, 
                             progress_callback: Optional[callable] = None) -> Dict[str, Any]:
        """
        Execute workflow plan with optimized RMSD-first approach
        
        Args:
            workflow_plan: The workflow plan to execute
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary with execution results
        """
        stages = workflow_plan.get_execution_stages()
        results = {}
        total_stages = len(stages)
        
        logger.info(f"Executing workflow with {total_stages} stages for session {workflow_plan.session_id}")
        
        for stage_idx, stage_tasks in enumerate(stages):
            stage_start_time = time.time()
            
            # Progress update
            if progress_callback:
                progress = (stage_idx / total_stages) * 100
                progress_callback({
                    'progress': progress,
                    'message': f"Stage {stage_idx + 1}/{total_stages}: {[t.computation_type.value for t in stage_tasks]}"
                })
            
            # Check cache for stage tasks
            cached_results = {}
            remaining_tasks = []
            
            for task in stage_tasks:
                cache_key = f"{task.computation_type.value}_{workflow_plan.session_id}"
                cached = self.computation_cache.get(
                    workflow_plan.session_id,
                    task.computation_type.value,
                    (workflow_plan.native_pdb_path, workflow_plan.traj_xtc_path)
                )
                
                if cached:
                    cached_results[task.computation_type] = cached
                    logger.info(f"Using cached result for {task.computation_type.value}")
                else:
                    remaining_tasks.append(task)
            
            # Execute remaining tasks in parallel
            if remaining_tasks:
                stage_results = await self._execute_stage_parallel(
                    remaining_tasks, workflow_plan
                )
                results.update(stage_results)
            
            # Add cached results
            results.update(cached_results)
            
            stage_duration = time.time() - stage_start_time
            logger.info(f"Stage {stage_idx + 1} completed in {stage_duration:.2f}s")
        
        # Final progress update
        if progress_callback:
            progress_callback({
                'progress': 100,
                'message': "Workflow execution complete"
            })
        
        return results
    
    async def _execute_stage_parallel(self, tasks: List[ComputationTask], 
                                    workflow_plan: WorkflowPlan) -> Dict[ComputationType, Any]:
        """Execute tasks in a stage in parallel using Celery groups"""
        from tasks import (
            generate_rmsd_plot, generate_ermsd_plot, generate_torsion_plot,
            generate_contact_map_plot, generate_annotate_plot, generate_landscape_plot,
            generate_2Dpairing_plot, generate_radius_of_gyration_plot,
            generate_end_to_end_distance_plot, generate_dimensionality_reduction_plot
        )
        
        # Map computation types to Celery tasks
        task_mapping = {
            ComputationType.RMSD: generate_rmsd_plot,
            ComputationType.ERMSD: generate_ermsd_plot,
            ComputationType.TORSION: generate_torsion_plot,
            ComputationType.ANNOTATE: generate_annotate_plot,
            ComputationType.CONTACT_MAPS: generate_contact_map_plot,
            ComputationType.BASE_PAIRING: generate_2Dpairing_plot,
            ComputationType.LANDSCAPE: generate_landscape_plot,
            ComputationType.RADIUS_OF_GYRATION: generate_radius_of_gyration_plot,
            ComputationType.END_TO_END_DISTANCE: generate_end_to_end_distance_plot,
            ComputationType.PCA: generate_dimensionality_reduction_plot,
            ComputationType.UMAP: generate_dimensionality_reduction_plot,
            ComputationType.TSNE: generate_dimensionality_reduction_plot,
        }
        
        # Create Celery tasks
        celery_tasks = []
        for task in tasks:
            if task.computation_type in task_mapping:
                celery_task = task_mapping[task.computation_type]
                
                # Prepare task arguments based on task type
                args = self._prepare_task_args(task, workflow_plan)
                
                celery_tasks.append(celery_task.s(*args))
        
        # Execute tasks in parallel using Celery group
        if celery_tasks:
            job = group(celery_tasks).apply_async()
            group_results = job.get()  # Wait for all tasks to complete
            
            # Process results
            results = {}
            for i, task in enumerate(tasks):
                if i < len(group_results):
                    results[task.computation_type] = group_results[i]
                    
                    # Cache the result
                    self.computation_cache.set(
                        workflow_plan.session_id,
                        task.computation_type.value,
                        (workflow_plan.native_pdb_path, workflow_plan.traj_xtc_path),
                        group_results[i]
                    )
            
            return results
        
        return {}
    
    def _prepare_task_args(self, task: ComputationTask, workflow_plan: WorkflowPlan) -> List[Any]:
        """Prepare arguments for Celery task based on task type"""
        base_args = [
            workflow_plan.native_pdb_path,
            workflow_plan.traj_xtc_path,
            f"static/uploads/{workflow_plan.session_id}/download_data/{task.computation_type.value.upper()}",
            f"static/uploads/{workflow_plan.session_id}/download_plot/{task.computation_type.value.upper()}",
        ]

        if task.computation_type == ComputationType.LANDSCAPE:
            # Add landscape-specific parameters
            landscape_params = [
                workflow_plan.session_params.get("landscape_stride", 1),
                workflow_plan.session_params.get("landscape_first_component", "RMSD"),
                workflow_plan.session_params.get("landscape_second_component", "eRMSD")
            ]
            base_args.append(workflow_plan.session_id)
            base_args.append(landscape_params)
            base_args.append(f"static/uploads/{workflow_plan.session_id}/generated_data")
        elif task.computation_type == ComputationType.TORSION:
            # Add torsion-specific parameters
            base_args.append(workflow_plan.session_id)
            base_args.append(workflow_plan.session_params.get("torsionResidue", 0))
        elif task.computation_type == ComputationType.CONTACT_MAPS:
            # Add contact maps-specific parameters
            base_args.append(f"static/uploads/{workflow_plan.session_id}/generated_data")
            base_args.append(workflow_plan.session_id)
        elif task.computation_type in [ComputationType.RADIUS_OF_GYRATION, ComputationType.END_TO_END_DISTANCE]:
            # New RNA analysis plots take only session_id
            return [workflow_plan.session_id]
        elif task.computation_type in [ComputationType.PCA, ComputationType.UMAP, ComputationType.TSNE]:
            # Dimensionality reduction plots take session_id and method
            method_map = {
                ComputationType.PCA: 'pca',
                ComputationType.UMAP: 'umap', 
                ComputationType.TSNE: 'tsne'
            }
            return [workflow_plan.session_id, method_map[task.computation_type]]
        else:
            base_args.append(workflow_plan.session_id)

        return base_args

# Workflow factory function
def create_rmsd_first_workflow(session_id: str, selected_plots: List[str],
                              native_pdb_path: str, traj_xtc_path: str,
                              session_params: Dict[str, Any]) -> WorkflowPlan:
    """
    Factory function to create RMSD-first optimized workflow
    """
    orchestrator = WorkflowOrchestrator(session_id)
    workflow_plan = orchestrator.create_workflow(
        selected_plots, native_pdb_path, traj_xtc_path, session_params
    )
    
    # Log workflow optimization summary
    optimization_summary = workflow_plan.get_optimization_summary()
    logger.info(f"Workflow optimization summary for session {session_id}:")
    logger.info(f"  - Total tasks: {optimization_summary['total_tasks']}")
    logger.info(f"  - Execution stages: {optimization_summary['execution_stages']}")
    logger.info(f"  - Time savings: {optimization_summary['time_savings']:.1f}s ({optimization_summary['efficiency_gain']:.1f}%)")
    logger.info(f"  - Parallelization factor: {optimization_summary['parallelization_factor']:.1f}x")
    
    return workflow_plan