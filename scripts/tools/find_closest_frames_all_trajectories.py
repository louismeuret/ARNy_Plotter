import pandas as pd
import numpy as np
from glob import glob
import os

def find_closest_frames(trajectory_name, target_combinations):
    """
    Find the closest frames for given Q and RMSD combinations
    
    Parameters:
    trajectory_name: str, name of trajectory file
    target_combinations: list of tuples [(Q1, RMSD1), (Q2, RMSD2), ...]
    """
    
    # Load dataframe
    base_name = trajectory_name.replace(".xtc", "")
    df = pd.read_pickle(f"saved_results/{base_name}_dataframe.pkl")
    
    print(f"\nTrajectory: {base_name}")
    print(f"Total frames: {len(df)}")
    print("="*60)
    
    results = []
    
    for i, (target_q, target_rmsd) in enumerate(target_combinations):
        # Calculate Euclidean distance to target point
        # Normalize Q and RMSD to same scale for distance calculation
        q_norm = (df['Q'] - target_q) / df['Q'].std()
        rmsd_norm = (df['RMSD'] - target_rmsd) / df['RMSD'].std()
        
        distances = np.sqrt(q_norm**2 + rmsd_norm**2)
        
        # Find closest frame
        closest_idx = distances.idxmin()
        closest_frame = df.loc[closest_idx]
        
        # Calculate actual distance (not normalized)
        actual_distance = np.sqrt((closest_frame['Q'] - target_q)**2 + 
                                (closest_frame['RMSD'] - target_rmsd)**2)
        
        results.append({
            'trajectory_name': base_name,
            'target_q': target_q,
            'target_rmsd': target_rmsd,
            'frame': int(closest_frame['frame']),
            'actual_q': closest_frame['Q'],
            'actual_rmsd': closest_frame['RMSD'],
            'distance': actual_distance,
            'traj': closest_frame['traj']
        })
        
        print(f"Target {i+1}: Q={target_q:.4f}, RMSD={target_rmsd:.3f}")
        print(f"  Closest frame: {int(closest_frame['frame'])}")
        print(f"  Actual Q: {closest_frame['Q']:.6f}")
        print(f"  Actual RMSD: {closest_frame['RMSD']:.3f} nm")
        print(f"  Distance: {actual_distance:.6f}")
        print(f"  Trajectory: {closest_frame['traj']}")
        print("-"*40)
    
    return results

def find_best_matches_across_all_trajectories(target_combinations):
    """Find the best matches across ALL available trajectories"""
    
    # Find all available trajectory files
    trajectory_files = []
    
    # Look for merged_trajectory*.xtc files
    merged_files = glob("merged_trajectory*.xtc")
    trajectory_files.extend(merged_files)
    
    # Look for all_new_trajectories.xtc
    if os.path.exists("all_new_trajectories.xtc"):
        trajectory_files.append("all_new_trajectories.xtc")
    
    if not trajectory_files:
        print("No trajectory files found!")
        return []
    
    print("Found trajectory files:")
    for traj_file in trajectory_files:
        print(f"  {traj_file}")
    
    # Collect results from all trajectories
    all_results = []
    trajectory_results = {}
    
    for traj_file in trajectory_files:
        print(f"\n{'='*80}")
        print(f"Processing: {traj_file}")
        print(f"{'='*80}")
        
        try:
            results = find_closest_frames(traj_file, target_combinations)
            all_results.extend(results)
            trajectory_results[traj_file] = results
        except Exception as e:
            print(f"Error processing {traj_file}: {e}")
            continue
    
    # Find the best match for each target across all trajectories
    print(f"\n{'='*80}")
    print("BEST MATCHES ACROSS ALL TRAJECTORIES")
    print(f"{'='*80}")
    
    best_matches = []
    
    for i, (target_q, target_rmsd) in enumerate(target_combinations):
        # Get all results for this target
        target_results = [r for r in all_results if r['target_q'] == target_q and r['target_rmsd'] == target_rmsd]
        
        if not target_results:
            continue
            
        # Find the result with minimum distance
        best_result = min(target_results, key=lambda x: x['distance'])
        best_matches.append(best_result)
        
        print(f"\nTarget {i+1}: Q={target_q:.4f}, RMSD={target_rmsd:.3f}")
        print(f"  BEST MATCH:")
        print(f"    Trajectory: {best_result['trajectory_name']}")
        print(f"    Frame: {best_result['frame']}")
        print(f"    Actual Q: {best_result['actual_q']:.6f}")
        print(f"    Actual RMSD: {best_result['actual_rmsd']:.3f} nm")
        print(f"    Distance: {best_result['distance']:.6f}")
        print(f"    Traj ID: {best_result['traj']}")
        
        # Show alternatives from other trajectories
        other_results = [r for r in target_results if r['trajectory_name'] != best_result['trajectory_name']]
        if other_results:
            print(f"  Alternatives:")
            for j, alt in enumerate(sorted(other_results, key=lambda x: x['distance'])[:2]):
                print(f"    {j+2}. {alt['trajectory_name']}: Frame {alt['frame']}, "
                      f"Q={alt['actual_q']:.6f}, RMSD={alt['actual_rmsd']:.3f}, "
                      f"Distance={alt['distance']:.6f}")
    
    return best_matches, trajectory_results

def create_summary_table(best_matches, target_combinations, output_file="closest_frames_summary.txt"):
    """Create a summary table of the best matches"""
    
    with open(output_file, 'w') as f:
        f.write("CLOSEST FRAMES SUMMARY - ALL TRAJECTORIES\n")
        f.write("="*80 + "\n\n")
        
        f.write("Search performed on:\n")
        trajectory_files = glob("merged_trajectory*.xtc")
        if os.path.exists("all_new_trajectories.xtc"):
            trajectory_files.append("all_new_trajectories.xtc")
        
        for traj_file in trajectory_files:
            f.write(f"  - {traj_file}\n")
        f.write("\n")
        
        f.write("Target -> Best Match Mapping:\n")
        f.write("-" * 80 + "\n")
        
        for i, (best, (target_q, target_rmsd)) in enumerate(zip(best_matches, target_combinations)):
            f.write(f"\nTarget {i+1}:\n")
            f.write(f"  Search: Q={target_q:.4f}, RMSD={target_rmsd:.3f} nm\n")
            f.write(f"  Best Match:\n")
            f.write(f"    Trajectory: {best['trajectory_name']}\n")
            f.write(f"    Frame: {best['frame']}\n")
            f.write(f"    Actual Q: {best['actual_q']:.6f}\n")
            f.write(f"    Actual RMSD: {best['actual_rmsd']:.3f} nm\n")
            f.write(f"    Distance: {best['distance']:.6f}\n")
            f.write(f"    Traj ID: {best['traj']}\n")
    
    print(f"\nSummary saved to: {output_file}")

def compare_trajectory_coverage(trajectory_results, target_combinations):
    """Compare how well each trajectory covers the target space"""
    
    print(f"\n{'='*80}")
    print("TRAJECTORY COMPARISON")
    print(f"{'='*80}")
    
    trajectory_names = list(trajectory_results.keys())
    
    print(f"{'Target':<8} {'Q':<7} {'RMSD':<6}", end="")
    for traj_name in trajectory_names:
        print(f" {traj_name[:15]:<15}", end="")
    print()
    print("-" * (21 + len(trajectory_names) * 16))
    
    for i, (target_q, target_rmsd) in enumerate(target_combinations):
        print(f"{i+1:<8} {target_q:<7.3f} {target_rmsd:<6.2f}", end="")
        
        for traj_name in trajectory_names:
            if traj_name in trajectory_results:
                # Find the result for this target in this trajectory
                traj_results = trajectory_results[traj_name]
                target_result = next((r for r in traj_results if r['target_q'] == target_q and r['target_rmsd'] == target_rmsd), None)
                
                if target_result:
                    print(f" {target_result['distance']:<15.6f}", end="")
                else:
                    print(f" {'N/A':<15}", end="")
            else:
                print(f" {'N/A':<15}", end="")
        print()
    
    print(f"\nValues shown are distances to target (lower = better match)")

if __name__ == "__main__":
    
    # Define target combinations (Q, RMSD) - same as your modified version
    target_combinations = [
        (0.990, 0.22),
        (0.90, 0.9),
        (0.8, 0.61),
        (0.8, 1.12),
        (0.67, 0.9),
        (0.6, 1.54),
        (0.45, 1.8,
    ]
    
    print("Target Q-RMSD combinations:")
    for i, (q, rmsd) in enumerate(target_combinations):
        print(f"  {i+1}. Q={q:.4f}, RMSD={rmsd:.3f} nm")
    
    # Find best matches across all trajectories
    best_matches, trajectory_results = find_best_matches_across_all_trajectories(target_combinations)
    
    if best_matches:
        # Create summary table
        create_summary_table(best_matches, target_combinations)
        
        # Compare trajectory coverage
        compare_trajectory_coverage(trajectory_results, target_combinations)
        
        print(f"\n{'='*80}")
        print("ANALYSIS COMPLETE")
        print(f"{'='*80}")
        print(f"- Found best matches for {len(best_matches)} targets")
        print(f"- Searched across {len(trajectory_results)} trajectories")
        print(f"- Summary saved to: closest_frames_summary.txt")
    else:
        print("No matches found!")
