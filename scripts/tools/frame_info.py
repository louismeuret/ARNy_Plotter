import argparse
import MDAnalysis as mda

parser = argparse.ArgumentParser()
parser.add_argument("--top", required=True)
parser.add_argument("--traj", required=True)
args = parser.parse_args()

u = mda.Universe(args.top, args.traj)
print(f"Atoms: {u.atoms.n_atoms}")
print(f"Residues: {u.residues.n_residues}")
print(f"Frames: {len(u.trajectory)}")
print(f"Timestep: {u.trajectory.dt} ps")
print(f"Total time: {u.trajectory.totaltime} ps")
if u.dimensions is not None:
    print(f"Box: {u.dimensions}")
