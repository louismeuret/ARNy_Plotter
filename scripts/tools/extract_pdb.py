import argparse
import MDAnalysis as mda

parser = argparse.ArgumentParser()
parser.add_argument("--top", required=True)
parser.add_argument("--traj", required=True)
parser.add_argument("--frame", type=int, required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--selection", default="all")
args = parser.parse_args()

u = mda.Universe(args.top, args.traj)
u.trajectory[args.frame]
atoms = u.select_atoms(args.selection)
atoms.write(args.output)
print(f"Frame {args.frame} saved to {args.output} ({atoms.n_atoms} atoms)")
