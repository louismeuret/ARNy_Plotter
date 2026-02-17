import argparse
import MDAnalysis as mda

parser = argparse.ArgumentParser()
parser.add_argument("--top", required=True)
parser.add_argument("--traj", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--stride", type=int, default=1)
parser.add_argument("--selection", default="all")
args = parser.parse_args()

u = mda.Universe(args.top, args.traj)
atoms = u.select_atoms(args.selection)

with mda.Writer(args.output, n_atoms=atoms.n_atoms) as W:
    for ts in u.trajectory[::args.stride]:
        W.write(atoms)

print(f"Wrote {len(u.trajectory)//args.stride} frames to {args.output}")
