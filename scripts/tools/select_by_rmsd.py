import argparse
import MDAnalysis as mda
from MDAnalysis.analysis import rms

parser = argparse.ArgumentParser()
parser.add_argument("--top", required=True)
parser.add_argument("--traj", required=True)
parser.add_argument("--ref", required=True)
parser.add_argument("--threshold", type=float, required=True)
parser.add_argument("--mode", choices=["below", "above"], default="below")
parser.add_argument("--output", required=True)
args = parser.parse_args()

u = mda.Universe(args.top, args.traj)
ref = mda.Universe(args.ref)

with mda.Writer(args.output, n_atoms=u.atoms.n_atoms) as W:
    for ts in u.trajectory:
        rmsd_val = rms.rmsd(u.atoms.positions, ref.atoms.positions)
        if args.mode == "below" and rmsd_val <= args.threshold:
            W.write(u)
        elif args.mode == "above" and rmsd_val >= args.threshold:
            W.write(u)

print(f"Filtered frames (RMSD {args.mode} {args.threshold}) saved to {args.output}")
