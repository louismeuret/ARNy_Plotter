import argparse
import MDAnalysis as mda
from MDAnalysis.analysis import align

parser = argparse.ArgumentParser()
parser.add_argument("--top", required=True)
parser.add_argument("--traj", required=True)
parser.add_argument("--ref", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--selection", default="all")
args = parser.parse_args()

u = mda.Universe(args.top, args.traj)
ref = mda.Universe(args.ref)

align.AlignTraj(u, ref, select=args.selection, filename=args.output).run()
print(f"Aligned trajectory saved to {args.output}")
