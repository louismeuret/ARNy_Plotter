import argparse
import MDAnalysis as mda

parser = argparse.ArgumentParser()
parser.add_argument("--top", required=True)
parser.add_argument("--traj", required=True)
parser.add_argument("--start", type=int, default=0)
parser.add_argument("--stop", type=int, default=-1)
parser.add_argument("--stride", type=int, default=1)
parser.add_argument("--output", required=True)
args = parser.parse_args()

u = mda.Universe(args.top, args.traj)
stop = args.stop if args.stop != -1 else len(u.trajectory)

with mda.Writer(args.output, n_atoms=u.atoms.n_atoms) as W:
    for ts in u.trajectory[args.start:stop:args.stride]:
        W.write(u)

print(f"Wrote frames {args.start}:{stop}:{args.stride} to {args.output}")
