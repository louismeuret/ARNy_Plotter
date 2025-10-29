import MDAnalysis as mda
from MDAnalysis.coordinates import DCD, TRR

# Load the trajectory
u = mda.Universe("frame_0_noions.pdb", "SHAW_8500_10418.xtc")

# Convert to DCD
with DCD.DCDWriter("output.dcd", u.atoms.n_atoms) as writer:
    for ts in u.trajectory:
        writer.write(u.atoms)

# Or convert to TRR
with TRR.TRRWriter("output.trr", u.atoms.n_atoms) as writer:
    for ts in u.trajectory:
        writer.write(u.atoms)
        
        
        import MDAnalysis as mda

u.trajectory[0]  # Go to the first frame

# Export to GRO
with mda.coordinates.GRO.GROWriter("first_frame.gro") as gro_writer:
    gro_writer.write(u.atoms)


