import MDAnalysis as mda
from MDAnalysis.analysis import align

# Load the topology and trajectory
u = mda.Universe("frame_0.pdb", "convert/full_centered.xtc")

# Select only nucleic acids
nucleic_acids = u.select_atoms("nucleic")

# Save the topology (nucleic only) to PDB
nucleic_acids.write("nucleic_only.pdb")

# Create a new universe with nucleic-acid-only selection
nucleic_only_universe = mda.Merge(nucleic_acids)
nucleic_only_universe.trajectory = u.trajectory  # Attach the trajectory

# Save the trajectory (nucleic only) to XTC
with mda.Writer("nucleic_only.xtc", n_atoms=nucleic_acids.n_atoms) as writer:
    for ts in nucleic_only_universe.trajectory:
        writer.write(nucleic_acids)

