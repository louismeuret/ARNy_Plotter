import mdtraj as md

# Load the PDB file
md_temp = md.load('correct_conf.pdb')

# Define the regex for atom selection
regex = "(name =~ 'N[1-34679]') or (name =~ 'C[24-68]') or (name =~ 'O[462]') or (name =~ 'H(1|2|3|8|5|6|2[12]|6[12]|4[12])')"

# Select atoms based on the regex
selected_atoms = md_temp.top.select(regex)

# Filter out atoms with names ending in a prime
filtered_atoms = [atom.index for atom in md_temp.topology.atoms if atom.index in selected_atoms and not atom.name.endswith("'")]

# Create a new trajectory with the filtered atoms
md_temp2 = md_temp.atom_slice(filtered_atoms)

# Save the filtered atoms to a new PDB file
md_temp2.save('filtered_selected_atoms.pdb')

