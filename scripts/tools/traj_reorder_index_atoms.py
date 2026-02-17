import mdtraj as md
import os
import glob
import json

def load_atom_mapping(mapping_file):
    """
    Load atom mapping from a text file.
    The file should contain lines in the format:
    old_index new_index
    """
    atom_mapping = {}
    with open(mapping_file, 'r') as f:
        for line in f:
            old_index, new_index = map(int, line.split())
            atom_mapping[old_index] = new_index
    return atom_mapping

def reorder_atoms(input_pdb, input_xtc, output_xtc, atom_mapping):
    """
    Reorder atoms in the trajectory based on the given atom mapping.

    Parameters:
        input_pdb (str): Path to the input PDB file.
        input_xtc (str): Path to the input XTC file.
        output_xtc (str): Path to save the reordered XTC file.
        atom_mapping (dict): Dictionary mapping old atom indices to new indices.
    """
    # Load the trajectory and structure
    traj = md.load(input_xtc, top=input_pdb)

    # Create a new array to hold the reordered atoms
    reordered_indices = [atom_mapping[i] for i in sorted(atom_mapping)]

    # Reorder the atom indices in the trajectory
    reordered_traj = traj.atom_slice(reordered_indices)

    # Write the reordered trajectory (XTC)
    reordered_traj.save(output_xtc)

    # Save the reordered PDB for reference
    reordered_traj[0].save(os.path.join(os.path.dirname(output_xtc), 'reordered_output.pdb'))

    print(f"Reordered trajectory saved as '{output_xtc}'")
    print(f"Reordered PDB saved in the same directory.")

if __name__ == "__main__":
    # Get user input for required files and directories
    input_pdb = input("Enter the path to the input PDB file: ").strip()
    xtc_folder = input("Enter the directory containing the XTC files: ").strip()
    mapping_file = input("Enter the path to the atom mapping text file: ").strip()
    output_folder = input("Enter the output folder for reordered files: ").strip()

    # Load atom mapping from the file
    try:
        atom_mapping = load_atom_mapping(mapping_file)
    except Exception as e:
        print(f"Error loading atom mapping: {e}")
        exit(1)

    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Get all XTC files in the specified folder
    xtc_files = glob.glob(os.path.join(xtc_folder, "*.xtc"))

    if not xtc_files:
        print("No XTC files found in the specified folder.")
        exit(1)

    # Reorder the atoms in all XTC files
    for input_xtc in xtc_files:
        output_xtc = os.path.join(output_folder, os.path.basename(input_xtc).replace('.xtc', '_reordered.xtc'))
        try:
            reorder_atoms(input_pdb, input_xtc, output_xtc, atom_mapping)
        except Exception as e:
            print(f"Error processing file '{input_xtc}': {e}")

    print("All files processed. Reordered trajectories are saved in the output folder.")
