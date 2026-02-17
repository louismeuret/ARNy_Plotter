import MDAnalysis as mda

def save_trajectory(input_trr, input_pdb, output_xtc):
    """
    Save the trajectory from a TRR file to an XTC file.

    Parameters:
        input_trr (str): Path to the input TRR file.
        input_pdb (str): Path to the input PDB file.
        output_xtc (str): Path to save the output XTC file.
    """
    # Load the universe
    u = mda.Universe(input_pdb, input_trr)

    # Write the trajectory to an XTC file
    with mda.Writer(output_xtc, n_atoms=u.atoms.n_atoms) as W:
        for ts in u.trajectory:
            W.write(u)

    print(f"Trajectory saved to '{output_xtc}'")

if __name__ == "__main__":
    # Get user input for file paths
    input_trr = input("Enter the path to the input trajectory file: ").strip()
    input_pdb = input("Enter the path to the input PDB file: ").strip()
    output_xtc = input("Enter the path to save the output XTC file: ").strip()

    # Save the trajectory
    save_trajectory(input_trr, input_pdb, output_xtc)

