import MDAnalysis as mda

def save_trajectory(input_trr, input_pdb, output_xtc, selection="all"):
    """
    Save the trajectory from a TRR file to an XTC file with optional atom selection.

    Parameters:
        input_trr (str): Path to the input TRR file.
        input_pdb (str): Path to the input PDB file.
        output_xtc (str): Path to save the output XTC file.
        selection (str): MDAnalysis atom selection string (default is "all").
    """
    # Load the universe
    u = mda.Universe(input_pdb, input_trr)
    u2 = u.select_atoms(selection)

    # Write the trajectory to an XTC file
    with mda.Writer(output_xtc, n_atoms=u2.n_atoms) as W:
        for ts in u.trajectory:  # Iterate over `u.trajectory`, not `u2.trajectory`
            W.write(u2)

    print(f"Trajectory saved to '{output_xtc}'")

if __name__ == "__main__":
    # Get user input for file paths
    input_trr = input("Enter the path to the input trajectory file: ").strip()
    input_pdb = input("Enter the path to the input PDB file: ").strip()
    output_xtc = input("Enter the path to save the output XTC file: ").strip()
    selection = input("Enter the selection (default: all): ").strip() or "all"

    # Save the trajectory
    save_trajectory(input_trr, input_pdb, output_xtc, selection)

