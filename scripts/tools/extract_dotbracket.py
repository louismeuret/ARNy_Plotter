import os
import csv
import subprocess

def process_pdb_files():
    # Find all PDB files in the current directory
    pdb_files = sorted([f for f in os.listdir() if f.endswith('.pdb')])
    
    output_file = "output.csv"
    
    with open(output_file, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["File", "Line 1", "Line 2", "Line 3"])  # CSV Header
        
        for pdb in pdb_files:
            print(f"Processing {pdb}...")
            # Run the command
            subprocess.run(["./x3dna-dssr", f"-i={pdb}"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Read the output file
            dbn_file = "dssr-2ndstrs.dbn"
            if os.path.exists(dbn_file):
                with open(dbn_file, 'r') as f:
                    lines = f.readlines()[:3]  # Get the first 3 lines
                
                # Ensure we have exactly three lines
                lines = [line.strip() for line in lines] + [""] * (3 - len(lines))
                
                # Write to CSV
                csv_writer.writerow([pdb] + lines)
                
                # Clean up if necessary
                os.remove(dbn_file)
            else:
                print(f"Warning: {dbn_file} not found for {pdb}")

    print(f"Processing complete. Results saved to {output_file}")

if __name__ == "__main__":
    process_pdb_files()

