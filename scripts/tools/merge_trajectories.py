import glob
import mdtraj as md

# Get all .xtc files in the folder
traj_files = glob.glob("**/md_noPBC.xtc")


topology_file = "correct_conf.pdb"  # Adjust to your topology file
traj = md.load(traj_files[0], top=topology_file)

# Initialize a list to store all the trajectory objects
trajectories = [traj]

# Load all remaining trajectory files
for traj_file in traj_files[1:]:
    print(traj_file)
    traj = md.load(traj_file, top=topology_file)
    trajectories.append(traj)
print("concatenation")
# Concatenate all trajectories
combined_traj = trajectories[0]
for t in trajectories[1:]:
    combined_traj = combined_traj.join(t)

# Save the combined trajectory to a new file
combined_traj.save_xtc("merged_trajectory.xtc")
