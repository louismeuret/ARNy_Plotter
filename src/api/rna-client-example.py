from rna_trajectory_client import RNATrajectoryAnalysis
import os

def main():
    # Initialize the client
    client = RNATrajectoryAnalysis(base_url="http://127.0.0.1:4242")
    
    # Get available plot types
    print("Available analysis types:")
    for plot in client.get_available_plots():
        print(f"- {plot}")
    
    # Upload files for analysis
    native_pdb_path = "path/to/your/structure.pdb"
    traj_xtc_path = "path/to/your/trajectory.xtc"
    
    # Select analyses to perform
    analysis_options = ["RMSD", "ERMSD", "TORSION", "SEC_STRUCTURE"]
    
    # Configure frame options
    frame_options = {
        "n_frames": 10,
        "first_frame": 1,
        "last_frame": 1000,
        "stride": 10
    }
    
    # Upload files and submit analysis
    upload_result = client.upload_files(
        native_pdb_path, 
        traj_xtc_path, 
        analysis_options=analysis_options,
        frame_options=frame_options
    )
    
    if upload_result["status"] == "success":
        print(f"Files uploaded successfully. Session ID: {upload_result['session_id']}")
        
        # Monitor analysis progress
        analysis_status = client.get_analysis_status(max_retries=60, retry_interval=5)
        
        if analysis_status["status"] == "success":
            print(f"Analysis complete!")
            print(f"Results URL: {analysis_status['results_url']}")
            print(f"View URL: {analysis_status['view_url']}")
            
            # Download results for each analysis type
            for plot_type in analysis_options:
                print(f"\nDownloading data for {plot_type}...")
                plot_data = client.download_plot_data(plot_type)
                if plot_data["status"] == "success":
                    # Save and extract the data
                    output_dir = f"./results/{plot_type}/data"
                    if "zip" in plot_data.get("content_type", ""):
                        files = client.extract_zip(plot_data, output_dir=output_dir)
                        print(f"Extracted files: {files}")
                    else:
                        saved_path = client.save_file(plot_data, output_dir=output_dir)
                        print(f"Saved data to: {saved_path}")
                
                print(f"Downloading plot for {plot_type}...")
                plot_files = client.download_plot(plot_type)
                if plot_files["status"] == "success":
                    # Save and extract the plots
                    output_dir = f"./results/{plot_type}/plots"
                    files = client.extract_zip(plot_files, output_dir=output_dir)
                    print(f"Extracted plot files: {files}")
        else:
            print(f"Analysis failed: {analysis_status['message']}")
    else:
        print(f"Upload failed: {upload_result['message']}")

if __name__ == "__main__":
    main()
