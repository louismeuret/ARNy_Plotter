import requests
import time
import json
import os
import zipfile
import io
import argparse
import sys
from urllib.parse import urljoin
import warnings

class RNATrajectoryAnalysis:
    """
    Client for interacting with the RNA trajectory analysis web service.
    """

    def __init__(self, base_url="http://localhost:5000", verify_ssl=True):
        """
        Initialize the RNA trajectory analysis client.

        Args:
            base_url (str): The base URL of the RNA trajectory analysis service
            verify_ssl (bool): Whether to verify SSL certificates
        """
        self.base_url = base_url
        self.verify_ssl = verify_ssl
        self.session_id = None
        self.native_pdb = None
        self.traj_xtc = None

        if not verify_ssl:
            warnings.warn("SSL verification is disabled. This is not recommended for production use.")

    def start_session(self):
        """
        Start a new analysis session.

        Returns:
            str: The session ID for the new session
        """
        response = requests.get(urljoin(self.base_url, "/get_session"), verify=self.verify_ssl)
        # Extract session ID from JSON response
        session_id = response.json().get("session_id")
        if not session_id:
            # Alternative approach - make a POST request to get a session ID
            response = requests.post(urljoin(self.base_url, "/"), verify=self.verify_ssl)
            session_id = response.cookies.get("session")

        self.session_id = session_id
        print(session_id)
        return session_id

    def upload_files(self, native_pdb_path, traj_xtc_path, analysis_options=None, frame_options=None, landscape_params=None, plot_settings=None):
        """
        Upload PDB and XTC files for analysis.

        Args:
            native_pdb_path (str): Path to the native PDB file
            traj_xtc_path (str): Path to the trajectory XTC file
            analysis_options (list): List of analysis types to perform (e.g., ["RMSD", "ERMSD", "TORSION"])
            frame_options (dict): Dict with frame selection options (e.g., {"n_frames": 10, "first_frame": 1, "last_frame": 100, "stride": 1})
            landscape_params (dict): Parameters for LANDSCAPE plot (e.g., {"x_param": "ERMSD", "y_param": "Q"})
            plot_settings (dict): Plot-specific settings for customization (e.g., {"rmsd": {"heavy_atom": True, "title": "Custom RMSD"}})

        Returns:
            dict: Response from the server with the status of the upload
        """
        if not self.session_id:
            self.start_session()

        # Set default options if not provided
        if analysis_options is None:
            analysis_options = ["RMSD", "ERMSD"]

        if frame_options is None:
            frame_options = {"n_frames": 1, "first_frame": "", "last_frame": "", "stride": ""}

        if landscape_params is None:
            landscape_params = {"x_param": "ERMSD", "y_param": "Q"}

        if plot_settings is None:
            plot_settings = {}

        # Prepare form data
        form_data = {
            "n_frames": frame_options.get("n_frames", 1),
            "firstFrame": frame_options.get("first_frame", ""),
            "lastFrame": frame_options.get("last_frame", ""),
            "stride": frame_options.get("stride", ""),
            "session_id": self.session_id
        }

        # Add selected plots to form data
        for plot in analysis_options:
            form_data[plot.lower()] = "on"

        # Add LANDSCAPE parameters if LANDSCAPE is selected
        if "LANDSCAPE" in analysis_options:
            x_param = landscape_params.get("x_param", "ERMSD")
            y_param = landscape_params.get("y_param", "Q")


            form_data["firstDimension"] = landscape_params.get("x_param", "ERMSD")
            form_data["secondDimension"] = landscape_params.get("y_param", "Q")
            form_data["landscape_stride"] = 1

            # Replace ERMSD with eRMSD if needed
            if x_param == "ERMSD":
                form_data["firstDimension"] = "eRMSD"
            if y_param == "ERMSD":
                form_data["secondDimension"] = "eRMSD"

        # Add plot settings if provided
        if plot_settings:
            form_data["plot_settings"] = json.dumps(plot_settings)

        # Prepare files
        files = {
            "nativePdb": (os.path.basename(native_pdb_path), open(native_pdb_path, "rb"), "chemical/x-pdb"),
            "trajXtc": (os.path.basename(traj_xtc_path), open(traj_xtc_path, "rb"), "application/octet-stream")
        }

        # Store filenames for later use
        self.native_pdb = os.path.basename(native_pdb_path)
        self.traj_xtc = os.path.basename(traj_xtc_path)
        print(form_data)

        try:
            # Make the upload request
            response = requests.post(
                urljoin(self.base_url, "/upload-files"),
                files=files,
                data=form_data,
                verify=self.verify_ssl
            )

            # Print detailed response information for debugging
            #print(f"Response status code: {response.status_code}")
            #print(f"Response headers: {response.headers}")
            #print(f"Response text: {response.text}")

            # Close file handles
            for file_obj in files.values():
                file_obj[1].close()

            if response.status_code == 200:
                return {"status": "success", "session_id": self.session_id}
            else:
                return {"status": "error", "message": f"Upload failed with status code {response.status_code}", "response": response.text}

        except Exception as e:
            # Close file handles on error
            for file_obj in files.values():
                file_obj[1].close()
            return {"status": "error", "message": str(e)}

    def get_analysis_status(self, max_retries=30, retry_interval=2, show_progress=True):
        """
        Check the status of the analysis and wait until it's complete.

        Args:
            max_retries (int): Maximum number of retries
            retry_interval (int): Interval between retries in seconds
            show_progress (bool): Whether to show a progress animation

        Returns:
            dict: Analysis results or error information
        """
        if not self.session_id or not self.native_pdb or not self.traj_xtc:
            return {"status": "error", "message": "No active analysis session"}

        # URL for retrieving results
        results_url = urljoin(
            self.base_url,
            f"/retrieve-results?session_id={self.session_id}"
        )

        # ASCII loading animations
        loading_frames = [
            "O       o O       o O       o\n| O   o | | O   o | | O   o |\n| | O | | | | O | | | | O | |\n| o   O | | o   O | | o   O |\no       O o       O o       O",
            "o       O o       O o       O\n| o   O | | o   O | | o   O |\n| | O | | | | O | | | | O | |\n| O   o | | O   o | | O   o |\nO       o O       o O       o",
            "O       o O       o O       o\n| O   o | | O   o | | O   o |\n| | O | | | | O | | | | O | |\n| o   O | | o   O | | o   O |\no       O o       O o       O",
            "o       O o       O o       O\n| o   O | | o   O | | o   O |\n| | O | | | | O | | | | O | |\n| O   o | | O   o | | O   o |\nO       o O       o O       o"
        ]

        # Then poll for results
        for attempt in range(max_retries):
            try:
                response = requests.get(results_url, verify=self.verify_ssl)

                if response.status_code == 200:
                    # Successful result
                    if show_progress:
                        # Clear the loading animation
                        sys.stdout.write("\033[F" * 5)  # Move cursor up 5 lines
                        sys.stdout.write("\033[K" * 5)  # Clear 5 lines
                        sys.stdout.flush()
                    return {
                        "status": "success",
                        "message": "Analysis complete",
                        "results_url": results_url
                    }
                elif response.status_code == 404:
                    # Results not ready yet
                    if show_progress:
                        # Show loading animation
                        frame_idx = attempt % len(loading_frames)
                        # Clear previous frame if not the first attempt
                        if attempt > 0:
                            sys.stdout.write("\033[F" * 5)  # Move cursor up 5 lines
                        print(loading_frames[frame_idx])
                        sys.stdout.flush()
                    else:
                        print(f"Analysis in progress... (attempt {attempt+1}/{max_retries})")
                    time.sleep(retry_interval)
                else:
                    # Unexpected status code
                    if show_progress:
                        # Clear the loading animation
                        sys.stdout.write("\033[F" * 5)  # Move cursor up 5 lines
                        sys.stdout.write("\033[K" * 5)  # Clear 5 lines
                        sys.stdout.flush()
                    return {
                        "status": "error",
                        "message": f"Got unexpected status code: {response.status_code}"
                    }

            except Exception as e:
                if show_progress:
                    # Clear the loading animation
                    sys.stdout.write("\033[F" * 5)  # Move cursor up 5 lines
                    sys.stdout.write("\033[K" * 5)  # Clear 5 lines
                    sys.stdout.flush()
                return {"status": "error", "message": str(e)}

        if show_progress:
            # Clear the loading animation
            sys.stdout.write("\033[F" * 5)  # Move cursor up 5 lines
            sys.stdout.write("\033[K" * 5)  # Clear 5 lines
            sys.stdout.flush()
        return {"status": "error", "message": "Analysis timed out"}

    def download_plot_data(self, plot_id):
        """
        Download data for a specific plot.

        Args:
            plot_id (str): ID of the plot (e.g., "RMSD", "ERMSD")

        Returns:
            dict: Dictionary with file content or error message
        """
        if not self.session_id:
            return {"status": "error", "message": "No active session"}

        download_url = urljoin(
            self.base_url,
            f"/download/plot_data/{self.session_id}/{plot_id}"
        )

        try:
            response = requests.get(download_url, verify=self.verify_ssl)

            if response.status_code == 200:
                filename = plot_id + "_data.zip"
                content_type = response.headers.get('Content-Type', '')

                if 'zip' in content_type:
                    # Return as zip file bytes
                    return {
                        "status": "success",
                        "filename": filename,
                        "content": response.content,
                        "content_type": content_type
                    }
                else:
                    # Return as raw data
                    return {
                        "status": "success",
                        "filename": plot_id + "_data.csv",
                        "content": response.content,
                        "content_type": content_type
                    }
            else:
                return {"status": "error", "message": f"Download failed with status code {response.status_code}"}

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def download_plot(self, plot_id):
        """
        Download plot visualization for a specific analysis.

        Args:
            plot_id (str): ID of the plot (e.g., "RMSD", "ERMSD")

        Returns:
            dict: Dictionary with file content or error message
        """
        if not self.session_id:
            return {"status": "error", "message": "No active session"}

        download_url = urljoin(
            self.base_url,
            f"/download/plot/{self.session_id}/{plot_id}"
        )

        try:
            response = requests.get(download_url, verify=self.verify_ssl)

            if response.status_code == 200:
                filename = plot_id + "_plots.zip"
                content_type = response.headers.get('Content-Type', '')

                return {
                    "status": "success",
                    "filename": filename,
                    "content": response.content,
                    "content_type": content_type
                }
            else:
                return {"status": "error", "message": f"Download failed with status code {response.status_code}"}

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def save_file(self, file_data, output_dir="./outputs"):
        """
        Save downloaded file data to disk.

        Args:
            file_data (dict): File data returned from download_plot or download_plot_data
            output_dir (str): Directory to save the file

        Returns:
            str: Path to the saved file or error message
        """
        if file_data.get("status") != "success":
            return file_data.get("message", "Unknown error")

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Save the file
        output_path = os.path.join(output_dir, file_data["filename"])
        with open(output_path, "wb") as f:
            f.write(file_data["content"])

        return output_path

    def extract_zip(self, zip_data, output_dir="./outputs"):
        """
        Extract contents of a zip file returned from download_plot or download_plot_data.

        Args:
            zip_data (dict): Zip file data returned from download functions
            output_dir (str): Directory to extract the files to

        Returns:
            list: List of extracted file paths or error message
        """
        if zip_data.get("status") != "success":
            return zip_data.get("message", "Unknown error")

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Extract the zip file
        try:
            extracted_files = []
            with zipfile.ZipFile(io.BytesIO(zip_data["content"])) as z:
                z.extractall(output_dir)
                extracted_files = [os.path.join(output_dir, name) for name in z.namelist()]

            return extracted_files
        except Exception as e:
            return [f"Error extracting zip: {str(e)}"]

    def get_available_plots(self):
        """
        Get a list of available plot types for RNA analysis.

        Returns:
            list: List of available plot types
        """
        return [
            "RMSD",
            "ERMSD",
            "CONTACT_MAPS",
            "TORSION",
            "SEC_STRUCTURE",
            "DOTBRACKET",
            "ARC",
            "ANNOTATE",
            "DS_MOTIF",
            "SS_MOTIF",
            "JCOUPLING",
            "ESCORE",
            "LANDSCAPE",
            "BASE_PAIRING",
            "RADIUS_OF_GYRATION",
            "END_TO_END_DISTANCE",
            "PCA",
            "UMAP",
            "TSNE"
        ]

    def get_landscape_parameters(self):
        """
        Get available parameters for LANDSCAPE plots.

        Returns:
            dict: Dictionary with available X and Y parameters
        """
        return {
            "x_params": ["RMSD", "ERMSD", "TORSION", "Q"],
            "y_params": ["RMSD", "ERMSD", "TORSION", "Q"]
        }

    def get_plot_settings_info(self):
        """
        Get information about available plot settings.

        Returns:
            dict: Dictionary with plot settings information and examples
        """
        return {
            "rmsd": {
                "heavy_atom": "Use heavy atoms only (bool, default: True)",
                "title": "Custom plot title (string)",
                "color_scheme": "Color scheme (viridis, plasma, blues, reds)"
            },
            "ermsd": {
                "heavy_atom": "Use heavy atoms only (bool, default: True)",
                "title": "Custom plot title (string)",
                "color_scheme": "Color scheme (viridis, plasma, blues, reds)"
            },
            "radius_of_gyration": {
                "selection": "Atom selection (nucleic, all, backbone)",
                "show_components": "Show X,Y,Z components (bool, default: True)",
                "title": "Custom plot title (string)"
            },
            "end_to_end_distance": {
                "five_prime_atom": "5' terminal atom (C5', P)",
                "three_prime_atom": "3' terminal atom (C3', P)",
                "title": "Custom plot title (string)"
            },
            "pca": {
                "n_components": "Number of components (int, 2-50, default: 10)",
                "show_variance": "Show variance explained (bool, default: True)",
                "title": "Custom plot title (string)"
            },
            "umap": {
                "n_neighbors": "Number of neighbors (int, 2-100, default: 15)",
                "min_dist": "Minimum distance (float, 0.0-1.0, default: 0.1)",
                "random_state": "Random seed (int, default: 42)",
                "title": "Custom plot title (string)"
            },
            "tsne": {
                "perplexity": "Perplexity (int, 5-50, default: 30)",
                "random_state": "Random seed (int, default: 42)",
                "title": "Custom plot title (string)"
            },
            "landscape": {
                "grid_size": "Grid resolution (int, 20-200, default: 65)",
                "first_dimension": "X-axis parameter (RMSD, eRMSD)",
                "second_dimension": "Y-axis parameter (RMSD, eRMSD)",
                "title": "Custom plot title (string)"
            }
        }

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='RNA Trajectory Analysis Tool')
    parser.add_argument('trajectory', help='Path to the trajectory XTC file')
    parser.add_argument('topology', help='Path to the topology PDB file')
    parser.add_argument('--analyses', nargs='+', default=['RMSD', 'ERMSD'],
                      choices=['RMSD', 'ERMSD', 'CONTACT_MAPS', 'TORSION', 'SEC_STRUCTURE',
                              'DOTBRACKET', 'ARC', 'LANDSCAPE', 'BASE_PAIRING', 
                              'RADIUS_OF_GYRATION', 'END_TO_END_DISTANCE', 'PCA', 'UMAP', 'TSNE', 'ALL'],
                      help='List of analyses to run (or "ALL" to run everything)')
    parser.add_argument('--n_frames', type=int, default=1,
                       help='Number of frames to analyze')
    parser.add_argument('--first_frame', type=int, default=0,
                       help='First frame to analyze')
    parser.add_argument('--last_frame', type=int, default=None,
                       help='Last frame to analyze')
    parser.add_argument('--stride', type=int, default=1,
                       help='Stride for frame selection')
    parser.add_argument('--server', default='http://localhost:4242',
                       help='Server URL for the analysis service')
    parser.add_argument('--no-verify-ssl', action='store_true',
                       help='Disable SSL certificate verification (not recommended for production)')
    parser.add_argument('--landscape-x', choices=['RMSD', 'ERMSD', 'TORSION', 'Q'], default='ERMSD',
                       help='X-axis parameter for LANDSCAPE plot')
    parser.add_argument('--landscape-y', choices=['RMSD', 'ERMSD', 'TORSION', 'Q'], default='Q',
                       help='Y-axis parameter for LANDSCAPE plot')
    parser.add_argument('--no-progress', action='store_true',
                       help='Disable progress animation')
    
    # Plot settings arguments
    parser.add_argument('--rmsd-heavy-atoms', action='store_true', default=True,
                       help='Use heavy atoms only for RMSD calculation (default: True)')
    parser.add_argument('--ermsd-heavy-atoms', action='store_true', default=True,
                       help='Use heavy atoms only for eRMSD calculation (default: True)')
    parser.add_argument('--rog-selection', choices=['nucleic', 'all', 'backbone'], default='nucleic',
                       help='Atom selection for radius of gyration (default: nucleic)')
    parser.add_argument('--e2e-five-prime', choices=["C5'", 'P'], default="C5'",
                       help="5' terminal atom for end-to-end distance (default: C5')")
    parser.add_argument('--e2e-three-prime', choices=["C3'", 'P'], default="C3'",
                       help="3' terminal atom for end-to-end distance (default: C3')")
    parser.add_argument('--pca-components', type=int, default=10, metavar='N',
                       help='Number of PCA components to compute (default: 10)')
    parser.add_argument('--umap-neighbors', type=int, default=15, metavar='N',
                       help='Number of neighbors for UMAP (default: 15)')
    parser.add_argument('--umap-min-dist', type=float, default=0.1, metavar='DIST',
                       help='Minimum distance for UMAP (default: 0.1)')
    parser.add_argument('--tsne-perplexity', type=int, default=30, metavar='N',
                       help='Perplexity for t-SNE (default: 30)')
    parser.add_argument('--landscape-grid-size', type=int, default=65, metavar='N',
                       help='Grid size for landscape plot (default: 65)')
    parser.add_argument('--random-seed', type=int, default=42, metavar='SEED',
                       help='Random seed for reproducible results (default: 42)')

    args = parser.parse_args()

    all_analyses = ['RMSD', 'ERMSD', 'CONTACT_MAPS', 'TORSION',
                    'SEC_STRUCTURE', 'DOTBRACKET', 'ARC',
                    'LANDSCAPE', 'BASE_PAIRING', 'RADIUS_OF_GYRATION',
                    'END_TO_END_DISTANCE', 'PCA', 'UMAP', 'TSNE']

    if 'ALL' in args.analyses:
        args.analyses = all_analyses

    # Create frame options dictionary
    frame_options = {
        'n_frames': args.n_frames,
        'first_frame': args.first_frame,
        'last_frame': args.last_frame,
        'stride': args.stride
    }

    # Create landscape parameters dictionary
    landscape_params = {
        'x_param': args.landscape_x,
        'y_param': args.landscape_y
    }

    # Create plot settings dictionary
    plot_settings = {
        'rmsd': {
            'heavy_atom': args.rmsd_heavy_atoms,
            'title': 'RMSD Analysis',
            'color_scheme': 'viridis'
        },
        'ermsd': {
            'heavy_atom': args.ermsd_heavy_atoms,
            'title': 'eRMSD Analysis',
            'color_scheme': 'viridis'
        },
        'radius_of_gyration': {
            'selection': args.rog_selection,
            'title': 'Radius of Gyration Analysis',
            'show_components': True,
            'color_scheme': 'viridis'
        },
        'end_to_end_distance': {
            'five_prime_atom': args.e2e_five_prime,
            'three_prime_atom': args.e2e_three_prime,
            'title': 'End-to-End Distance Analysis',
            'color_scheme': 'viridis'
        },
        'pca': {
            'n_components': args.pca_components,
            'title': 'PCA Analysis',
            'show_variance': True,
            'color_scheme': 'viridis'
        },
        'umap': {
            'n_neighbors': args.umap_neighbors,
            'min_dist': args.umap_min_dist,
            'random_state': args.random_seed,
            'title': 'UMAP Analysis',
            'color_scheme': 'viridis'
        },
        'tsne': {
            'perplexity': args.tsne_perplexity,
            'random_state': args.random_seed,
            'title': 't-SNE Analysis',
            'color_scheme': 'viridis'
        },
        'landscape': {
            'stride': 1,  # This is handled separately by landscape_stride in form
            'grid_size': args.landscape_grid_size,
            'first_dimension': args.landscape_x,
            'second_dimension': args.landscape_y,
            'title': 'Energy Landscape',
            'color_scheme': 'viridis'
        }
    }

    # Initialize the client
    client = RNATrajectoryAnalysis(base_url=args.server, verify_ssl=not args.no_verify_ssl)

    # Upload files and start analysis
    print("Uploading files and starting analysis...")
    upload_result = client.upload_files(
        native_pdb_path=args.topology,
        traj_xtc_path=args.trajectory,
        analysis_options=args.analyses,
        frame_options=frame_options,
        landscape_params=landscape_params,
        plot_settings=plot_settings
    )

    if upload_result['status'] != 'success':
        print(f"Error uploading files: {upload_result['message']}")
        return

    # Wait for analysis to complete
    print("Waiting for analysis to complete...")
    status_result = client.get_analysis_status(
        max_retries=60,
        retry_interval=5,
        show_progress=not args.no_progress
    )

    if status_result['status'] != 'success':
        print(f"Error during analysis: {status_result['message']}")
        return

    # Print the results URL
    print("\n\n\nAnalysis complete!")
    print(f"Access raw data at: {status_result['results_url']}")

if __name__ == "__main__":
    main()
