import requests
import time
import json
import os
import zipfile
import io
from urllib.parse import urljoin

class RNATrajectoryAnalysis:
    """
    Client for interacting with the RNA trajectory analysis web service.
    """
    
    def __init__(self, base_url="http://localhost:4242"):
        """
        Initialize the RNA trajectory analysis client.
        
        Args:
            base_url (str): The base URL of the RNA trajectory analysis service
        """
        self.base_url = base_url
        self.session_id = None
        self.native_pdb = None
        self.traj_xtc = None
    
    def start_session(self):
        """
        Start a new analysis session.
        
        Returns:
            str: The session ID for the new session
        """
        response = requests.get(self.base_url)
        # Extract session ID from response
        # This is a simplified approach - in reality we might need to parse HTML
        # or rely on a specific API endpoint that returns a session ID
        session_id = requests.get(urljoin(self.base_url, "/")).cookies.get("session")
        if not session_id:
            # Alternative approach - make a POST request to get a session ID
            response = requests.post(urljoin(self.base_url, "/"))
            session_id = response.cookies.get("session")
        
        self.session_id = session_id
        return session_id
    
    def upload_files(self, native_pdb_path, traj_xtc_path, analysis_options=None, frame_options=None):
        """
        Upload PDB and XTC files for analysis.
        
        Args:
            native_pdb_path (str): Path to the native PDB file
            traj_xtc_path (str): Path to the trajectory XTC file
            analysis_options (list): List of analysis types to perform (e.g., ["RMSD", "ERMSD", "TORSION"])
            frame_options (dict): Dict with frame selection options (e.g., {"n_frames": 10, "first_frame": 1, "last_frame": 100, "stride": 1})
        
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
        
        # Prepare files
        files = {
            "nativePdb": (os.path.basename(native_pdb_path), open(native_pdb_path, "rb"), "chemical/x-pdb"),
            "trajXtc": (os.path.basename(traj_xtc_path), open(traj_xtc_path, "rb"), "application/octet-stream")
        }
        
        # Store filenames for later use
        self.native_pdb = os.path.basename(native_pdb_path)
        self.traj_xtc = os.path.basename(traj_xtc_path)
        
        try:
            # Make the upload request
            response = requests.post(
                urljoin(self.base_url, "/upload-files"),
                files=files,
                data=form_data
            )
            
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
    
    def get_analysis_status(self, max_retries=30, retry_interval=2):
        """
        Check the status of the analysis and wait until it's complete.
        
        Args:
            max_retries (int): Maximum number of retries
            retry_interval (int): Interval between retries in seconds
            
        Returns:
            dict: Analysis results or error information
        """
        if not self.session_id or not self.native_pdb or not self.traj_xtc:
            return {"status": "error", "message": "No active analysis session"}
        
        # URL for the trajectory view
        view_url = urljoin(
            self.base_url, 
            f"/view-trajectory/{self.session_id}/{self.native_pdb}/{self.traj_xtc}"
        )
        
        # URL for retrieving results
        results_url = urljoin(
            self.base_url,
            f"/retrieve-results?session_id={self.session_id}"
        )
        
        # First, trigger the analysis process by accessing the view URL
        requests.get(view_url)
        
        # Then poll for results
        for attempt in range(max_retries):
            try:
                response = requests.get(results_url)
                
                if response.status_code == 200:
                    # Successful result
                    return {
                        "status": "success", 
                        "message": "Analysis complete",
                        "results_url": results_url,
                        "view_url": view_url
                    }
                elif response.status_code == 404:
                    # Results not ready yet
                    print(f"Analysis in progress... (attempt {attempt+1}/{max_retries})")
                    time.sleep(retry_interval)
                else:
                    # Unexpected status code
                    return {
                        "status": "error", 
                        "message": f"Got unexpected status code: {response.status_code}"
                    }
                    
            except Exception as e:
                return {"status": "error", "message": str(e)}
        
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
            response = requests.get(download_url)
            
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
            response = requests.get(download_url)
            
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
            "BASE_PAIRING"
        ]
