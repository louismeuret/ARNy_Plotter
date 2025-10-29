#!/usr/bin/env python3
"""
Installer script that checks for git, clones a repository, and installs requirements
from both pip and conda, plus downloads a Redis server with progress tracking.
"""
import os
import sys
import subprocess
import platform
import shutil
from pathlib import Path
import json
import argparse
import urllib.request
import time
import re


class Installer:
    def __init__(self, repo_url, branch=None, install_path=None, redis_url=None):
        """
        Initialize installer with configuration

        Args:
            repo_url (str): URL of git repository to clone
            branch (str): Git branch to clone (optional)
            install_path (str): Path to install the program to
            redis_url (str): URL to download Redis server from
        """
        self.repo_url = repo_url
        self.branch = branch
        self.redis_url = redis_url or "https://github.com/microsoftarchive/redis/releases/download/win-3.2.100/Redis-x64-3.2.100.zip"

        # Determine installation path
        if install_path:
            self.install_path = Path(install_path)
        else:
            repo_name = self.repo_url.split("/")[-1].replace(".git", "")
            home = Path.home()
            self.install_path = home / f".{repo_name}"

        # Determine system info
        self.system = platform.system().lower()
        self.is_windows = self.system == "windows"
        self.is_mac = self.system == "darwin"
        self.is_linux = self.system == "linux"

        # Temp directory for downloads
        self.temp_dir = Path.cwd() / "temp"
        self.temp_dir.mkdir(exist_ok=True)

        # Repository directory
        self.repo_dir = self.temp_dir / self.repo_url.split("/")[-1].replace(".git", "")

        # Redis directory
        self.redis_dir = self.install_path / "redis"

    def run_command(self, command, shell=False, env=None, check=True):
        """Run a command and return output"""
        print(f"Running: {' '.join(command) if isinstance(command, list) else command}")

        try:
            if shell:
                process = subprocess.run(command, shell=True,
                                        capture_output=True, text=True, env=env,
                                        check=check)
            else:
                process = subprocess.run(command, check=check,
                                        capture_output=True, text=True, env=env)

            if process.stdout and len(process.stdout.strip()) > 0:
                print(process.stdout)

            return process.stdout
        except subprocess.CalledProcessError as e:
            print(f"Error executing command: {e}")
            print(f"Output: {e.stdout}")
            print(f"Error: {e.stderr}")
            return None

    def verify_conda_env(self):
        """Verify that we're running in a conda environment"""
        print("\n=== Verifying conda environment ===")

        # Check if CONDA_PREFIX environment variable is set
        conda_prefix = os.environ.get("CONDA_PREFIX")
        if not conda_prefix:
            print("Error: Not running in a conda environment.")
            print("Please activate a conda environment before running this script.")
            print("Example: conda activate my_env")
            return False

        print(f"Running in conda environment: {os.path.basename(conda_prefix)}")
        return True

    def check_git(self):
        """Check if git is installed and available"""
        print("\n=== Checking for git installation ===")

        try:
            result = subprocess.run(["git", "--version"],
                                   capture_output=True, text=True, check=False)
            if result.returncode == 0:
                print(f"Found git: {result.stdout.strip()}")
                return True
            else:
                print("Git command returned an error")
                return False
        except FileNotFoundError:
            print("Git not found in PATH. Please install git and try again.")
            print("Download from: https://git-scm.com/downloads")
            return False

    def clone_repository(self):
        """Clone the git repository"""
        print(f"\n=== Cloning repository from {self.repo_url} ===")

        # Remove repository directory if it already exists
        if self.repo_dir.exists():
            print(f"Removing existing repository at {self.repo_dir}")
            shutil.rmtree(self.repo_dir)

        # Prepare git clone command
        clone_cmd = ["git", "clone", self.repo_url, str(self.repo_dir)]

        # Add branch if specified
        if self.branch:
            clone_cmd.extend(["-b", self.branch])

        # Clone the repository
        if self.run_command(clone_cmd) is not None:
            print(f"Repository cloned successfully to {self.repo_dir}")
            return True
        else:
            print("Failed to clone repository")
            return False

    def download_with_progress(self, url, destination):
        """Download a file with progress reporting"""
        print(f"Downloading {url} to {destination}")

        def progress_callback(blocks_transferred, block_size, total_size):
            if total_size > 0:
                percentage = min(blocks_transferred * block_size * 100 / total_size, 100)
                progress_size = blocks_transferred * block_size
                progress_size_mb = progress_size / (1024 * 1024)
                total_size_mb = total_size / (1024 * 1024)
                sys.stdout.write(f"\r{percentage:.1f}% ({progress_size_mb:.1f}MB / {total_size_mb:.1f}MB)")
                sys.stdout.flush()

        try:
            urllib.request.urlretrieve(url, destination, progress_callback)
            print("\nDownload completed!")
            return True
        except Exception as e:
            print(f"\nError downloading file: {e}")
            return False

    def download_redis(self):
        """Download Redis server"""
        print("\n=== Downloading Redis server ===")

        # Create Redis directory
        self.redis_dir.mkdir(parents=True, exist_ok=True)

        # Determine file extension based on URL
        file_extension = os.path.splitext(self.redis_url)[1]
        redis_archive = self.temp_dir / f"redis{file_extension}"

        # Download Redis
        if not self.download_with_progress(self.redis_url, redis_archive):
            return False

        # Extract or install Redis based on file type
        print("\n=== Installing Redis server ===")

        if file_extension == ".zip":
            import zipfile
            try:
                with zipfile.ZipFile(redis_archive, 'r') as zip_ref:
                    zip_ref.extractall(self.redis_dir)
                print(f"Redis extracted to {self.redis_dir}")
                return True
            except Exception as e:
                print(f"Error extracting Redis: {e}")
                return False
        elif file_extension == ".tar.gz" or file_extension == ".tgz":
            import tarfile
            try:
                with tarfile.open(redis_archive, 'r:gz') as tar_ref:
                    tar_ref.extractall(self.redis_dir)
                print(f"Redis extracted to {self.redis_dir}")
                return True
            except Exception as e:
                print(f"Error extracting Redis: {e}")
                return False
        else:
            # Just copy the file if it's an executable
            try:
                shutil.copy2(redis_archive, self.redis_dir / f"redis-server{file_extension}")
                os.chmod(self.redis_dir / f"redis-server{file_extension}", 0o755)  # Make executable
                print(f"Redis copied to {self.redis_dir}")
                return True
            except Exception as e:
                print(f"Error copying Redis: {e}")
                return False

    def install_requirements(self):
        """Install requirements from requirements.txt and conda_program.txt"""
        print("\n=== Installing requirements ===")

        # Check for requirements.txt
        req_path = self.repo_dir / "installation/requirements.txt"
        print(req_path)
        if req_path.exists():
            print("Installing pip requirements from requirements.txt")
            cmd = [sys.executable, "-m", "pip", "install", "-r", str(req_path)]
            if self.run_command(cmd) is None:
                print("Failed to install pip requirements")
                return False
        else:
            print("No requirements.txt found, skipping pip requirements")

        # Check for conda_program.txt
        conda_req_path = self.repo_dir / "installation/conda-forge-packages.txt"
        if conda_req_path.exists():
            print("Installing conda requirements")

            # Read the conda requirements file
            with open(conda_req_path, 'r') as f:
                conda_packages = [line.strip() for line in f if line.strip() and not line.startswith('#')]

            if conda_packages:
                cmd = ["conda", "install", "-y"] + conda_packages
                if self.run_command(cmd) is None:
                    print("Failed to install conda requirements")
                    return False
        else:
            print("No conda_program.txt found, skipping conda requirements")

        print("All requirements installed successfully")
        return True

    def copy_program_files(self):
        """Copy program files to installation directory"""
        print(f"\n=== Installing program to {self.install_path} ===")

        # Create installation directory
        self.install_path.mkdir(parents=True, exist_ok=True)

        # Copy all files from repository directory except unnecessary ones
        exclude_dirs = ['.git', '__pycache__', '.github']

        for item in self.repo_dir.iterdir():
            if item.name in exclude_dirs:
                continue

            destination = self.install_path / item.name

            if item.is_dir():
                if destination.exists():
                    shutil.rmtree(destination)
                shutil.copytree(item, destination)
            else:
                shutil.copy2(item, destination)

        print(f"Program files copied to {self.install_path}")
        return True

    def create_launcher(self):
        """Create launcher script/shortcut"""
        print("\n=== Creating launcher ===")

        # Find main script - assuming it's in the root directory and has a main function
        main_script = None
        for file in self.install_path.glob("*.py"):
            with open(file, "r") as f:
                content = f.read()
                if "def main" in content or "if __name__ == '__main__'" in content:
                    main_script = file.name
                    break

        if not main_script:
            print("Could not find main script. Please create launcher manually.")
            return False

        # Get current conda environment name
        conda_env = os.path.basename(os.environ.get("CONDA_PREFIX", "base"))

        # Create launcher script based on platform
        if self.is_windows:
            launcher_path = self.install_path / "run_program.bat"
            with open(launcher_path, "w") as f:
                f.write(f"@echo off\r\n")
                f.write(f"if \"%1\" == \"--stop\" goto :stop\r\n")
                f.write(f"call conda activate {conda_env}\r\n")
                f.write(f"if not exist logs mkdir logs\r\n")

                # Start Redis
                f.write(f"start \"redis\" cmd /c redis-server\r\n")

                # Start Celery Worker
                f.write(f"start \"celery_worker\" cmd /c celery -A tasks_celery worker --loglevel=INFO --concurrency=10 > logs\\celery_worker_output.log 2>&1\r\n")

                # Start Flower
                f.write(f"start \"flower\" cmd /c celery --broker=redis://localhost:6379/0 flower > logs\\flower_output.log 2>&1\r\n")

                # Start Gunicorn (use alternative if gunicorn not available on Windows)
                f.write(f"start \"gunicorn\" cmd /c gunicorn --worker-class eventlet --access-logfile - --error-logfile - --timeout 600 -b 127.0.0.1:4242 -w 1 app7:app > logs\\gunicorn_output.log 2>&1\r\n")

                f.write(f"goto :eof\r\n\r\n")

                # Stop section
                f.write(f":stop\r\n")
                f.write(f"echo Stopping services...\r\n")
                f.write(f"taskkill /F /IM redis-server.exe >nul 2>&1\r\n")
                f.write(f"taskkill /F /IM celery.exe >nul 2>&1\r\n")
                f.write(f"taskkill /F /IM gunicorn.exe >nul 2>&1\r\n")
                f.write(f"rem Flower usually runs under python, so we kill that too carefully\r\n")
                f.write(f"taskkill /F /FI \"WINDOWTITLE eq flower\" >nul 2>&1\r\n")
                f.write(f"taskkill /F /IM python.exe >nul 2>&1\r\n")
                f.write(f"exit /B\r\n")


        else:  # Linux/Mac
            launcher_path = self.install_path / "run_program.sh"
            with open(launcher_path, "w") as f:
                f.write("#!/bin/bash\n\n")
                f.write("if [[ \"$1\" == \"--stop\" ]]; then\n")
                f.write("    echo \"Stopping services...\"\n")
                f.write("    pkill -f 'redis-server'\n")
                f.write("    pkill -f 'celery -A tasks_celery worker'\n")
                f.write("    pkill -f 'celery --broker=redis://localhost:6379/0 flower'\n")
                f.write("    pkill -f 'gunicorn --worker-class eventlet'\n")
                f.write("    exit 0\n")
                f.write("fi\n\n")

                f.write(f"eval \"$(conda shell.bash hook)\"\n")
                f.write(f"conda activate {conda_env}\n")
                f.write(f"mkdir -p logs\n")
                f.write(f"nohup redis-server > /dev/null 2>&1 &\n")
                f.write(f"nohup celery -A tasks_celery worker --loglevel=INFO --concurrency=10 > logs/celery_worker_output.log 2>&1 &\n")
                f.write(f"nohup celery --broker=redis://localhost:6379/0 flower > logs/flower_output.log 2>&1 &\n")
                f.write(f"nohup gunicorn --worker-class eventlet --access-logfile '-' --error-logfile '-' --timeout 600 -b 127.0.0.1:4242 -w 1 'app7:app' > logs/gunicorn_output.log 2>&1 &\n")

                #f.write(f"python \"{self.install_path / main_script}\" \"$@\"\n")

            # Make executable
            os.chmod(launcher_path, 0o755)

        print(f"Launcher created at {launcher_path}")
        return True

    def run_installation(self):
        """Run the full installation process"""
        print("\n=== Starting installation ===")

        if not self.verify_conda_env():
            print("Please activate a conda environment and try again.")
            return False

        steps = [
            ("Checking git installation", self.check_git),
            ("Cloning repository", self.clone_repository),
            ("Installing requirements", self.install_requirements),
            ("Copying program files", self.copy_program_files),
            ("Creating launcher", self.create_launcher)
        ]

        for step_name, step_func in steps:
            print(f"\n--- {step_name} ---")
            if not step_func():
                print(f"Error during {step_name}. Installation failed.")
                return False

        # Clean up temp directory
        print("\n=== Cleaning up temporary files ===")
        shutil.rmtree(self.temp_dir)

        print("\n=== Installation completed successfully! ===")
        print(f"Program installed to: {self.install_path}")
        print(f"Redis server installed to: {self.redis_dir}")

        if self.is_windows:
            print(f"Run the program with: {self.install_path}/run_program.bat")
        else:
            print(f"Run the program with: {self.install_path}/run_program.sh")

        return True


def main():
    parser = argparse.ArgumentParser(description='Install Python program from git repository')
    parser.add_argument('--repo', required=True, help='Git repository URL to clone')
    parser.add_argument('--branch', help='Git branch to clone')
    parser.add_argument('--install-path', help='Custom installation path')
    parser.add_argument('--redis-url', help='URL to download Redis server from')
    args = parser.parse_args()

    installer = Installer(
        repo_url=args.repo,
        branch=args.branch,
        install_path=args.install_path,
        redis_url=args.redis_url
    )

    installer.run_installation()


if __name__ == "__main__":
    main()
