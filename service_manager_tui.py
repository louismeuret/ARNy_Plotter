#!/usr/bin/env python3
import subprocess
import threading
import time
import os
import signal
import sys
import logging
import shutil
import json
from datetime import datetime, timedelta
from enum import Enum

class ServiceStatus(Enum):
    STOPPED = "red"
    RUNNING = "green" 
    ERROR = "yellow"

class ServiceManager:
    def __init__(self):
        # Create logs directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)
        
        self.services = {
            "1": {
                "name": "Celery Worker",
                "cmd": ["celery", "-A", "src.core.tasks", "worker", "--loglevel=INFO", "--concurrency=5"],
                "process": None,
                "status": ServiceStatus.STOPPED,
                "log_file": "logs/celery_worker.log",
                "description": "Handles background task processing (plots, calculations)"
            },
            "2": {
                "name": "Gunicorn Server", 
                "cmd": ["gunicorn", "--bind", "127.0.0.1:4242", "--workers", "5", 
                       "--worker-class", "eventlet", "--timeout", "600", "--chdir", "src/core", "app:app"],
                "process": None,
                "status": ServiceStatus.STOPPED,
                "log_file": "logs/gunicorn.log",
                "description": "Flask web server on port 4242"
            },
            "3": {
                "name": "Redis Server",
                "cmd": ["redis-server", "--logfile", "logs/redis.log"],
                "process": None,
                "status": ServiceStatus.STOPPED,
                "log_file": "logs/redis.log",
                "description": "Message broker for Celery tasks"
            },
            "4": {
                "name": "Celery Flower",
                "cmd": ["celery", "-A", "src.core.tasks", "--broker=redis://localhost:6379/0", "flower", "--port=5555"],
                "process": None,
                "status": ServiceStatus.STOPPED,
                "log_file": "logs/flower.log",
                "description": "Task monitoring interface on port 5555"
            }
        }
        self.running = True
        self.last_message = ""
        self.upload_path = "static/uploads"
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/service_manager.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('ServiceManager')
        self.logger.info("Service Manager initialized")
        
    def clear_screen(self):
        os.system('clear' if os.name == 'posix' else 'cls')
        
    def get_color_code(self, status):
        colors = {
            ServiceStatus.STOPPED: "\033[91m",    # Red
            ServiceStatus.RUNNING: "\033[92m",    # Green  
            ServiceStatus.ERROR: "\033[93m",      # Yellow
        }
        return colors.get(status, "\033[0m")
        
    def reset_color(self):
        return "\033[0m"
        
    def check_service_status(self, service_id):
        service = self.services[service_id]
        if service["process"] is None:
            service["status"] = ServiceStatus.STOPPED
        else:
            poll = service["process"].poll()
            if poll is None:
                service["status"] = ServiceStatus.RUNNING
            elif poll == 0:
                service["status"] = ServiceStatus.STOPPED
                service["process"] = None
            else:
                service["status"] = ServiceStatus.ERROR
                service["process"] = None
                
    def start_service(self, service_id):
        service = self.services[service_id]
        if service["process"] is not None:
            self.stop_service(service_id)
        
        self.last_message = f"Starting {service['name']}..."
        self.logger.info(f"Starting service {service_id}: {service['name']}")
        self.logger.info(f"Command: {' '.join(service['cmd'])}")
            
        try:
            # Open log files for stdout/stderr
            log_file = open(service["log_file"], "a")
            log_file.write(f"\n=== Service started at {datetime.now()} ===\n")
            log_file.flush()
            
            service["process"] = subprocess.Popen(
                service["cmd"],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid if os.name == 'posix' else None
            )
            
            time.sleep(1.5)  # Give process more time to start
            self.check_service_status(service_id)
            
            if service["status"] == ServiceStatus.RUNNING:
                self.last_message = f"{service['name']} started successfully"
                self.logger.info(f"Service {service_id} started with PID {service['process'].pid}")
            else:
                self.last_message = f"Failed to start {service['name']}"
                self.logger.error(f"Service {service_id} failed to start")
                
        except FileNotFoundError as e:
            service["status"] = ServiceStatus.ERROR
            service["process"] = None
            self.last_message = f"Command not found for {service['name']}: {e}"
            self.logger.error(f"Service {service_id} command not found: {e}")
        except Exception as e:
            service["status"] = ServiceStatus.ERROR  
            service["process"] = None
            self.last_message = f"Error starting {service['name']}: {e}"
            self.logger.error(f"Service {service_id} startup error: {e}")
            
    def stop_service(self, service_id):
        service = self.services[service_id]
        if service["process"] is not None:
            self.last_message = f"Stopping {service['name']}..."
            self.logger.info(f"Stopping service {service_id}: {service['name']} (PID: {service['process'].pid})")
            
            try:
                # Log shutdown
                with open(service["log_file"], "a") as log_file:
                    log_file.write(f"\n=== Service stopped at {datetime.now()} ===\n\n")
                
                if os.name == 'posix':
                    os.killpg(os.getpgid(service["process"].pid), signal.SIGTERM)
                else:
                    service["process"].terminate()
                service["process"].wait(timeout=5)
                
                self.last_message = f"{service['name']} stopped successfully"
                self.logger.info(f"Service {service_id} stopped gracefully")
                
            except subprocess.TimeoutExpired:
                self.logger.warning(f"Service {service_id} didn't stop gracefully, forcing kill")
                if os.name == 'posix':
                    os.killpg(os.getpgid(service["process"].pid), signal.SIGKILL)
                else:
                    service["process"].kill()
                self.last_message = f"{service['name']} force killed"
                
            except Exception as e:
                self.last_message = f"Error stopping {service['name']}: {e}"
                self.logger.error(f"Service {service_id} stop error: {e}")
                
            finally:
                service["process"] = None
                service["status"] = ServiceStatus.STOPPED
                
    def restart_service(self, service_id):
        self.stop_service(service_id)
        time.sleep(1)
        self.start_service(service_id)
        
    def draw_box(self, service_id):
        service = self.services[service_id]
        color = self.get_color_code(service["status"])
        reset = self.reset_color()
        
        status_text = service["status"].name
        name = service["name"]
        
        # Box drawing with wider width for more info
        box_width = 80
        top_line = "┌" + "─" * (box_width - 2) + "┐"
        bottom_line = "└" + "─" * (box_width - 2) + "┘"
        
        # Service name and ID line
        name_line = f"│ {service_id}. {name:<{box_width-7}} │"
        
        # Status line with color and PID if running
        if service["process"] and service["status"] == ServiceStatus.RUNNING:
            status_info = f"{status_text} (PID: {service['process'].pid})"
            size_width = 35
        else:
            status_info = status_text
            size_width = 33
        status_line = f"│ Status: {color}{status_info:<20}{reset} {'':>{box_width-size_width}} │"
        
        # Description line
        desc_line = f"│ {service['description']:<{box_width-4}} │"
        
        # Log file line
        log_line = f"│ Log: {service['log_file']:<{box_width-9}} │"
        
        # Command preview (truncated)
        cmd_preview = " ".join(service["cmd"])
        if len(cmd_preview) > box_width - 12:
            cmd_preview = cmd_preview[:box_width-15] + "..."
        cmd_line = f"│ Cmd: {cmd_preview:<{box_width-9}} │"
        
        return [top_line, name_line, status_line, desc_line, log_line, cmd_line, bottom_line]
        
    def display_ui(self):
        self.clear_screen()
        
        print("=" * 85)
        print("                           ARNy PLOTTER SERVICE MANAGER")
        print("=" * 85)
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if self.last_message:
            print(f"Last action: {self.last_message}")
        print()
        
        # Update all service statuses
        for service_id in self.services:
            self.check_service_status(service_id)
            
        # Display all service boxes
        for service_id in sorted(self.services.keys()):
            box_lines = self.draw_box(service_id)
            for line in box_lines:
                print(line)
            print()
        
        # Show quick status summary
        running_count = sum(1 for s in self.services.values() if s["status"] == ServiceStatus.RUNNING)
        total_count = len(self.services)
        print(f"Services running: {running_count}/{total_count}")
        print()
            
        print("Commands:")
        print("  [1-4] + s : Start service    [1-4] + t : Stop service")
        print("  [1-4] + r : Restart service  [1-4] + l : View log tail")
        print("  a : Start all               x : Stop all")
        print("  f : File manager            q : Quit")
        print("  h : Help")
        print()
        print("Press command: ", end="", flush=True)
        
    def show_help(self):
        self.clear_screen()
        print("=" * 85)
        print("                              SERVICE MANAGER HELP")
        print("=" * 85)
        print()
        print("Available Commands:")
        print("  [1-4] + s : Start individual service")
        print("  [1-4] + t : Stop individual service")
        print("  [1-4] + r : Restart individual service")
        print("  [1-4] + l : Show last 20 lines of service log")
        print("  a         : Start all services")
        print("  x         : Stop all services")
        print("  f         : File manager (upload sessions)")
        print("  q         : Quit service manager")
        print("  h         : Show this help")
        print()
        print("Service Details:")
        for service_id, service in self.services.items():
            print(f"  {service_id}. {service['name']}: {service['description']}")
        print()
        print("Log files are stored in the 'logs/' directory.")
        print("Press Enter to return to main screen...")
        input()
    
    def show_log_tail(self, service_id):
        service = self.services[service_id]
        self.clear_screen()
        print(f"Last 20 lines of {service['name']} log:")
        print("=" * 60)
        try:
            with open(service["log_file"], "r") as f:
                lines = f.readlines()
                for line in lines[-20:]:
                    print(line.rstrip())
        except FileNotFoundError:
            print(f"Log file {service['log_file']} not found.")
        except Exception as e:
            print(f"Error reading log: {e}")
        print("=" * 60)
        print("Press Enter to return to main screen...")
        input()

    def get_upload_sessions(self):
        """Get list of upload sessions with metadata"""
        sessions = []
        if not os.path.exists(self.upload_path):
            return sessions
        
        for session_id in os.listdir(self.upload_path):
            session_path = os.path.join(self.upload_path, session_id)
            if os.path.isdir(session_path):
                try:
                    # Get directory size
                    size = sum(os.path.getsize(os.path.join(dirpath, filename))
                              for dirpath, dirnames, filenames in os.walk(session_path)
                              for filename in filenames)
                    
                    # Get creation time
                    creation_time = datetime.fromtimestamp(os.path.getctime(session_path))
                    
                    # Try to get session metadata
                    session_data_path = os.path.join(session_path, "session_data.json")
                    plots = []
                    files = {}
                    if os.path.exists(session_data_path):
                        try:
                            with open(session_data_path, 'r') as f:
                                data = json.load(f)
                                plots = data.get('selected_plots', [])
                                files = data.get('files', {})
                        except:
                            pass
                    
                    sessions.append({
                        'id': session_id,
                        'path': session_path,
                        'size': size,
                        'size_mb': size / (1024 * 1024),
                        'created': creation_time,
                        'age_days': (datetime.now() - creation_time).days,
                        'plots': plots,
                        'files': files
                    })
                except Exception as e:
                    self.logger.warning(f"Error processing session {session_id}: {e}")
        
        return sorted(sessions, key=lambda x: x['created'], reverse=True)

    def show_upload_sessions(self):
        """Display upload sessions with management options"""
        self.clear_screen()
        print("=" * 85)
        print("                              UPLOAD SESSION MANAGER")
        print("=" * 85)
        
        sessions = self.get_upload_sessions()
        if not sessions:
            print("No upload sessions found.")
            print("Press Enter to return...")
            input()
            return
        
        total_size_mb = sum(s['size_mb'] for s in sessions)
        print(f"Total sessions: {len(sessions)} | Total size: {total_size_mb:.1f} MB")
        print()
        
        print(f"{'#':<3} {'Session ID':<36} {'Age':<8} {'Size':<10} {'Plots':<15} {'Files'}")
        print("-" * 85)
        
        for i, session in enumerate(sessions[:20]):  # Show first 20
            plots_str = f"{len(session['plots'])} plots" if session['plots'] else "No plots"
            files_str = ", ".join([f"{k}: {v}" for k, v in session['files'].items()][:2])
            if len(files_str) > 25:
                files_str = files_str[:22] + "..."
            
            print(f"{i+1:<3} {session['id']:<36} {session['age_days']}d {session['size_mb']:<8.1f}MB {plots_str:<15} {files_str}")
        
        if len(sessions) > 20:
            print(f"... and {len(sessions) - 20} more sessions")
        
        print()
        print("Commands:")
        print("  [1-20] + d : Delete session     [1-20] + i : Show session info")
        print("  old <days> : Delete sessions older than N days")
        print("  size <mb>  : Delete sessions larger than N MB")
        print("  all        : Delete ALL sessions (DANGEROUS!)")
        print("  q          : Return to main menu")
        print()
        
        while True:
            cmd = input("Enter command: ").strip().lower()
            
            if cmd == 'q':
                break
            elif cmd == 'all':
                if self.confirm_delete_all():
                    self.delete_all_sessions()
                    break
            elif cmd.startswith('old '):
                try:
                    days = int(cmd.split()[1])
                    self.delete_old_sessions(days)
                    break
                except:
                    print("Invalid format. Use: old <days>")
            elif cmd.startswith('size '):
                try:
                    mb = float(cmd.split()[1])
                    self.delete_large_sessions(mb)
                    break
                except:
                    print("Invalid format. Use: size <mb>")
            elif len(cmd) >= 2 and cmd[-1] in ['d', 'i'] and cmd[:-1].isdigit():
                idx = int(cmd[:-1]) - 1
                action = cmd[-1]
                if 0 <= idx < min(len(sessions), 20):
                    if action == 'd':
                        self.delete_session(sessions[idx])
                        break
                    elif action == 'i':
                        self.show_session_info(sessions[idx])
                        break
                else:
                    print(f"Invalid session number. Use 1-{min(len(sessions), 20)}")
            else:
                print("Invalid command")

    def show_session_info(self, session):
        """Show detailed information about a session"""
        self.clear_screen()
        print(f"Session Information: {session['id']}")
        print("=" * 60)
        print(f"Created: {session['created'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Age: {session['age_days']} days")
        print(f"Size: {session['size_mb']:.2f} MB")
        print(f"Path: {session['path']}")
        print()
        
        if session['files']:
            print("Files:")
            for key, value in session['files'].items():
                print(f"  {key}: {value}")
            print()
        
        if session['plots']:
            print(f"Selected Plots ({len(session['plots'])}):")
            for plot in session['plots']:
                print(f"  - {plot}")
            print()
        
        # Show directory contents
        try:
            contents = []
            for root, dirs, files in os.walk(session['path']):
                for file in files:
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, session['path'])
                    size = os.path.getsize(file_path)
                    contents.append((rel_path, size))
            
            if contents:
                print(f"Directory Contents ({len(contents)} files):")
                contents.sort(key=lambda x: x[1], reverse=True)
                for rel_path, size in contents[:10]:  # Show top 10 largest files
                    print(f"  {rel_path:<40} {size/1024:.1f} KB")
                if len(contents) > 10:
                    print(f"  ... and {len(contents) - 10} more files")
        except Exception as e:
            print(f"Error reading directory: {e}")
        
        print()
        print("Press Enter to return...")
        input()

    def delete_session(self, session):
        """Delete a specific session"""
        print(f"\nDelete session {session['id']}?")
        print(f"Size: {session['size_mb']:.1f} MB, Age: {session['age_days']} days")
        confirm = input("Type 'yes' to confirm: ").strip().lower()
        
        if confirm == 'yes':
            try:
                shutil.rmtree(session['path'])
                self.last_message = f"Deleted session {session['id'][:8]}... ({session['size_mb']:.1f} MB)"
                self.logger.info(f"Deleted session {session['id']}")
                print("Session deleted successfully!")
            except Exception as e:
                self.last_message = f"Error deleting session: {e}"
                self.logger.error(f"Error deleting session {session['id']}: {e}")
                print(f"Error deleting session: {e}")
        else:
            print("Deletion cancelled.")
        
        time.sleep(1)

    def delete_old_sessions(self, days):
        """Delete sessions older than specified days"""
        sessions = self.get_upload_sessions()
        old_sessions = [s for s in sessions if s['age_days'] >= days]
        
        if not old_sessions:
            print(f"No sessions found older than {days} days.")
            time.sleep(1)
            return
        
        total_size = sum(s['size_mb'] for s in old_sessions)
        print(f"\nFound {len(old_sessions)} sessions older than {days} days")
        print(f"Total size to delete: {total_size:.1f} MB")
        
        confirm = input("Type 'yes' to delete all: ").strip().lower()
        if confirm == 'yes':
            deleted_count = 0
            for session in old_sessions:
                try:
                    shutil.rmtree(session['path'])
                    deleted_count += 1
                except Exception as e:
                    self.logger.error(f"Error deleting session {session['id']}: {e}")
            
            self.last_message = f"Deleted {deleted_count} old sessions ({total_size:.1f} MB)"
            self.logger.info(f"Deleted {deleted_count} sessions older than {days} days")
            print(f"Deleted {deleted_count} sessions successfully!")
        else:
            print("Deletion cancelled.")
        
        time.sleep(2)

    def delete_large_sessions(self, mb_threshold):
        """Delete sessions larger than specified MB"""
        sessions = self.get_upload_sessions()
        large_sessions = [s for s in sessions if s['size_mb'] >= mb_threshold]
        
        if not large_sessions:
            print(f"No sessions found larger than {mb_threshold} MB.")
            time.sleep(1)
            return
        
        total_size = sum(s['size_mb'] for s in large_sessions)
        print(f"\nFound {len(large_sessions)} sessions larger than {mb_threshold} MB")
        print(f"Total size to delete: {total_size:.1f} MB")
        
        confirm = input("Type 'yes' to delete all: ").strip().lower()
        if confirm == 'yes':
            deleted_count = 0
            for session in large_sessions:
                try:
                    shutil.rmtree(session['path'])
                    deleted_count += 1
                except Exception as e:
                    self.logger.error(f"Error deleting session {session['id']}: {e}")
            
            self.last_message = f"Deleted {deleted_count} large sessions ({total_size:.1f} MB)"
            self.logger.info(f"Deleted {deleted_count} sessions larger than {mb_threshold} MB")
            print(f"Deleted {deleted_count} sessions successfully!")
        else:
            print("Deletion cancelled.")
        
        time.sleep(2)

    def confirm_delete_all(self):
        """Confirm deletion of all sessions"""
        sessions = self.get_upload_sessions()
        total_size = sum(s['size_mb'] for s in sessions)
        
        print(f"\n⚠️  WARNING: This will delete ALL {len(sessions)} sessions!")
        print(f"Total size: {total_size:.1f} MB")
        print("This action cannot be undone!")
        
        confirm1 = input("Type 'DELETE ALL' to confirm: ").strip()
        if confirm1 != 'DELETE ALL':
            print("Deletion cancelled.")
            return False
        
        confirm2 = input("Are you absolutely sure? Type 'yes': ").strip().lower()
        return confirm2 == 'yes'

    def delete_all_sessions(self):
        """Delete all upload sessions"""
        sessions = self.get_upload_sessions()
        total_size = sum(s['size_mb'] for s in sessions)
        deleted_count = 0
        
        for session in sessions:
            try:
                shutil.rmtree(session['path'])
                deleted_count += 1
            except Exception as e:
                self.logger.error(f"Error deleting session {session['id']}: {e}")
        
        self.last_message = f"Deleted ALL {deleted_count} sessions ({total_size:.1f} MB)"
        self.logger.info(f"Deleted all {deleted_count} upload sessions")
        print(f"Deleted {deleted_count} sessions successfully!")
        time.sleep(2)

    def handle_command(self, cmd):
        cmd = cmd.strip().lower()
        
        if cmd == 'q':
            self.last_message = "Shutting down service manager..."
            self.logger.info("Service manager shutdown requested")
            return False
        elif cmd == 'h':
            self.show_help()
        elif cmd == 'a':
            self.last_message = "Starting all services..."
            self.logger.info("Starting all services")
            for service_id in sorted(self.services.keys()):
                self.start_service(service_id)
                time.sleep(0.5)  # Brief delay between starts
        elif cmd == 'x':
            self.last_message = "Stopping all services..."
            self.logger.info("Stopping all services")
            for service_id in reversed(sorted(self.services.keys())):
                self.stop_service(service_id)
                time.sleep(0.5)  # Brief delay between stops
        elif cmd == 'f':
            self.show_upload_sessions()
        elif len(cmd) == 2 and cmd[0] in self.services:
            service_id = cmd[0]
            action = cmd[1]
            
            if action == 's':
                self.start_service(service_id)
            elif action == 't':
                self.stop_service(service_id)
            elif action == 'r':
                self.restart_service(service_id)
            elif action == 'l':
                self.show_log_tail(service_id)
        else:
            self.last_message = f"Unknown command: {cmd}"
                
        return True
        
    def cleanup(self):
        for service_id in self.services:
            self.stop_service(service_id)
            
    def run(self):
        try:
            while self.running:
                self.display_ui()
                
                # Non-blocking input with timeout
                import select
                if os.name == 'posix':
                    if select.select([sys.stdin], [], [], 0.1) == ([sys.stdin], [], []):
                        cmd = sys.stdin.readline()
                        if not self.handle_command(cmd):
                            break
                else:
                    # Windows fallback - blocking input
                    try:
                        cmd = input()
                        if not self.handle_command(cmd):
                            break
                    except KeyboardInterrupt:
                        break
                        
                time.sleep(0.5)
                
        except KeyboardInterrupt:
            pass
        finally:
            self.cleanup()
            self.clear_screen()
            print("Service Manager stopped. All services have been terminated.")

if __name__ == "__main__":
    manager = ServiceManager()
    manager.run()
