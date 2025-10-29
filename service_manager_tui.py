#!/usr/bin/env python3
import subprocess
import threading
import time
import os
import signal
import sys
import logging
from datetime import datetime
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
                "cmd": ["gunicorn", "--bind", "127.0.0.1:4242", "--workers", "1", 
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
        name_line = f"│ {service_id}. {name:<{box_width-6}} │"
        
        # Status line with color and PID if running
        if service["process"] and service["status"] == ServiceStatus.RUNNING:
            status_info = f"{status_text} (PID: {service['process'].pid})"
        else:
            status_info = status_text
        status_line = f"│ Status: {color}{status_info:<20}{reset} {'':>{box_width-30}} │"
        
        # Description line
        desc_line = f"│ {service['description']:<{box_width-4}} │"
        
        # Log file line
        log_line = f"│ Log: {service['log_file']:<{box_width-10}} │"
        
        # Command preview (truncated)
        cmd_preview = " ".join(service["cmd"])
        if len(cmd_preview) > box_width - 12:
            cmd_preview = cmd_preview[:box_width-15] + "..."
        cmd_line = f"│ Cmd: {cmd_preview:<{box_width-8}} │"
        
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
        print("  q : Quit                    h : Help")
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