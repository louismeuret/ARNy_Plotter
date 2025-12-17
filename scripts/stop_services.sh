#!/bin/bash

# Script to stop all RNA Plot services

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# PID file directory
PID_DIR="/tmp/rna_plot_services"

echo "Stopping RNA Plot services..."

# Function to stop a service by PID file
stop_service() {
    local service_name="$1"
    local pidfile="$2"
    
    if [ -f "$pidfile" ]; then
        local pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping $service_name (PID: $pid)..."
            kill -TERM "$pid"
            
            # Wait for graceful shutdown, then force kill if necessary
            local count=0
            while kill -0 "$pid" 2>/dev/null && [ $count -lt 10 ]; do
                sleep 1
                count=$((count + 1))
            done
            
            if kill -0 "$pid" 2>/dev/null; then
                echo "Force killing $service_name..."
                kill -9 "$pid"
            fi
            
            rm -f "$pidfile"
            echo "$service_name stopped."
        else
            echo "$service_name was not running."
            rm -f "$pidfile"
        fi
    else
        echo "No PID file found for $service_name."
    fi
}

# Stop Gunicorn
stop_service "Gunicorn" "$PID_DIR/gunicorn.pid"

# Stop Flower
stop_service "Flower" "$PID_DIR/flower.pid"

# Stop Celery worker
stop_service "Celery worker" "$PID_DIR/celery_worker.pid"

# Stop any remaining celery processes
echo "Checking for remaining Celery processes..."
CELERY_PIDS=$(pgrep -f "celery.*worker")
if [ -n "$CELERY_PIDS" ]; then
    echo "Stopping remaining Celery processes: $CELERY_PIDS"
    echo "$CELERY_PIDS" | xargs kill -TERM
    sleep 2
    # Force kill if still running
    CELERY_PIDS=$(pgrep -f "celery.*worker")
    if [ -n "$CELERY_PIDS" ]; then
        echo "Force killing remaining Celery processes: $CELERY_PIDS"
        echo "$CELERY_PIDS" | xargs kill -9
    fi
fi

# Stop any remaining Flower processes
echo "Checking for remaining Flower processes..."
FLOWER_PIDS=$(pgrep -f "flower")
if [ -n "$FLOWER_PIDS" ]; then
    echo "Stopping remaining Flower processes: $FLOWER_PIDS"
    echo "$FLOWER_PIDS" | xargs kill -TERM
    sleep 2
    # Force kill if still running
    FLOWER_PIDS=$(pgrep -f "flower")
    if [ -n "$FLOWER_PIDS" ]; then
        echo "Force killing remaining Flower processes: $FLOWER_PIDS"
        echo "$FLOWER_PIDS" | xargs kill -9
    fi
fi

# Stop any remaining Gunicorn processes (for this project)
echo "Checking for remaining Gunicorn processes..."
GUNICORN_PIDS=$(pgrep -f "gunicorn.*app:app")
if [ -n "$GUNICORN_PIDS" ]; then
    echo "Stopping remaining Gunicorn processes: $GUNICORN_PIDS"
    echo "$GUNICORN_PIDS" | xargs kill -TERM
    sleep 2
    # Force kill if still running
    GUNICORN_PIDS=$(pgrep -f "gunicorn.*app:app")
    if [ -n "$GUNICORN_PIDS" ]; then
        echo "Force killing remaining Gunicorn processes: $GUNICORN_PIDS"
        echo "$GUNICORN_PIDS" | xargs kill -9
    fi
fi

# Optionally stop Redis (commented out as it might be used by other services)
# echo "Stopping Redis server..."
# redis-cli shutdown

# Clean up PID directory
rm -rf "$PID_DIR"

echo "All RNA Plot services stopped."