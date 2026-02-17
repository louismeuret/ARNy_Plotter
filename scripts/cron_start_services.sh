#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"
export PYTHONPATH="$PROJECT_DIR:$PYTHONPATH"

cd "$PROJECT_DIR" || exit 1

mkdir -p logs

mkdir -p /tmp/rna_plot_services

is_service_running() {
    local pidfile="$1"
    if [ -f "$pidfile" ]; then
        local pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            return 0  # Service is running
        else
            rm -f "$pidfile"  # Clean up stale PID file
        fi
    fi
    return 1  # Service is not running
}

if ! pgrep -f "redis-server" > /dev/null; then
    echo "Starting Redis server..."
    redis-server --daemonize yes --logfile "$PROJECT_DIR/logs/redis.log"
fi

CELERY_PID="/tmp/rna_plot_services/celery_worker.pid"
if ! is_service_running "$CELERY_PID"; then
    echo "Starting Celery worker..."
    nohup celery -A src.core.tasks worker --loglevel=INFO --concurrency=5 --pidfile="$CELERY_PID" > logs/celery_worker.log 2>&1 &
    sleep 2
fi

FLOWER_PID="/tmp/rna_plot_services/flower.pid"
if ! is_service_running "$FLOWER_PID"; then
    echo "Starting Flower..."
    nohup celery --broker=redis://localhost:6379/0 flower --pidfile="$FLOWER_PID" > logs/flower.log 2>&1 &
    sleep 2
fi

GUNICORN_PID="/tmp/rna_plot_services/gunicorn.pid"
if ! is_service_running "$GUNICORN_PID"; then
    echo "Starting Gunicorn..."
    cd src/core
    nohup gunicorn --bind 0.0.0.0:4242 --workers 1 --worker-class eventlet --threads 4 --timeout 0 --pid="$GUNICORN_PID" app:app > ../../logs/gunicorn.log 2>&1 &
    cd "$PROJECT_DIR"
    sleep 2
fi

echo "All services startup completed. Check logs for details."
