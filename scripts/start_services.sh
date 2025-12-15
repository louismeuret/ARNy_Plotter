#!/bin/bash

# Start Celery worker in the background and store output
#
cd ..
nohup celery -A src.core.tasks worker --loglevel=INFO --concurrency=5 > logs/celery_worker_output.log 2>&1 &

# Start Flower in the background and store output
nohup celery --broker=redis://localhost:6379/0 flower > logs/flower_output.log 2>&1 &

# Start Gunicorn with eventlet and store output
nohup gunicorn --bind 0.0.0.0:4242 --workers 1 --worker-class eventlet --threads 4 --timeout 0 --chdir src/core app:app > logs/gunicorn.log 2>&1 &
echo "All services are started in the background."
