#!/bin/bash

# Start Celery worker in the background and store output
nohup celery -A tasks_celery worker --loglevel=INFO --concurrency=10 > celery_worker_output.log 2>&1 &

# Start Flower in the background and store output
nohup celery --broker=redis://localhost:6379/0 flower > flower_output.log 2>&1 &

# Start Gunicorn with eventlet and store output
nohup gunicorn --worker-class eventlet --access-logfile '-' --error-logfile '-' --timeout 600 -b 127.0.0.1:4242 -w 1 'app7:app' > gunicorn_output.log 2>&1 &

echo "All services are started in the background."
