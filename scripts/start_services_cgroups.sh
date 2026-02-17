#!/bin/bash

# Create cgroups for resource management
create_cgroup() {
    local cgroup_name=$1
    local memory_limit=$2
    local cpu_limit=$3
    
    sudo mkdir -p /sys/fs/cgroup/memory/$cgroup_name
    sudo mkdir -p /sys/fs/cgroup/cpu/$cgroup_name
    
    if [ -n "$memory_limit" ]; then
        echo $memory_limit | sudo tee /sys/fs/cgroup/memory/$cgroup_name/memory.limit_in_bytes
    fi
    
    if [ -n "$cpu_limit" ]; then
        echo $cpu_limit | sudo tee /sys/fs/cgroup/cpu/$cgroup_name/cpu.cfs_quota_us
        echo 100000 | sudo tee /sys/fs/cgroup/cpu/$cgroup_name/cpu.cfs_period_us
    fi
}

# Create cgroup for celery with 10GB RAM and 16 cores
create_cgroup "celery_worker" "10737418240" "1600000"

cd ..

# Function to start process in cgroup
start_in_cgroup() {
    local cgroup_name=$1
    shift
    local cmd="$@"
    
    # Start the process
    eval "$cmd" &
    local pid=$!
    
    # Add process to cgroups
    echo $pid | sudo tee /sys/fs/cgroup/memory/$cgroup_name/cgroup.procs
    echo $pid | sudo tee /sys/fs/cgroup/cpu/$cgroup_name/cgroup.procs
}

# Start Celery worker in cgroup with resource limits
start_in_cgroup "celery_worker" "nohup celery -A src.core.tasks worker --loglevel=INFO --concurrency=5 > logs/celery_worker_output.log 2>&1"

# Start Flower in the background without resource limits
nohup celery --broker=redis://localhost:6379/0 flower > logs/flower_output.log 2>&1 &

# Start Gunicorn with eventlet without resource limits
nohup gunicorn --bind 0.0.0.0:4242 --workers 1 --worker-class eventlet --threads 4 --timeout 0 --chdir src/core app:app > logs/gunicorn.log 2>&1 &

echo "All services are started in the background with cgroups resource management."
echo "Celery worker is limited to 10GB RAM and 16 CPU cores."