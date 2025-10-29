#!/usr/bin/env python3
"""
Cleanup script for deleting files older than 10 days in the upload directory.

This script removes upload sessions that are older than 10 days to free up disk space.
By default it targets 'static/uploads' directory but can be configured.

Usage:
    python scripts/cleanup_old_files.py [--days N] [--path PATH] [--dry-run] [--verbose]

Examples:
    python scripts/cleanup_old_files.py                    # Delete files older than 10 days
    python scripts/cleanup_old_files.py --days 7           # Delete files older than 7 days  
    python scripts/cleanup_old_files.py --dry-run          # Show what would be deleted without doing it
    python scripts/cleanup_old_files.py --verbose          # Show detailed output
"""

import os
import shutil
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path


def setup_logging(verbose=False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/cleanup.log', mode='a')
        ]
    )
    return logging.getLogger('cleanup')


def get_directory_size(path):
    """Calculate total size of directory in bytes"""
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                try:
                    total_size += os.path.getsize(file_path)
                except (OSError, FileNotFoundError):
                    pass
    except (OSError, FileNotFoundError):
        pass
    return total_size


def format_size(size_bytes):
    """Format size in human readable format"""
    if size_bytes == 0:
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def find_old_directories(upload_path, days_threshold, logger):
    """Find directories older than the specified threshold"""
    old_dirs = []
    cutoff_date = datetime.now() - timedelta(days=days_threshold)
    
    if not os.path.exists(upload_path):
        logger.warning(f"Upload path does not exist: {upload_path}")
        return old_dirs
    
    try:
        for item in os.listdir(upload_path):
            item_path = os.path.join(upload_path, item)
            
            if os.path.isdir(item_path):
                try:
                    # Get creation time of directory
                    creation_time = datetime.fromtimestamp(os.path.getctime(item_path))
                    
                    if creation_time < cutoff_date:
                        size = get_directory_size(item_path)
                        age_days = (datetime.now() - creation_time).days
                        
                        old_dirs.append({
                            'path': item_path,
                            'name': item,
                            'created': creation_time,
                            'age_days': age_days,
                            'size_bytes': size,
                            'size_formatted': format_size(size)
                        })
                        
                except (OSError, FileNotFoundError) as e:
                    logger.warning(f"Error processing directory {item_path}: {e}")
                    
    except (OSError, FileNotFoundError) as e:
        logger.error(f"Error accessing upload directory: {e}")
    
    return sorted(old_dirs, key=lambda x: x['created'])


def cleanup_old_files(upload_path="static/uploads", days=10, dry_run=False, verbose=False):
    """
    Main cleanup function
    
    Args:
        upload_path (str): Path to the upload directory
        days (int): Delete files older than this many days
        dry_run (bool): If True, only show what would be deleted without doing it
        verbose (bool): Enable verbose logging
    """
    logger = setup_logging(verbose)
    
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)
    
    logger.info(f"Starting cleanup process: {upload_path}")
    logger.info(f"Deleting directories older than {days} days")
    
    if dry_run:
        logger.info("DRY RUN MODE - No files will actually be deleted")
    
    # Find old directories
    old_dirs = find_old_directories(upload_path, days, logger)
    
    if not old_dirs:
        logger.info(f"No directories found older than {days} days")
        return
    
    # Calculate totals
    total_size = sum(d['size_bytes'] for d in old_dirs)
    total_size_formatted = format_size(total_size)
    
    logger.info(f"Found {len(old_dirs)} directories to delete")
    logger.info(f"Total size to free: {total_size_formatted}")
    
    if verbose:
        logger.info("Directories to delete:")
        for d in old_dirs:
            logger.info(f"  {d['name']} - {d['age_days']} days old - {d['size_formatted']}")
    
    # Delete directories
    deleted_count = 0
    freed_space = 0
    
    for directory in old_dirs:
        try:
            if dry_run:
                logger.info(f"Would delete: {directory['name']} ({directory['size_formatted']})")
            else:
                logger.debug(f"Deleting directory: {directory['path']}")
                shutil.rmtree(directory['path'])
                deleted_count += 1
                freed_space += directory['size_bytes']
                logger.info(f"Deleted: {directory['name']} ({directory['size_formatted']})")
                
        except Exception as e:
            logger.error(f"Error deleting {directory['path']}: {e}")
    
    if not dry_run:
        freed_space_formatted = format_size(freed_space)
        logger.info(f"Cleanup completed successfully")
        logger.info(f"Deleted {deleted_count} directories, freed {freed_space_formatted}")
    else:
        logger.info(f"Dry run completed - would delete {len(old_dirs)} directories")


def main():
    """Command line interface"""
    parser = argparse.ArgumentParser(
        description="Cleanup old upload files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python scripts/cleanup_old_files.py                    # Delete files older than 10 days
    python scripts/cleanup_old_files.py --days 7           # Delete files older than 7 days  
    python scripts/cleanup_old_files.py --dry-run          # Show what would be deleted
    python scripts/cleanup_old_files.py --verbose          # Show detailed output
        """
    )
    
    parser.add_argument(
        '--days', 
        type=int, 
        default=10,
        help='Delete files older than this many days (default: 10)'
    )
    
    parser.add_argument(
        '--path',
        type=str,
        default='static/uploads',
        help='Path to upload directory (default: static/uploads)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.days <= 0:
        print("Error: --days must be a positive integer")
        return 1
    
    if not os.path.exists(args.path):
        print(f"Error: Upload path does not exist: {args.path}")
        return 1
    
    try:
        cleanup_old_files(
            upload_path=args.path,
            days=args.days,
            dry_run=args.dry_run,
            verbose=args.verbose
        )
        return 0
        
    except KeyboardInterrupt:
        print("\nCleanup interrupted by user")
        return 1
    except Exception as e:
        print(f"Error during cleanup: {e}")
        return 1


if __name__ == "__main__":
    exit(main())