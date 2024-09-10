"""
Folder Synchronization Script

This script synchronizes two folders: a source folder and a replica folder.
It maintains a full, identical copy of the source folder at the replica folder location.

Usage:
    python sync_script.py <source_folder> <replica_folder> <log_file_path> <sync_interval_in_seconds>

Author: Vinicius Hermann Dutra Liczkoski
Date: 10/09/2024
"""

import os
import shutil
import hashlib
import time
import argparse
import logging
import sys
from typing import Dict, Any
from logging.handlers import RotatingFileHandler
from tqdm import tqdm

CHUNK_SIZE = 4096


def setup_logging(log_file: str) -> None:
    """
    Set up logging to both file and console.
    Logs will be saved to the log file with rotation enabled.

    Args:
        log_file (str): Path to the log file.
    """
    log_dir = os.path.dirname(log_file)
    os.makedirs(log_dir, exist_ok=True)

    file_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024, backupCount=5)
    console_handler = logging.StreamHandler()

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[file_handler, console_handler])


def calculate_md5(file_path: str) -> str:
    """
    Calculate MD5 hash of a file to detect changes.

    Args:
        file_path (str): Path to the file.

    Returns:
        str: MD5 hash of the file.
    """
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_file_info(file_path: str) -> Dict[str, Any]:
    """
    Get file size and modification time.

    Args:
        file_path (str): Path to the file.

    Returns:
        Dict[str, Any]: Dictionary with file size and modification time.
    """
    stat = os.stat(file_path)
    return {
        'size': stat.st_size,
        'mtime': stat.st_mtime
    }


def create_directory_if_missing(directory: str) -> None:
    """
    Ensure the directory exists, creating it if necessary.

    Args:
        directory (str): Path to the directory.
    """
    if not os.path.exists(directory):
        logging.info(f"Creating missing directory: {directory}")
        os.makedirs(directory)


def remove_outdated_items(source_items: set, replica: str, replica_items: set, stats: Dict[str, int]) -> None:
    """
    Remove files and directories from the replica that don't exist in the source.

    Args:
        source_items (set): Set of items in the source folder.
        replica (str): Path to the replica folder.
        replica_items (set): Set of items in the replica folder.
        stats (Dict[str, int]): Dictionary to store statistics (deleted, errors).
    """
    for item in tqdm(replica_items - source_items, desc="Removing outdated items", disable=not sys.stdout.isatty()):
        item_path = os.path.join(replica, item)
        try:
            if os.path.isfile(item_path):
                os.remove(item_path)
                logging.info(f"Removed file: {item_path}")
                stats['deleted'] += 1
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
                logging.info(f"Removed directory: {item_path}")
                stats['deleted'] += 1
        except OSError as e:
            logging.error(f"Error removing {item_path}: {e}")
            stats['errors'] += 1


def sync_item(source_path: str, replica_path: str, stats: Dict[str, int]) -> None:
    """
    Sync a single file or directory from source to replica.

    Args:
        source_path (str): Path to the item in the source folder.
        replica_path (str): Path to the item in the replica folder.
        stats (Dict[str, int]): Dictionary to store statistics (copied, updated, errors).
    """
    try:
        if os.path.isfile(source_path):
            if not os.path.exists(replica_path) or calculate_md5(source_path) != calculate_md5(replica_path):
                shutil.copy2(source_path, replica_path)
                if os.path.exists(replica_path):
                    logging.info(f"Updated file: {source_path} -> {replica_path}")
                    stats['updated'] += 1
                else:
                    logging.info(f"Copied file: {source_path} -> {replica_path}")
                    stats['copied'] += 1
        elif os.path.isdir(source_path):
            create_directory_if_missing(replica_path)
            sub_stats = sync_folders(source_path, replica_path)  # Recursively sync subfolders
            for key in stats:
                stats[key] += sub_stats[key]
    except OSError as e:
        logging.error(f"Error processing {source_path}: {e}")
        stats['errors'] += 1


def sync_folders(source: str, replica: str) -> Dict[str, int]:
    """
    Synchronize the source folder with the replica folder.

    Args:
        source (str): Path to the source folder.
        replica (str): Path to the replica folder.

    Returns:
        Dict[str, int]: Statistics on copied, updated, deleted files, and errors.
    """
    stats = {'copied': 0, 'updated': 0, 'deleted': 0, 'errors': 0}

    # Ensure both source and replica directories exist
    create_directory_if_missing(source)
    create_directory_if_missing(replica)

    try:
        source_items = set(os.listdir(source))
        replica_items = set(os.listdir(replica))
    except OSError as e:
        logging.error(f"Error accessing directories: {e}")
        stats['errors'] += 1
        return stats

    # Remove outdated files and directories
    remove_outdated_items(source_items, replica, replica_items, stats)

    # Sync new or updated files
    for item in tqdm(source_items, desc="Syncing items", disable=not sys.stdout.isatty()):
        source_path = os.path.join(source, item)
        replica_path = os.path.join(replica, item)
        sync_item(source_path, replica_path, stats)

    return stats


def main() -> None:
    """
    Main function to execute the folder synchronization.
    """
    parser = argparse.ArgumentParser(description="Folder Synchronization Script")

    # Command-line arguments
    parser.add_argument("source", help="Path to the source folder")
    parser.add_argument("replica", help="Path to the replica folder")
    parser.add_argument("log_file", help="Path to the log file")
    parser.add_argument("interval", type=int, help="Synchronization interval in seconds")

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_file)

    logging.info("Starting folder synchronization process")

    try:
        while True:
            logging.info("Starting synchronization...")
            stats = sync_folders(args.source, args.replica)
            logging.info(f"Synchronization completed. "
                         f"Copied: {stats['copied']}, Updated: {stats['updated']}, "
                         f"Deleted: {stats['deleted']}, Errors: {stats['errors']}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logging.info("Synchronization process stopped by user.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
