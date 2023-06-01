import os
import subprocess
import hashlib
import mysql.connector
from loguru import logger
import yaml


def calculate_file_hash(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def check_video_integrity(path, db_connection):
    cursor = db_connection.cursor()

    for root, dirs, files in os.walk(path):
        for file in files:
            if file.lower().endswith(('.mp4', '.avi', '.mkv', '.mov')):  # Add more video formats as needed
                file_path = os.path.join(root, file)
                file_hash = calculate_file_hash(file_path)

                cursor.execute("SELECT COUNT(*) FROM checked_files WHERE file_hash = %s", (file_hash,))
                result = cursor.fetchone()
                if result[0] > 0:
                    logger.info(f'Skipping {file_path} (already checked)')
                    continue

                try:
                    subprocess.run(['ffmpeg', '-v', 'error', '-i', file_path, '-f', 'null', '-'],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True)
                    logger.info(f'{file_path}: OK')

                    # Store the file in the database
                    cursor.execute("INSERT INTO checked_files (file_hash, file_name) VALUES (%s, %s)",
                                   (file_hash, file))
                    db_connection.commit()
                except subprocess.CalledProcessError as e:
                    logger.error(f'{file_path}: Error - {e.stderr.decode().strip()}')

    cursor.close()
    db_connection.close()

if __name__ == "__main__":
    config = yaml.safe_load(open("config.yml"))
    path = input("Please enter the path to the folder to check: ")

    db_host = config['db']['host']
    db_user = config['db']['user']
    db_password = config['db']['password']
    db_name = config['db']['database']

    db_connection = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )

    # Configure Loguru logger settings
    logger.add("video_integrity.log", rotation="10 MB", compression="zip", level="INFO")

    # Call the function to check integrity
    check_video_integrity(path, db_connection)
