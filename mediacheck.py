import os
import subprocess
import hashlib
import mysql.connector
from loguru import logger
import yaml
import time

def calculate_short_file_hash(file_path):
    chunk_size = 4096
    max_size = 2 * 1024 * 1024 * 1024  # 2 GB
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as f:
        bytes_read = 0
        while bytes_read < max_size:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            sha256_hash.update(chunk)
            bytes_read += len(chunk)
    return sha256_hash.hexdigest()

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
                file_scanned = False
                while not file_scanned:
                    try:
                        start_time = time.time()
                        file_path = os.path.join(root, file)
                        file_hash = calculate_short_file_hash(file_path)


                        cursor.execute("SELECT COUNT(*) FROM checked_files WHERE short_file_hash = %s AND result = 'OK'", (file_hash,))
                        result = cursor.fetchone()
                        if result[0] > 0:
                            logger.info(f'Skipping {file_path} (already checked)')
                            file_scanned = True
                            continue
                            
                        cursor.execute("SELECT COUNT(*) FROM checked_files WHERE file_name = %s AND result = 'OK'", (file,))
                        result = cursor.fetchone()
                        if result[0] > 0:
                            logger.info(f'Skipping {file_path} (already checked)')
                            logger.info(f'Recalculate Hash...')
                            cursor.execute("SELECT file_hash FROM checked_files WHERE file_name = %s AND result = 'OK'", (file,))
                            old_results = cursor.fetchall()
                            for old_result in old_results:
                                old_file_hash = old_result[0]
                                recalc_old_hash = calculate_file_hash(file_path)
                                if recalc_old_hash != old_file_hash:
                                    logger.error('Path not matching old hash! Old Hash: %s Recalced-Hash: %s' % (old_file_hash, recalc_old_hash))
                                    continue

                                # Berechne den neuen Hash basierend auf dem alten Dateinamen
                                new_file_hash = file_hash

                                # Aktualisiere den neuen Hash in der Datenbank
                                cursor.execute("UPDATE checked_files SET short_file_hash = %s WHERE file_hash = %s", (new_file_hash, old_file_hash,))
                                db_connection.commit()
                                logger.info('Updated File-Hash')
                            file_scanned = True
                            continue
                        
                        ffmpeg_result = ""
                        try:
                            subprocess.run(['ffmpeg', '-v', 'error', '-i', file_path, '-f', 'null', '-'],
                                        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True)
                            ffmpeg_result = "OK"
                            logger.info(f'{file_path}: OK')
                        except subprocess.CalledProcessError as e:
                            ffmpeg_result = "ERROR"
                            logger.error(f'{file_path}: Error - {e.stderr.decode().strip()}')

                        end_time = time.time()

                        logger.debug("Took %d" % ((end_time - start_time)))

                        cursor.execute("SELECT COUNT(*) FROM checked_files WHERE file_hash = %s AND result = 'ERROR'", (file_hash,))
                        result = cursor.fetchone()
                        if result[0] > 0:
                            logger.info(f'{file_path}: Rechecked')
                            cursor.execute("UPDATE checked_files SET last_check = CURRENT_TIMESTAMP(), result = %s, duration = %s WHERE file_hash = %s",
                                            (ffmpeg_result, str(end_time - start_time), file_hash,))
                            db_connection.commit()
                        else:
                            cursor.execute("INSERT INTO checked_files (file_hash, file_name, result, duration) VALUES (%s, %s, %s, %s)",
                                            (file_hash, file, ffmpeg_result, str(end_time - start_time)))
                            db_connection.commit()
                        file_scanned = True
                    except mysql.connector.errors.OperationalError as e:
                        if e.errno == 2013:  # Lost connection to MySQL server
                            logger.warning("Connection lost. Reconnecting...")
                            db_connection.reconnect()
                            cursor = db_connection.cursor()
                        logger.error(e)
                        logger.warning("Recheck file!")
                        file_scanned = False
                    except Exception as e:
                        logger.error(e)
                        logger.warning("Recheck file. Unknown Error")
                        file_scanned = False
                    

    cursor.close()
    db_connection.close()

if __name__ == "__main__":
    config = yaml.safe_load(open("config.yml"))
    path = config['check']['path']

    db_host = config['db']['host']
    db_user = config['db']['user']
    db_password = config['db']['password']
    db_name = config['db']['database']

    while True:

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