import os
import subprocess
from loguru import logger

def check_video_integrity(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.lower().endswith(('.mp4', '.avi', '.mkv', '.mov')):  # Füge weitere Videoformate nach Bedarf hinzu
                file_path = os.path.join(root, file)
                try:
                    # Verwende ffmpeg, um die Integrität der Videodatei zu überprüfen
                    subprocess.run(['ffmpeg', '-v', 'error', '-i', file_path, '-f', 'null', '-'],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True)
                    logger.info(f'{file_path}: OK')
                except subprocess.CalledProcessError as e:
                    logger.error(f'{file_path}: Error - {e.stderr.decode().strip()}')

if __name__ == "__main__":
    # Abfrage des Pfads beim Starten
    path = input("Bitte gib den Pfad zum überprüfenden Ordner ein: ")
    
    # Konfiguration der Loguru-Logger-Einstellungen
    logger.add("video_integrity.log", rotation="10 MB", compression="zip")
    
    # Aufruf der Funktion zum Überprüfen der Integrität
    check_video_integrity(path)
