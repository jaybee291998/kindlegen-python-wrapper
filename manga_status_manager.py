import pika
import subprocess
import json
import argparse
import logging
import re
import os
from typing import Dict, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RabbitMQManager:
    def __init__(self):
        self.host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.port = int(os.getenv('RABBITMQ_PORT', '5627'))
        self.queue = os.getenv('RABBITMQ_QUEUE', '56627')
        self.username = os.getenv('RABBITMQ_USERNAME')
        self.password = os.getenv('RABBITMQ_PASSWORD')
        self.virtual_host = os.getenv('RABBITMQ_VIRTUAL_HOST', '/')
        self.connection = None
        self.channel = None

    def connect(self):
        try:
           credentials = None
           if self.username and self.password:
            credentials = pika.PlainCredentials(self.username, self.password)

            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    virtual_host=self.virtual_host,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
           self.channel = self.connection.channel()
           self.channel.queue_declare(queue=self.queue, durable=True)
           logger.info('Connected to RabbitMQ')
        except Exception as e:
            logger.error(f'Failed to connect to RabbitMQ: {str(e)}')
    
    def send_message(self, message: Dict[str, Any]):
       try:
        if not self.connection or self.connection.is_closed:
             self.connect()
        
        message['timestamp'] = datetime.utcnow().isoformat()

        self.channel.basic_publish(
           exchange='',
           routing_key=self.queue,
           body=json.dumps(message),
           properties=pika.BasicProperties(
              delivery_mode=2,
              content_encoding='application/json'
           )
        )
        logger.debug(f'Sent message to RabbitMQ: {message}')

       except Exception as e:
        logger.error(f'Failed to send message to RabbitMQ: {str(e)}')
        raise
    
    def close(self):
        if self.connection and not self.connection.is_closed:
           self.connection.close()
           logger.info('Closed RabbitMQ connection')

class MangaProcessor:

   def __init__(self):
    self.rabbitmq = RabbitMQManager()
    self.rabbitmq.connect()
    self.current_volume = None
    self.manga_name = None
    self.job_number = None
    self.base_path = os.getenv('MANGA_BASE_PATH', '/home/rootjaybee/manga_to_kindle/kcc')

    def send_status(self, step: str, status: str, details: Dict[str, Any] = None):
        if details is None:
            details = {}
        
        message = {
           'step': step,
           'status': status,
           'manga_name': self.manga_name,
           'volume': self.current_volume,
           'job_number': self.job_number,
           **details
        }

        self.rabbitmq.send_message(message)
    
    def parse_log_line(self, line: str):
        line = line.strip()

        # Track Spring Boot application startup
        if "Starting MangadexDownloadServiceApplication" in line:
            self.send_status(
                step='download',
                status='initializing',
                details={'service': 'MangadexDownloadService'}
            )
        
        # Track download completion
        elif re.match(r"downloaded volume \d+ of .+", line):
            vol_num = line.split()[2]
            self.send_status(
                step="download",
                status="completed",
                details={"volume_number": vol_num}
            )
        
        # Track CBZ conversion start
        elif "converting to cbz" in line:
            self.send_status(
                step="compression",
                status="started",
                details={"format": "CBZ"}
            )
        
        # Track 7z compression progress
        elif "7-Zip" in line and "Creating archive:" in line:
            archive_path = line.split(":")[1].strip()
            self.send_status(
                step="compression",
                status="in_progress",
                details={"archive_path": archive_path}
            )
        
        # Track compression completion
        elif "Everything is Ok" in line:
            self.send_status(
                step="compression",
                status="completed",
                details={"result": "success"}
            )
        
        # Track output directory creation
        elif "Created output directory:" in line:
            dir_path = line.split(":")[1].strip()
            self.send_status(
                step="output_preparation",
                status="completed",
                details={"directory": dir_path}
            )
        
        # Track KCC conversion start
        elif "comic2ebook" in line and "Working on" in line:
            source_file = line.split("...")[0].split("on ")[1]
            self.send_status(
                step="conversion",
                status="started",
                details={
                    "tool": "Kindle Comic Converter",
                    "source": source_file
                }
            )
        
        # Track conversion steps
        elif any(step in line for step in [
            "Preparing source images",
            "Checking images",
            "Processing images",
            "Creating EPUB file",
            "Creating MOBI files"
        ]):
            self.send_status(
                step="conversion",
                status="in_progress",
                details={"current_operation": line.split("...")[0].strip()}
            )
        
        # Track conversion completion
        elif "conversion done" in line:
            self.send_status(
                step="conversion",
                status="completed",
                details={"result": "success"}
            )
        
        # Track upload
        elif "successfully uploaded" in line:
            file_path = line.split()[-1]
            self.send_status(
                step="upload",
                status="completed",
                details={
                    "file_path": file_path,
                    "result": "success"
                }
            )
        
        # Log other lines for debugging
        else:
            logger.debug(f"Processing line: {line}")
    
    def process_manga(self, manga_id: str, manga_name: str, volume_number: str, separate_chapter_folder: str):
        self.current_volume = volume_number
        self.manga_name = manga_name

        try:
            self.send_status(
               step='start',
               status='initiated',
               details={
                  'manga_id': manga_id,
                  'manga_name': manga_name,
                  'volume_number': volume_number,
                  'separate_folder': separate_chapter_folder
               }
            )

            script_path = os.path.join(self.base_path, 'prototype6.sh')

            cmd = [
               script_path,
               '-i', manga_id,
               '-m', manga_name,
               '-v', volume_number,
               '-s', separate_chapter_folder
            ]
        
            process = subprocess.Popen(
               cmd,
               stdout=subprocess.PIPE,
               stderr=subprocess.PIPE,
               universal_newlines=True,
               bufsize=1
            )

            while True:
               output = process.stdout.readline()
               if output == '' and process.poll() is not None:
                  break
               if output:
                  logger.info(output.strip())
                  self.parse_log_line(output)

            return_code = process.poll()
            if return_code != 0:
               error_output = process.stderr.read()
               self.send_status(
                  step = 'error',
                  status = 'failed',
                  details = {
                     'error': error_output,
                     'return_code': return_code
                  }
               )
               logger.error(f'Process failed with error: {error_output}')
               raise subprocess.CalledProcessError(return_code, cmd)
            
            self.send_status(
               step='complete',
               status='success',
               details={
                  'manga_id': manga_id,
                  'manga_name': manga_name,
                  'volume_number': volume_number
               }
            )

        except Exception as e:
           self.send_status(
              step='error',
              status='failed',
              details={'error': str(e)}

           )
           logger.error(f'Exception occured: {str(e)}')
        finally:
           self.rabbitmq.close()

def main():
   parser = argparse.ArgumentParser(description="Manga to kindle Processor with RabbitMQ status updates")
   parser.add_argument('-i', '--manga_id', required=True, help='Manga ID')
   parser.add_argument('-m', '--manga_name', required=True, help='Manga name')
   parser.add_argument('-v', '--volume', required=True, help='Volume number')
   parser.add_argument('-s', '--separate_chapter_folder', required=True, help="Separate chapter folder flag (Y/N)")
   parser.add_argument('-p', '--job_number', required=True, help="Job identifier")

   args = parser.parse_args()

   processor = MangaProcessor()
   processor.process_manga(
      args.manga_id,
      args.manga_name,
      args.volume_number,
      args.job_number,
      args.separate_chapter_folder
   )

if __name__ == '__main__':
   main()