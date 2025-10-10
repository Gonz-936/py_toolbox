# src/py_toolbox/aws/textract_processor.py
import logging
import boto3
import time
import json

class TextractProcessor:
    """
    Una clase para orquestar el análisis de documentos PDF usando AWS Textract.
    """
    def __init__(self, region_name: str):
        self.textract_client = boto3.client("textract", region_name=region_name)
        self.s3_client = boto3.client("s3", region_name=region_name)
        logging.info(f"TextractProcessor inicializado en la región {region_name}.")

    def _upload_to_s3(self, local_pdf_path: str, bucket_name: str, s3_key: str) -> bool:
        """Sube el archivo PDF a S3."""
        try:
            self.s3_client.upload_file(local_pdf_path, bucket_name, s3_key)
            logging.info(f"Archivo subido exitosamente a s3://{bucket_name}/{s3_key}")
            return True
        except Exception as e:
            logging.error(f"Error al subir el archivo a S3: {e}")
            return False

    def start_document_analysis(self, bucket_name: str, s3_key: str) -> str | None:
        """Inicia un trabajo de análisis de Textract y devuelve el JobId."""
        try:
            response = self.textract_client.start_document_analysis(
                DocumentLocation={'S3Object': {'Bucket': bucket_name, 'Name': s3_key}},
                FeatureTypes=['TABLES', 'FORMS'] # Le pedimos que detecte tablas y formularios
            )
            job_id = response['JobId']
            logging.info(f"Trabajo de análisis de Textract iniciado con JobId: {job_id}")
            return job_id
        except Exception as e:
            logging.error(f"Error al iniciar el análisis de Textract: {e}")
            return None

    def wait_for_job_completion(self, job_id: str):
        """Espera a que un trabajo de Textract finalice."""
        logging.info("Esperando a que el trabajo de Textract finalice. Esto puede tardar varios minutos...")
        while True:
            response = self.textract_client.get_document_analysis(JobId=job_id)
            status = response['JobStatus']
            logging.info(f"Estado del trabajo: {status}")
            if status in ['SUCCEEDED', 'FAILED']:
                break
            time.sleep(15) # Pausa de 15 segundos entre verificaciones
        
        if status != 'SUCCEEDED':
            raise Exception(f"El trabajo de Textract falló. Estado final: {status}")
        logging.info("El trabajo de Textract finalizó con éxito.")

    def get_full_results(self, job_id: str) -> list[dict]:
        """Obtiene todos los resultados paginados de un trabajo de Textract."""
        logging.info("Obteniendo resultados completos de Textract...")
        all_blocks = []
        next_token = None
        
        while True:
            params = {'JobId': job_id}
            if next_token:
                params['NextToken'] = next_token
            
            response = self.textract_client.get_document_analysis(**params)
            all_blocks.extend(response.get('Blocks', []))
            
            next_token = response.get('NextToken')
            if not next_token:
                break
        
        logging.info(f"Se obtuvieron {len(all_blocks)} bloques de Textract.")
        return all_blocks