# src/invoice_pipeline/athena_connector.py
import logging
import time
import boto3
from botocore.exceptions import ClientError

class AthenaConnector:
    def __init__(self, database_name: str, bucket_name: str, region_name: str):
        """
        Inicializa el conector de Athena.
        :param database_name: El nombre de la base de datos en Athena.
        :param bucket_name: El bucket de S3 donde Athena guardará los resultados de las consultas.
        :param region_name: La región de AWS.
        """
        self.athena_client = boto3.client('athena', region_name=region_name)
        self.database = database_name
        self.s3_output_location = f's3://{bucket_name}/athena_query_results/'
        logging.info("AthenaConnector inicializado.")

    def get_processed_file_ids(self, table_name: str) -> set[str]:
        """
        Consulta la tabla de facturas en Athena para obtener todos los file_id únicos
        que ya han sido procesados. Si ocurre un error de permisos, el job fallará.
        """
        query = f'SELECT DISTINCT file_id FROM "{table_name}" WHERE file_id IS NOT NULL;'
        logging.info(f"Ejecutando consulta en Athena para obtener IDs procesados: {query}")

        try:
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.s3_output_location}
            )
            query_execution_id = response['QueryExecutionId']
            logging.info(f"Consulta iniciada con ID: {query_execution_id}")

            while True:
                stats = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = stats['QueryExecution']['Status']['State']
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(1)

            if status != 'SUCCEEDED':
                error_info = stats['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido.')
                logging.error(f"La consulta de Athena falló con estado: {status}. Razón: {error_info}")
                # Si la consulta falla en Athena, devolvemos un set vacío para no detener el pipeline
                # por un error transitorio.
                return set()

            results_paginator = self.athena_client.get_paginator('get_query_results')
            results_iter = results_paginator.paginate(QueryExecutionId=query_execution_id)
            
            processed_ids = set()
            for results in results_iter:
                for row in results['ResultSet']['Rows'][1:]:
                    if 'Data' in row and row['Data'][0].get('VarCharValue'):
                        processed_ids.add(row['Data'][0]['VarCharValue'])
            
            logging.info(f"Se encontraron {len(processed_ids)} file_ids ya procesados en Athena.")
            return processed_ids

        except ClientError as e:
            # --- LÓGICA DE FALLO EXPLÍCITO ---
            # Manejamos el caso específico de que la tabla no exista como un escenario normal.
            if e.response['Error']['Code'] == 'InvalidRequestException' and 'does not exist' in e.response['Error']['Message']:
                logging.warning(f"La tabla '{table_name}' no existe aún. Se asume que no hay archivos procesados.")
                return set()
            else:
                # Para cualquier otro error de cliente (incluido AccessDenied), relanzamos la excepción.
                logging.error(f"Error de cliente no manejado al consultar Athena por file_ids: {e}")
                raise e

    def get_processed_invoice_numbers(self, table_name: str) -> set[int]:
        """
        Consulta la tabla de facturas en Athena para obtener todos los invoice_number únicos
        que ya han sido procesados. Si ocurre un error de permisos, el job fallará.
        """
        query = f'SELECT DISTINCT invoice_number FROM "{table_name}" WHERE invoice_number IS NOT NULL;'
        logging.info(f"Ejecutando consulta en Athena para obtener N° de facturas procesadas: {query}")

        try:
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.s3_output_location}
            )
            query_execution_id = response['QueryExecutionId']
            logging.info(f"Consulta de N° de Facturas iniciada con ID: {query_execution_id}")

            while True:
                stats = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = stats['QueryExecution']['Status']['State']
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(1)

            if status != 'SUCCEEDED':
                error_info = stats['QueryExecution']['Status'].get('StateChangeReason', 'Error desconocido.')
                logging.error(f"La consulta de Athena para N° de Facturas falló: {status}. Razón: {error_info}")
                return set()

            results_paginator = self.athena_client.get_paginator('get_query_results')
            results_iter = results_paginator.paginate(QueryExecutionId=query_execution_id)
            
            processed_numbers = set()
            for results in results_iter:
                for row in results['ResultSet']['Rows'][1:]:
                    if 'Data' in row and row['Data'][0].get('VarCharValue'):
                        processed_numbers.add(int(row['Data'][0]['VarCharValue']))
            
            logging.info(f"Se encontraron {len(processed_numbers)} invoice_numbers ya procesados en Athena.")
            return processed_numbers

        except ClientError as e:
            # --- LÓGICA DE FALLO EXPLÍCITO ---
            if e.response['Error']['Code'] == 'InvalidRequestException' and 'does not exist' in e.response['Error']['Message']:
                logging.warning(f"La tabla '{table_name}' no existe aún. Se asume que no hay facturas procesadas.")
                return set()
            else:
                logging.error(f"Error de cliente no manejado al consultar Athena por invoice_numbers: {e}")
                raise e