# src/py_toolbox/aws/secrets_manager.py
import json
import logging
import boto3
from typing import Union

class SecretsManager:
    def __init__(self, region_name: str):
        self.client = boto3.client(service_name='secretsmanager', region_name=region_name)
        logging.info(f"Cliente de AWS Secrets Manager inicializado para la región {region_name}.")

    def get_secret(self, secret_name: str) -> Union[dict, None]:
        try:
            get_secret_value_response = self.client.get_secret_value(SecretId=secret_name)
            secret = get_secret_value_response['SecretString']
            logging.info(f"Secreto '{secret_name}' obtenido exitosamente de Secrets Manager.")
            return json.loads(secret)
        except Exception as e:
            logging.error(f"Error al obtener el secreto '{secret_name}' de Secrets Manager: {e}", exc_info=True)
            raise e