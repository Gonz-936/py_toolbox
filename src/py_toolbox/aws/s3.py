# src/py_toolbox/aws/s3.py
import logging
import boto3
from botocore.exceptions import ClientError
import io

# Asegúrate de tener pandas y pyarrow instalados si usas el método del dataframe.
# pip install pandas pyarrow
try:
    import pandas as pd
except ImportError:
    pd = None

class S3:
    """
    Una clase de utilidad para interactuar con el servicio AWS S3.
    """
    def __init__(self, region_name: str):
        """
        Inicializa el cliente de S3.
        :param region_name: La región de AWS donde operará el cliente.
        """
        self.s3_client = boto3.client('s3', region_name=region_name)
        logging.info(f"Cliente de S3 inicializado en la región {region_name}.")

    def upload_file(self, local_path: str, bucket: str, key: str, extra_args: dict = None) -> bool:
        """
        Sube un archivo desde una ruta local a S3.

        :param local_path: Ruta del archivo local a subir.
        :param bucket: Nombre del bucket de S3 de destino.
        :param key: La ruta completa (prefijo + nombre) del objeto en S3.
        :param extra_args: Argumentos adicionales para pasar a la función de subida (ej. {'Metadata': {...}}).
        :return: True si la subida fue exitosa, False en caso contrario.
        """
        try:
            self.s3_client.upload_file(local_path, bucket, key, ExtraArgs=extra_args or {})
            logging.info(f"Archivo '{local_path}' subido exitosamente a s3://{bucket}/{key}")
            return True
        except ClientError as e:
            logging.error(f"Error de cliente al subir el archivo a S3: {e}", exc_info=True)
            return False
        except FileNotFoundError:
            logging.error(f"El archivo local no fue encontrado en la ruta: {local_path}")
            return False

    def upload_json_to_s3(self, json_data: str, bucket_name: str, s3_key: str) -> bool:
        """
        Sube un string en formato JSON a una ruta específica en un bucket de S3.
        
        :param json_data: El contenido del archivo JSON como un string.
        :param bucket_name: El nombre del bucket de S3.
        :param s3_key: La ruta completa del archivo en S3.
        :return: True si la subida fue exitosa, False en caso contrario.
        """
        try:
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            logging.info(f"Archivo JSON subido exitosamente a s3://{bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logging.error(f"Error al subir el archivo JSON a S3: {e}", exc_info=True)
            return False

    def upload_dataframe_as_parquet(self, df, bucket: str, key: str, index: bool = False) -> bool:
        """
        Convierte un DataFrame de Pandas a formato Parquet en memoria y lo sube a S3.

        :param df: El DataFrame de Pandas a subir.
        :param bucket: El nombre del bucket de S3 de destino.
        :param key: La ruta completa (prefijo + nombre) del objeto en S3.
        :param index: Booleano, si se debe incluir el índice del DataFrame en el archivo.
        :return: True si la subida fue exitosa, False en caso contrario.
        """
        if pd is None:
            logging.critical("La librería 'pandas' no está instalada. No se puede subir el DataFrame.")
            return False
        
        try:
            out_buffer = io.BytesIO()
            df.to_parquet(out_buffer, index=index, engine='pyarrow')
            out_buffer.seek(0)
            
            self.s3_client.put_object(Bucket=bucket, Key=key, Body=out_buffer.read())
            
            logging.info(f"DataFrame subido exitosamente como Parquet a s3://{bucket}/{key}")
            return True
            
        except ImportError:
            logging.critical("La librería 'pyarrow' es necesaria para escribir en formato Parquet. Por favor, instálala.")
            return False
        except Exception as e:
            logging.error(f"Fallo al subir el DataFrame como Parquet a S3: {e}", exc_info=True)
            return False

    def get_object_metadata(self, bucket: str, key: str, metadata_key: str, default=None):
        """
        Obtiene un valor de metadato específico de un objeto S3.

        :param bucket: Nombre del bucket.
        :param key: Clave del objeto.
        :param metadata_key: La clave del metadato a obtener.
        :param default: Valor a devolver si el metadato no se encuentra.
        :return: El valor del metadato o el valor por defecto.
        """
        try:
            head_object = self.s3_client.head_object(Bucket=bucket, Key=key)
            return head_object.get('Metadata', {}).get(metadata_key, default)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logging.warning(f"El objeto s3://{bucket}/{key} no fue encontrado para obtener metadata.")
            else:
                logging.error(f"Error al obtener metadata para s3://{bucket}/{key}: {e}", exc_info=True)
            return default

    def move_file(self, bucket: str, source_key: str, dest_prefix: str):
        """Mueve un archivo a una nueva ubicación (prefijo) dentro del mismo bucket."""
        try:
            file_name = source_key.split('/')[-1]
            dest_key = f"{dest_prefix.strip('/')}/{file_name}"
            copy_source = {'Bucket': bucket, 'Key': source_key}
            
            logging.info(f"Moviendo de s3://{bucket}/{source_key} a s3://{bucket}/{dest_key}")
            self.s3_client.copy_object(CopySource=copy_source, Bucket=bucket, Key=dest_key)
            self.s3_client.delete_object(Bucket=bucket, Key=source_key)
            logging.info("Movimiento completado exitosamente.")
        except Exception as e:
            logging.error(f"Fallo al mover el archivo {source_key}: {e}", exc_info=True)
            raise e

    def delete_file(self, bucket: str, key: str):
        """Elimina un archivo de S3."""
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=key)
            logging.info(f"Archivo s3://{bucket}/{key} eliminado exitosamente.")
        except Exception as e:
            logging.error(f"Fallo al eliminar el archivo {key}: {e}", exc_info=True)
            raise e