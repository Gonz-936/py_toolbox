# src/invoice_pipeline/s3_uploader.py
import logging
import boto3
from botocore.exceptions import ClientError

class S3Uploader:
    def __init__(self, region_name: str):
        """
        Inicializa el uploader de S3.
        :param region_name: La región de AWS donde está el bucket.
        """
        self.s3_client = boto3.client('s3', region_name=region_name)
        logging.info("S3Uploader inicializado.")

    def upload_json_to_s3(self, json_data: str, bucket_name: str, s3_key: str) -> bool:
        """
        Sube un string en formato JSON a una ruta específica en un bucket de S3.
        
        :param json_data: El contenido del archivo JSON como un string.
        :param bucket_name: El nombre del bucket de S3.
        :param s3_key: La ruta completa del archivo en S3 (ej. 'invoices/2025/02/factura_123.json').
        :return: True si la subida fue exitosa, False en caso contrario.
        """
        try:
            # S3 espera los datos en formato de bytes, así que codificamos el string a utf-8
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            logging.info(f"Archivo JSON subido exitosamente a s3://{bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logging.error(f"Error al subir el archivo a S3: {e}")
            return False
        except Exception as e:
            logging.error(f"Un error inesperado ocurrió durante la subida a S3: {e}")
            return False
    # Dentro de la clase S3Uploader en s3_uploader.py

    def get_object_metadata(self, bucket, key, metadata_key, default=None):
        """Obtiene un valor de metadato específico de un objeto S3."""
        try:
            head_object = self.s3_client.head_object(Bucket=bucket, Key=key)
            return head_object.get('Metadata', {}).get(metadata_key, default)
        except self.s3_client.exceptions.NoSuchKey:
            print(f"Advertencia: El objeto s3://{bucket}/{key} no fue encontrado para obtener metadata.")
            return default
        except Exception as e:
            print(f"Error al obtener metadata para s3://{bucket}/{key}: {e}")
            return default

    def move_file(self, bucket, source_key, dest_prefix):
        """Mueve un archivo a una nueva ubicación (prefijo) dentro del mismo bucket."""
        try:
            file_name = source_key.split('/')[-1]
            dest_key = f"{dest_prefix.strip('/')}/{file_name}"
            copy_source = {'Bucket': bucket, 'Key': source_key}
            
            print(f"INFO: Moviendo de s3://{bucket}/{source_key} a s3://{bucket}/{dest_key}")
            self.s3_client.copy_object(CopySource=copy_source, Bucket=bucket, Key=dest_key)
            self.s3_client.delete_object(Bucket=bucket, Key=source_key)
            print("INFO: Movimiento completado.")
        except Exception as e:
            print(f"ERROR: Fallo al mover archivo {source_key}: {e}")
            raise e

    def delete_file(self, bucket, key):
        """Elimina un archivo de S3."""
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=key)
            print(f"INFO: Archivo s3://{bucket}/{key} eliminado.")
        except Exception as e:
            print(f"ERROR: Fallo al eliminar archivo {key}: {e}")
            raise e
    def upload_dataframe_as_parquet(self, df, bucket, key, index=False):
        """
        Convierte un DataFrame de Pandas a formato Parquet en memoria y lo sube a S3.

        :param df: El DataFrame de Pandas a subir.
        :param bucket: El nombre del bucket de S3 de destino.
        :param key: La ruta completa (prefijo + nombre de archivo) del objeto en S3.
        :param index: Booleano, si se debe incluir el índice del DataFrame en el archivo.
        """
        try:
            # Crear un buffer de bytes en memoria
            out_buffer = io.BytesIO()
            
            # Escribir el DataFrame como Parquet en el buffer
            # Se requiere la librería 'pyarrow'
            df.to_parquet(out_buffer, index=index, engine='pyarrow')
            
            # Mover el cursor del buffer al principio
            out_buffer.seek(0)
            
            # Subir el contenido del buffer a S3 como si fuera un archivo
            self.s3_client.put_object(Bucket=bucket, Key=key, Body=out_buffer.read())
            
            print(f"INFO: DataFrame subido exitosamente a s3://{bucket}/{key}")
            
        except ImportError:
            print("ERROR CRÍTICO: La librería 'pyarrow' es necesaria para escribir en formato Parquet. Por favor, instálala.")
            raise
        except Exception as e:
            print(f"ERROR: Fallo al subir el DataFrame como Parquet a s3://{bucket}/{key}. Error: {e}")
            raise