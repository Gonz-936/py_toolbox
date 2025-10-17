# src/py_toolbox/google/drive.py
import os
import io
import logging
from typing import Union
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

class Drive:
    """
    Una clase de utilidad para interactuar con la API de Google Drive.
    """
    def __init__(self, credentials_dict: dict):
        """
        Inicializa el cliente de Google Drive usando un diccionario de credenciales.
        
        :param credentials_dict: Un diccionario que contiene las credenciales de la cuenta de servicio de Google.
        """
        self.service = self._create_drive_service(credentials_dict)
        logging.info("Servicio de Google Drive inicializado correctamente.")

    def _create_drive_service(self, credentials_dict: dict):
        """Método privado para construir el objeto de servicio de la API de Drive."""
        try:
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            return build('drive', 'v3', credentials=credentials)
        except Exception as e:
            logging.error(f"No se pudo crear el servicio de Google Drive: {e}", exc_info=True)
            raise e

    def get_folder_id_by_name(self, folder_name: str, parent_folder_id: str) -> Union[str, None]:
        """
        Busca el ID de una carpeta por su nombre exacto dentro de una carpeta padre.

        :param folder_name: El nombre de la carpeta a buscar.
        :param parent_folder_id: El ID de la carpeta donde se debe buscar.
        :return: El ID de la carpeta si se encuentra, o None si no existe.
        """
        query = (
            f"mimeType='application/vnd.google-apps.folder' "
            f"and name='{folder_name}' "
            f"and '{parent_folder_id}' in parents "
            f"and trashed=false"
        )
        try:
            response = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            folders = response.get('files', [])
            if folders:
                folder_id = folders[0].get('id')
                logging.info(f"Se encontró la carpeta '{folder_name}' con ID: {folder_id} dentro de la carpeta padre {parent_folder_id}.")
                return folder_id
            else:
                logging.warning(f"No se encontró la carpeta '{folder_name}' dentro de la carpeta padre {parent_folder_id}.")
                return None
        except HttpError as error:
            logging.error(f"Ocurrió un error al buscar la carpeta '{folder_name}': {error}", exc_info=True)
            return None

    def list_files_in_folder(self, folder_id: str) -> list:
        """
        Lista todos los archivos y carpetas directamente dentro de una carpeta específica.
        """
        if not folder_id:
            return []
        all_items = []
        try:
            page_token = None
            while True:
                response = self.service.files().list(
                    q=f"'{folder_id}' in parents and trashed=false",
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType)',
                    pageToken=page_token
                ).execute()
                all_items.extend(response.get('files', []))
                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break
        except HttpError as error:
            logging.error(f"Ocurrió un error al listar la carpeta {folder_id}: {error}", exc_info=True)
            raise error
        return all_items

    def download_file(self, file_id: str, local_path: str) -> Union[str, None]:
        """Descarga un archivo de Google Drive por su ID a una ruta específica."""
        logging.info(f"Iniciando descarga del archivo de Drive ID: {file_id} en {local_path}")
        try:
            request = self.service.files().get_media(fileId=file_id)
            with io.FileIO(local_path, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        file_name = os.path.basename(local_path)
                        logging.info(f"Descargando {file_name}: {int(status.progress() * 100)}%.")
            logging.info(f"Archivo descargado exitosamente en: {local_path}")
            return local_path
        except HttpError as error:
            logging.error(f"Error de API al descargar el archivo {file_id}: {error}", exc_info=True)
            return None