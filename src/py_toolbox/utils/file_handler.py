# src/py_toolbox/utils/file_handler.py
import json
import logging
from pathlib import Path

class FileHandler:
    """
    Utilidades para manejar operaciones comunes del sistema de archivos.
    """
    @staticmethod
    def ensure_dirs(*dir_paths: str | Path):
        """
        Asegura que uno o más directorios existan, creándolos si es necesario.
        """
        for dir_path in dir_paths:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            logging.info(f"Directorio asegurado: {dir_path}")

    @staticmethod
    def write_text(file_path: str | Path, content: str, encoding: str = 'utf-8'):
        """
        Escribe contenido de texto a un archivo.
        """
        try:
            path_obj = Path(file_path)
            # Asegurarse de que el directorio padre existe
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            path_obj.write_text(content, encoding=encoding)
            logging.info(f"Archivo de texto guardado en: {file_path}")
        except Exception as e:
            logging.error(f"Error al escribir archivo de texto en {file_path}: {e}")

    @staticmethod
    def write_json(file_path: str | Path, data: dict | list, pretty: bool = True, encoding: str = 'utf-8'):
        """
        Escribe un diccionario o lista a un archivo JSON.

        :param file_path: Ruta del archivo de salida.
        :param data: El objeto de Python (dict o list) a serializar.
        :param pretty: Si es True, el JSON se guardará con indentación para fácil lectura.
        """
        try:
            path_obj = Path(file_path)
            # Asegurarse de que el directorio padre existe
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            indent = 4 if pretty else None
            with open(path_obj, 'w', encoding=encoding) as f:
                json.dump(data, f, ensure_ascii=False, indent=indent)
            logging.info(f"Archivo JSON guardado en: {file_path}")
        except Exception as e:
            logging.error(f"Error al escribir archivo JSON en {file_path}: {e}")