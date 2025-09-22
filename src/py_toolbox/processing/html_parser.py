# src/py_toolbox/processing/html_parser.py
import logging
from pathlib import Path
from bs4 import BeautifulSoup

class HtmlParser:
    """
    Una clase de utilidad para cargar y parsear contenido HTML desde un archivo.
    """
    @staticmethod
    def get_soup(html_path: str | Path, parser: str = 'html.parser') -> BeautifulSoup | None:
        """
        Lee un archivo HTML y devuelve un objeto BeautifulSoup para su análisis.

        :param html_path: La ruta al archivo HTML de entrada.
        :param parser: El parser que BeautifulSoup debe usar. 'html.parser' es el
                       integrado en Python.
        :return: Un objeto BeautifulSoup o None si el archivo no se encuentra o hay un error.
        """
        try:
            path_obj = Path(html_path)
            logging.info(f"Cargando archivo HTML desde: {path_obj}")
            content = path_obj.read_text(encoding="utf-8")
            soup = BeautifulSoup(content, parser)
            logging.info("Archivo HTML cargado y parseado exitosamente en BeautifulSoup.")
            return soup
        except FileNotFoundError:
            logging.error(f"No se encontró el archivo HTML en la ruta: {html_path}")
            return None
        except Exception as e:
            logging.error(f"Ocurrió un error al leer o parsear el archivo HTML: {e}", exc_info=True)
            return None