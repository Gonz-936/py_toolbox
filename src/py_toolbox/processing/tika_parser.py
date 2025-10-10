# src/py_toolbox/processing/tika_parser.py
import logging
import requests 
from pathlib import Path

class TikaParser:
    """
    Una clase de utilidad para interactuar con un servidor Apache Tika.
    Actúa como un cliente HTTP, enviando archivos a un servidor ya en ejecución.
    """
    # El servidor Tika se ejecutará en localhost dentro del mismo contenedor
    TIKA_SERVER_URL = "http://localhost:9998/tika"

    @staticmethod
    def pdf_to_html(pdf_path: str, output_dir: str) -> str | None:
        """
        Envía un archivo PDF a un servidor Tika para su conversión a HTML
        y lo guarda en un directorio. La lógica de negocio no cambia:
        recibe una ruta de PDF y devuelve una ruta de HTML.

        :param pdf_path: La ruta al archivo PDF de entrada.
        :param output_dir: El directorio donde se guardará el archivo HTML.
        :return: La ruta al archivo HTML generado o None si falla.
        """
        pdf_path_obj = Path(pdf_path)
        output_dir_obj = Path(output_dir)
        output_dir_obj.mkdir(parents=True, exist_ok=True)

        html_file_name = pdf_path_obj.stem + ".html"
        html_out_path = output_dir_obj / html_file_name

        logging.info(f"Tika Client: Enviando {pdf_path} al servidor Tika en {TikaParser.TIKA_SERVER_URL}")
        
        try:
            with open(pdf_path_obj, 'rb') as f:
                headers = { "Accept": "text/html" }
                response = requests.put(TikaParser.TIKA_SERVER_URL, data=f, headers=headers)
                response.raise_for_status() 

            html = response.text
            if not html:
                logging.error("Tika Server no devolvió contenido del PDF.")
                return None

            html_out_path.write_text(html, encoding="utf-8")
            logging.info(f"Tika Client: HTML recibido y guardado en: {html_out_path}")
            return str(html_out_path)

        except requests.exceptions.RequestException as e:
            logging.error(f"Tika Client: Ocurrió un error de red al contactar al servidor Tika: {e}", exc_info=True)
            return None
        except Exception as e:
            logging.error(f"Tika Client: Ocurrió un error inesperado procesando el PDF: {e}", exc_info=True)
            return None