# src/py_toolbox/aws/athena.py
import boto3
import time
import pandas as pd
import logging

class Athena:
    def __init__(self, database, s3_output_location, region_name="us-east-1"):
        self.athena_client = boto3.client("athena", region_name=region_name)
        self.database = database
        self.s3_output_location = s3_output_location
        logging.info(f"Conector de Athena inicializado para la base de datos '{database}'.")

    # ... los métodos _execute_query y _get_query_results_dataframe no cambian ...
    def _execute_query(self, query):
        try:
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": self.database},
                ResultConfiguration={"OutputLocation": self.s3_output_location},
            )
            logging.info(f"Consulta de Athena iniciada. ID de ejecución: {response['QueryExecutionId']}")
            return response["QueryExecutionId"]
        except Exception as e:
            logging.error(f"No se pudo iniciar la consulta de Athena. Error: {e}", exc_info=True)
            raise

    def _get_query_results_dataframe(self, query_execution_id):
        try:
            results_paginator = self.athena_client.get_paginator("get_query_results")
            results_iter = results_paginator.paginate(
                QueryExecutionId=query_execution_id, PaginationConfig={"PageSize": 1000}
            )
            data = []
            columns = []
            first_page = True
            for page in results_iter:
                if first_page:
                    if not page["ResultSet"]["Rows"]: return pd.DataFrame(columns=columns) # Tabla vacía
                    columns = [col["Label"] for col in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
                    rows = page["ResultSet"]["Rows"][1:]
                    first_page = False
                else:
                    rows = page["ResultSet"]["Rows"]

                for row in rows:
                    data.append([item.get("VarCharValue") for item in row["Data"]])
            
            logging.info(f"Resultados de la consulta {query_execution_id} convertidos a DataFrame.")
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            logging.error(f"No se pudieron obtener los resultados de la consulta {query_execution_id}. Error: {e}", exc_info=True)
            raise
    
    def get_query_execution_details(self, query_execution_id: str) -> dict:
        """
        Obtiene el estado y la razón del fallo de una consulta.
        """
        try:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response["QueryExecution"]["Status"]
            return {
                "State": status.get("State"),
                "StateChangeReason": status.get("StateChangeReason", "No reason provided.")
            }
        except Exception as e:
            logging.error(f"Fallo al verificar el estado de la consulta {query_execution_id}. Error: {e}", exc_info=True)
            raise

    def get_query_results(self, query: str) -> pd.DataFrame:
        """
        Orquesta la ejecución de una consulta y devuelve los resultados.
        Si la consulta falla, lanza una excepción con el motivo del fallo.
        """
        query_execution_id = self._execute_query(query)

        while True:
            details = self.get_query_execution_details(query_execution_id)
            state = details["State"]

            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                logging.info(f"La consulta {query_execution_id} ha finalizado con estado: {state}")
                break
            time.sleep(1)

        if state == "SUCCEEDED":
            return self._get_query_results_dataframe(query_execution_id)
        else:
            # <<< LÓGICA MEJORADA >>>
            # Ahora incluimos la razón del fallo en el mensaje de error.
            error_reason = details["StateChangeReason"]
            error_message = f"La consulta de Athena {query_execution_id} falló. Razón: {error_reason}"
            logging.error(error_message)
            raise Exception(error_message)