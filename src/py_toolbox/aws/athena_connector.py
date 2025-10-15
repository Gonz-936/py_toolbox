import sys
import pandas as pd
from awsglue.utils import getResolvedOptions
from datetime import datetime

# ==============================================================================
# 1. IMPORTACIONES DESDE LA LIBRERÍA CENTRAL 'py_toolbox'
# ==============================================================================
try:
    from py_toolbox.aws.athena_connector import AthenaConnector
    from py_toolbox.aws.s3_uploader import S3Uploader
    print("INFO: Módulos de py_toolbox (AthenaConnector, S3Uploader) importados exitosamente.")
except ImportError as e:
    print(f"ERROR CRÍTICO: No se pudieron importar los módulos de py_toolbox. Error: {e}")
    sys.exit(1)

# ==============================================================================
# 2. CONSTANTES Y CONFIGURACIÓN DEL JOB
# ==============================================================================
# Las constantes de configuración se eliminan. Ahora se leerán desde los argumentos del job.

FINAL_COLUMNS = [
    'DOCUMENT TYPE', 'INVOICE NUMBER', 'CURRENCY', 'BILLING CYCLE DATE',
    'INVOICE ICA', 'ACTIVITY ICA', 'BILLABLE ICA', 'COLLECTION METHOD',
    'SERVICE CODE', 'SERVICE CODE DESCRIPTION', 'EVENT ID', 'EVENT DESCRIPTION',
    'AFFILIATE', 'UOM', 'QUANTITY AMOUNT', 'RATE', 'CHARGE', 'TAX CHARGE',
    'TOTAL CHARGE', 'FEE TYPE', 'CATEGORY', 'SUBCATEGORY', 'GDRIVE_FILE_ID'
]
COLUMNS_WITH_DEFAULTS = {
    'FEE TYPE': 'Not Available',
    'CATEGORY': 'Not Available',
    'SUBCATEGORY': 'Not Available',
    'GDRIVE_FILE_ID': 'Not Available'
}

# ==============================================================================
# 3. LÓGICA DE ORQUESTACIÓN DEL JOB
# ==============================================================================
def main():
    # --- Obtención de Parámetros ---
    # AÑADIMOS LOS NUEVOS PARÁMETROS DE CONFIGURACIÓN AQUÍ
    args = getResolvedOptions(sys.argv, [
        'S3_INPUT_BUCKET',
        'S3_INPUT_KEY',
        'S3_OUTPUT_PATH',
        'ATHENA_DATABASE',
        'ATHENA_TABLE',
        'AWS_REGION',
        'ATHENA_OUTPUT_LOCATION'
    ])
    
    s3_input_bucket = args['S3_INPUT_BUCKET']
    s3_input_key = args['S3_INPUT_KEY']
    s3_output_path = args['S3_OUTPUT_PATH']
    athena_database = args['ATHENA_DATABASE']
    athena_table = args['ATHENA_TABLE']
    aws_region = args['AWS_REGION']
    athena_output_location = args['ATHENA_OUTPUT_LOCATION']
    
    # --- Inicialización de Conectores de py_toolbox con los parámetros recibidos ---
    s3_uploader = S3Uploader(region_name=aws_region)
    athena_connector = AthenaConnector(database=athena_database, s3_output_location=athena_output_location, region_name=aws_region)

    
    gdrive_file_id = s3_uploader.get_object_metadata(s3_input_bucket, s3_input_key, 'gdrive_file_id', COLUMNS_WITH_DEFAULTS['GDRIVE_FILE_ID'])
    source_uri = f"s3://{s3_input_bucket}/{s3_input_key}"
    print(f"INFO: Iniciando procesamiento para archivo: {source_uri} (GDrive ID: {gdrive_file_id})")

    try:
        df = pd.read_csv(source_uri)
        df.columns = df.columns.str.strip()
        for col, default_value in COLUMNS_WITH_DEFAULTS.items():
            if col not in df.columns:
                df[col] = default_value
        df['GDRIVE_FILE_ID'] = gdrive_file_id
        df_final = df.reindex(columns=FINAL_COLUMNS)
    except Exception as e:
        print(f"ERROR: Fallo en la lectura o transformación del CSV. Moviendo a skipped/. Error: {e}")
        s3_uploader.move_file(s3_input_bucket, s3_input_key, 'invoices/skipped/')
        raise e

    invoice_number = df_final['INVOICE NUMBER'].iloc[0]
    print(f"INFO: Verificando duplicados para INVOICE_NUMBER: {invoice_number}")

    athena_query = f"""SELECT COUNT(*) AS count FROM "{athena_table}" WHERE "invoice number" = '{invoice_number}'"""
    
    try:
        query_results_df = athena_connector.get_query_results(athena_query)
        if not query_results_df.empty and query_results_df['count'].iloc[0] > 0:
            print(f"ADVERTENCIA: Factura '{invoice_number}' ya procesada.")
            s3_uploader.move_file(s3_input_bucket, s3_input_key, 'invoices/skipped/')
            return
    except Exception as e:
        print(f"ERROR: Falló la verificación de duplicados en Athena. Moviendo a skipped/. Error: {e}")
        s3_uploader.move_file(s3_input_bucket, s3_input_key, 'invoices/skipped/')
        raise e
        
    try:
        date_obj = pd.to_datetime(df_final['BILLING CYCLE DATE'].iloc[0], errors='coerce', infer_datetime_format=True)
        if pd.isna(date_obj):
             date_obj = datetime.utcnow()
        partition_path = f"year={date_obj.year}/month={date_obj.month:02d}/day={date_obj.day:02d}"
        
        output_key = f"{s3_output_path.replace(f's3://{s3_input_bucket}/', '')}/{partition_path}/{s3_input_key.split('/')[-1].replace('.csv', '.parquet')}"
        
        print(f"INFO: Escribiendo archivo Parquet en: s3://{s3_input_bucket}/{output_key}")
        s3_uploader.upload_dataframe_as_parquet(df_final, s3_input_bucket, output_key)
        
        s3_uploader.delete_file(s3_input_bucket, s3_input_key)
    except Exception as e:
        print(f"ERROR: No se pudo escribir el archivo Parquet en S3. Error: {e}")
        raise e

if __name__ == "__main__":
    main()