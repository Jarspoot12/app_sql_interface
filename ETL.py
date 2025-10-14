# -*- coding: utf-8 -*- 
import pandas as pd
import os
from sqlalchemy import create_engine, inspect, text
from datetime import datetime, date
import openpyxl
import hashlib
import logging
import re
from typing import Dict, List, Tuple, Optional 

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('procesamiento_split.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuración de la conexión a PostgreSQL
DB_USER = 'app_ri_user'
DB_PASSWORD = '1234'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'app_sql'

connection_string = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(
    connection_string,
    connect_args={"options": "-c search_path=app_sql,public"},
    pool_pre_ping=True
)

COLUMN_MAPPING = {
    "principal": {
        "FOLIO": "FOLIO", "FECHA": "FECHA", "TELEFONO": "TELEFONO", "UBICACION": "UBICACION",
        "COLONIA": "COLONIA", "MUNICIPIO": "MUNICIPIO", "RCBD": "RCBD", "DESP": "DESP",
        "LLEG": "LLEG", "LIBR": "LIBR", "T1": "T1", "T2": "T2", "T3": "T3", "T4": "T4",
        "CORPORACION": "CORPORACION", "TIPO": "TIPO", "MAKEDESC": "MAKEDESC", "MODEL": "MODEL",
        "COLOR": "COLOR", "VYR": "VYR", "VLIC": "VLIC", "ST": "ST", "ADDITIONAL": "ADDITIONAL",
        "CLSDESC": "CLSDESC", "OPERADOR": "OPERADOR", "DESPACHADOR": "DESPACHADOR",
        "UNIDAD": "UNIDAD", "DIV": "DIV", "COMENTARIOS": "COMENTARIOS", "CHLNAME": "CHLNAME",
        "CHFNAME": "CHFNAME", "ORIGEN": "ORIGEN", "LATITUD": "LATITUD", "LONGITUD": "LONGITUD",
        "PROCEDENTE": "PROCEDENTE", "MTVOCIERRE": "MTVOCIERRE", "NOTACIERRE": "NOTACIERRE",
        "SECTOR": "SECTOR", "NOTASUSR": "NOTASUSR", "PERSONASINV": "PERSONASINV",
        "VEHICULOSINV": "VEHICULOSINV", "TMPTIPIFICACION": "TMPTIPIFICACION", "TMPDESPACHO": "TMPDESPACHO",
    },
    "2024": {
        "FOLIO": ("FOLIO_LLAMADA", "A"), "FECHA": ("FECHA_LLAMADA", "B"), "TELEFONO": ("NUMERO_TELEFONO", "AB"),
        "UBICACION": ("REFERENCIAS", "M"), "COLONIA": ("COLONIA", "N"), "MUNICIPIO": ("MUNICIPIO", "L"),
        "RCBD": ("HORA_LLAMADA", "I"), "DESP": ("TIEMPO_DESPACHO", "AS"), "LLEG": ("TIEMPO_LLEGADA", "AT"),
        "LIBR": ("TIEMPO_SOLUCION", "AU"), "CORPORACION": ("NOMBRE_CORPORACION", "AN"),
        "TIPO": ("TIPO DE INCIDENTE", "X"), "MAKEDESC": ("MARCA", "BQ"), "MODEL": ("MODELO", "BR"),
        "COLOR": ("COLOR", "BV"), "VYR": ("ANIO", "BU"), "VLIC": ("PLACA", "BN"), "ADDITIONAL": ("TIPO_VEHICULO", "BP"),
        "CLSDESC": ("RAZONAMIENTO_CORPORACION", "AE"), "OPERADOR": ("TELEFONISTA", "AY"),
        "DESPACHADOR": ("NOMBRE_RADIO_OPERADOR", "AZ"), "UNIDAD": ("UNIDAD", "CO"),
        "COMENTARIOS": ("DESCRIPCION_DE_LA_LLAMADA + RESPONSABLE_DE_UNIDAD", ["AM", "AP"]),
        "CHFNAME": ("NOMBRE_DENUNCIANTE", "BG"), "ORIGEN": ("ORIGEN_LLAMADA", "AA"),
        "LATITUD": ("COORDENADA_X", "S"), "LONGITUD": ("COORDENADA_Y", "T")
    },
    "2015-2023": {
        "FOLIO": ("FOLIO_LLAMADA", "S"), "FECHA": ("FECHA_LLAMADA", "B"), "TELEFONO": ("NUMERO_TELEFONO", "D"),
        "UBICACION": ("REFERENCIAS", "J"), "COLONIA": ("COLONIA", "K"), "MUNICIPIO": ("MUNICIPIO", "I"),
        "RCBD": ("HORA_LLAMADA", "C"), "DESP": ("TIEMPO_DESPACHO", "AC"), "LLEG": ("TIEMPO_LLEGADA", "AD"),
        "LIBR": ("TIEMPO_SOLUCION", "AE"), "CORPORACION": ("NOMBRE_CORPORACION", "R"),
        "TIPO": ("TIPO DE INCIDENTE", "E"), "CLSDESC": ("RAZONAMIENTO_CORPORACION", "X"),
        "OPERADOR": ("TELEFONISTA", "AI"), "DESPACHADOR": ("NOMBRE_RADIO_OPERADOR", "AJ"),
        "COMENTARIOS": ("DESCRIPCION_DE_LA_LLAMADA + RESPONSABLE_DE_UNIDAD", ["Q", "Y"]),
        "CHFNAME": ("NOMBRE_DENUNCIANTE", "AT"), "ORIGEN": ("ORIGEN_LLAMADA", "AO"),
        "LATITUD": ("COORDENADA_X", "O"), "LONGITUD": ("COORDENADA_Y", "P")
    }
}

def calculate_file_hash(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def get_processed_files():
    try:
        with engine.connect() as conn:
            create_table_query = text("""
                CREATE TABLE IF NOT EXISTS processed_files_split (
                    id SERIAL PRIMARY KEY,
                    filename VARCHAR(255),
                    file_hash VARCHAR(32),
                    processed_date TIMESTAMP,
                    version_estructura VARCHAR(50),
                    filas_principales INTEGER,
                    filas_corporaciones INTEGER
                )
            """)
            conn.execute(create_table_query)
            conn.commit()
            
            select_query = text("SELECT filename, file_hash FROM processed_files_split")
            result = conn.execute(select_query)
            return {row[0]: row[1] for row in result}
    except Exception as e:
        logger.error(f"Error al obtener archivos procesados: {str(e)}")
        return {}

def is_excel_file(filename):
    return (filename.endswith(('.xlsx', '.xls')) and not filename.startswith('~$'))

def detect_version_structure(filename: str, headers: List[str]) -> str:
    year_match = re.search(r'20(1[5-9]|2[0-4])', filename)
    if year_match:
        year = int(year_match.group())
        if year == 2024:
            estructura_2024_indicadores = ["FOLIO_LLAMADA", "NUMERO_TELEFONO", "COORDENADA_X", "TIEMPO_DESPACHO"]
            indicadores_encontrados = sum(1 for header in headers if any(indicator in header for indicator in estructura_2024_indicadores))
            return "2024" if indicadores_encontrados >= 3 else "principal"
        elif 2015 <= year <= 2023:
            return "2015-2023"
    return "principal"

def get_column_index(column_ref: str) -> int:
    result = 0
    for char in column_ref:
        result = result * 26 + (ord(char.upper()) - ord('A') + 1)
    return result - 1

def extract_column_value(row_data: List, column_ref: str) -> str:
    try:
        index = get_column_index(column_ref)
        if index < len(row_data):
            value = row_data[index]
            return str(value) if value is not None else ""
        return ""
    except:
        return ""

def combine_columns(row_data: List, column_refs: List[str]) -> str:
    values = [extract_column_value(row_data, ref).strip() for ref in column_refs]
    return " | ".join(filter(None, values))

def transform_row_data(row_data: List, version: str, headers: List[str] = None) -> Dict:
    transformed = {}
    mapping = COLUMN_MAPPING.get(version, {})
    
    for target_col, source_info in mapping.items():
        value_str = ""
        if isinstance(source_info, str):
            if headers:
                try:
                    col_index = headers.index(source_info)
                    if col_index < len(row_data):
                        value = row_data[col_index]
                        value_str = str(value) if value is not None else ""
                except ValueError:
                    value_str = ""
        elif isinstance(source_info, tuple) and len(source_info) == 2:
            if isinstance(source_info[1], str):
                value_str = extract_column_value(row_data, source_info[1])
            elif isinstance(source_info[1], list):
                value_str = combine_columns(row_data, source_info[1])
        
        transformed[target_col] = value_str

    transformed['fecha_carga'] = datetime.now()
    transformed['version_estructura'] = str(version)
    return transformed

def create_split_tables():
    try:
        with engine.connect() as conn:
            create_principal_query = text("""
                CREATE TABLE IF NOT EXISTS principal (
                    id SERIAL PRIMARY KEY, FOLIO TEXT UNIQUE NOT NULL, FECHA DATE, TELEFONO TEXT,
                    UBICACION TEXT, COLONIA TEXT, MUNICIPIO TEXT, TIPO TEXT, MAKEDESC TEXT,
                    MODEL TEXT, COLOR TEXT, VYR TEXT, VLIC TEXT, ST TEXT, ADDITIONAL TEXT,
                    CLSDESC TEXT, OPERADOR TEXT, DESPACHADOR TEXT, UNIDAD TEXT, DIV TEXT,
                    CHLNAME TEXT, CHFNAME TEXT, ORIGEN TEXT, LATITUD TEXT, LONGITUD TEXT,
                    PROCEDENTE TEXT, SECTOR TEXT, PERSONASINV TEXT, VEHICULOSINV TEXT,
                    COMENTARIOS TEXT, fecha_carga TIMESTAMP, version_estructura TEXT, origen_archivo TEXT
                )
            """)
            conn.execute(create_principal_query)
            
            create_corporaciones_query = text("""
                CREATE TABLE IF NOT EXISTS corporaciones (
                    id SERIAL PRIMARY KEY, FOLIO TEXT NOT NULL, CORPORACION TEXT, RCBD TEXT,
                    DESP TEXT, LLEG TEXT, LIBR TEXT, T1 TEXT, T2 TEXT, T3 TEXT, T4 TEXT,
                    TMPTIPIFICACION TEXT, TMPDESPACHO TEXT, fecha_carga TIMESTAMP,
                    CONSTRAINT fk_corporaciones_principal FOREIGN KEY (FOLIO) REFERENCES principal(FOLIO) 
                    ON DELETE CASCADE ON UPDATE CASCADE
                )
            """)
            conn.execute(create_corporaciones_query)
            
            try:
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_principal_folio ON principal(FOLIO)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_principal_fecha ON principal(FECHA)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_corporaciones_folio ON corporaciones(FOLIO)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_corporaciones_corporacion ON corporaciones(CORPORACION)"))
                logger.info("OK: Índices creados/verificados exitosamente")
            except Exception as e:
                logger.warning(f"WARNING: Error creando índices: {str(e)}")
            
            conn.commit()
            logger.info("OK: Estructura de 2 tablas (principal, corporaciones) creada/verificada.")
    except Exception as e:
        logger.error(f"ERROR: Error creando tablas: {str(e)}")
        raise

def split_data_into_tables(df: pd.DataFrame, filename: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(f"SPLIT: Dividiendo datos de {filename} en 2 tablas...")
    
    df_comentarios_agg = df.groupby('folio').agg({
        'comentarios': lambda x: ' | '.join(filter(None, set(x.astype(str).str.strip()))),
    }).reset_index()

    df_principal = df.drop_duplicates(subset=['folio'], keep='first').copy()
    
    if 'comentarios' in df_principal.columns:
        df_principal = df_principal.drop(columns=['comentarios'])
    df_principal = pd.merge(df_principal, df_comentarios_agg, on='folio', how='left')

    columnas_principal = [
        'folio','fecha','telefono','ubicacion','colonia','municipio', 'tipo','makedesc',
        'model','color','vyr','vlic','st','additional','clsdesc', 'operador','despachador',
        'unidad','div','chlname','chfname','origen', 'latitud','longitud','procedente',
        'sector','personasinv','vehiculosinv', 'comentarios', 'fecha_carga',
        'version_estructura','origen_archivo'
    ]
    
    columnas_existentes_principal = [col for col in columnas_principal if col in df_principal.columns]
    df_principal = df_principal[columnas_existentes_principal]
    
    columnas_corporaciones = [
        'folio','corporacion','rcbd','desp','lleg','libr','t1','t2','t3','t4',
        'tmptipificacion','tmpdespacho','fecha_carga'
    ]
    columnas_existentes_corp = [col for col in columnas_corporaciones if col in df.columns]
    df_corporaciones = df[columnas_existentes_corp].copy()
    
    logger.info(f"   - Filas principales (únicas): {len(df_principal)}")
    logger.info(f"   - Filas corporaciones: {len(df_corporaciones)}")
    
    return df_principal, df_corporaciones

def get_existing_folios():
    try:
        with engine.connect() as conn:
            result_principal = conn.execute(text("SELECT DISTINCT FOLIO FROM principal"))
            return {row[0] for row in result_principal}
    except Exception as e:
        logger.warning(f"WARNING: Error obteniendo folios existentes: {str(e)}")
        return set()

def filter_new_data(df_principal: pd.DataFrame, df_corporaciones: pd.DataFrame, 
                      existing_folios_principal: set) -> Tuple[pd.DataFrame, pd.DataFrame]:
    
    df_principal_new = df_principal[~df_principal['folio'].isin(existing_folios_principal)].copy()
    
    folios_nuevos = set(df_principal_new['folio'])
    df_corporaciones_new = df_corporaciones[df_corporaciones['folio'].isin(folios_nuevos)].copy()
    
    logger.info(f"   - Filas principales nuevas: {len(df_principal_new)}")
    logger.info(f"   - Filas corporaciones nuevas: {len(df_corporaciones_new)}")
    
    return df_principal_new, df_corporaciones_new

def process_excel_file_split(file_path: str) -> bool:
    try:
        filename = os.path.basename(file_path)
        logger.info(f"\nPROCESANDO: Procesando archivo: {filename}")

        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        sheet = wb.active
        
        headers = [str(cell.value).strip() if cell.value is not None else f'columna_{i+1}' for i, cell in enumerate(next(sheet.rows))]
        
        version = detect_version_structure(filename, headers)
        logger.info(f"VERSION: Versión detectada: {version}")

        transformed_data = []
        for i, row in enumerate(sheet.iter_rows(min_row=2)):
            row_data = [cell.value for cell in row]
            if any(x is not None for x in row_data):
                transformed_row = transform_row_data(row_data, version, headers)
                transformed_row['origen_archivo'] = filename
                transformed_data.append(transformed_row)

        if not transformed_data:
            logger.warning(f"WARNING: No se encontraron datos válidos en {filename}")
            return False

        df_unified = pd.DataFrame(transformed_data)
        df_unified.columns = [c.strip().lower() for c in df_unified.columns]
        df_unified['fecha'] = pd.to_datetime(df_unified['fecha'], errors='coerce').dt.date

        df_principal, df_corporaciones = split_data_into_tables(df_unified, filename)
        
        existing_folios_principal = get_existing_folios()
        
        df_principal_new, df_corporaciones_new = filter_new_data(
            df_principal, df_corporaciones, existing_folios_principal
        )
        
        if len(df_principal_new) == 0 and len(df_corporaciones_new) == 0:
            logger.info(f"INFO: No hay datos nuevos en {filename}")
            return True
        
        # Cargar datos
        if not df_principal_new.empty:
            df_principal_new.to_sql('principal', con=engine, if_exists='append', index=False)
            logger.info(f"OK: Tabla PRINCIPAL actualizada con {len(df_principal_new)} filas nuevas")
        
        if not df_corporaciones_new.empty:
            df_corporaciones_new.to_sql('corporaciones', con=engine, if_exists='append', index=False)
            logger.info(f"OK: Tabla CORPORACIONES actualizada con {len(df_corporaciones_new)} filas nuevas")

        file_hash = calculate_file_hash(file_path)
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO processed_files_split (filename, file_hash, processed_date, version_estructura, 
                     filas_principales, filas_corporaciones) 
                    VALUES (:filename, :file_hash, :processed_date, :version_estructura,
                            :filas_principales, :filas_corporaciones)
                """),
                {
                    "filename": filename, "file_hash": file_hash, "processed_date": datetime.now(),
                    "version_estructura": version, "filas_principales": len(df_principal_new),
                    "filas_corporaciones": len(df_corporaciones_new)
                }
            )
            conn.commit()

        logger.info(f"OK: Archivo {filename} procesado y acumulado exitosamente")
        return True

    except Exception as e:
        logger.error(f"ERROR: Error procesando {filename}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def verify_integrity():
    try:
        with engine.connect() as conn:
            orphan_corporaciones = conn.execute(text("""
                SELECT COUNT(*) FROM corporaciones c LEFT JOIN principal p ON c.FOLIO = p.FOLIO WHERE p.FOLIO IS NULL
            """)).scalar()
            
            total_principal = conn.execute(text("SELECT COUNT(*) FROM principal")).scalar()
            total_corporaciones = conn.execute(text("SELECT COUNT(*) FROM corporaciones")).scalar()
            
            logger.info("INTEGRIDAD: Verificación de relaciones:")
            logger.info(f"   - Tabla PRINCIPAL: {total_principal} registros")
            logger.info(f"   - Tabla CORPORACIONES: {total_corporaciones} registros")
            logger.info(f"   - Folios huérfanos en CORPORACIONES: {orphan_corporaciones}")
            
            if orphan_corporaciones == 0:
                logger.info("✅ INTEGRIDAD: Todas las relaciones están correctas")
                return True
            else:
                logger.warning("⚠️ INTEGRIDAD: Se encontraron folios huérfanos")
                return False
    except Exception as e:
        logger.error(f"ERROR: Error verificando integridad: {str(e)}")
        return False

def main():
    data_folder = 'DATA'
    logger.info("INICIANDO: Proceso de ETL con estructura de 2 tablas...")

    if not os.path.exists(data_folder):
        logger.error(f"ERROR: La carpeta {data_folder} no existe")
        return

    create_split_tables()
    excel_files = [f for f in os.listdir(data_folder) if is_excel_file(f)]
    if not excel_files:
        logger.warning(f"ERROR: No se encontraron archivos Excel en {data_folder}")
        return

    processed_files = get_processed_files()
    logger.info(f"ARCHIVOS: {len(excel_files)} archivos encontrados. {len(processed_files)} ya procesados.")
    
    summary = {"processed": 0, "skipped": 0, "failed": 0}
    for excel_file in excel_files:
        file_path = os.path.join(data_folder, excel_file)
        file_hash = calculate_file_hash(file_path)
        
        if excel_file in processed_files and processed_files[excel_file] == file_hash:
            logger.info(f"SALTANDO: {excel_file} (ya procesado)")
            summary["skipped"] += 1
            continue
        
        if process_excel_file_split(file_path):
            summary["processed"] += 1
        else:
            summary["failed"] += 1

    verify_integrity()
    logger.info("\nRESUMEN: Proceso de acumulación finalizado:")
    logger.info(f"   - Procesados: {summary['processed']}, Saltados: {summary['skipped']}, Fallidos: {summary['failed']}")

if __name__ == "__main__":
    main()
    logger.info("\nCOMPLETADO: Proceso de ETL finalizado.")
