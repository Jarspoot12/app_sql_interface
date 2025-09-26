# archivo que contiene toda la lógica del backend
from fastapi import FastAPI
from pydantic import BaseModel, Field #-> para definir el molde de lo que esperamos del frontend
from typing import List, Literal, Optional
import pandas as pd
import psycopg2
import io
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import datetime


# 1. Creamos una "instancia" de FastAPI. 
# La variable 'app' será el punto central de toda nuestra API.
app = FastAPI()

# Un middleware es código que se ejecuta en medio de la petición y la respuesta, agregaremos uno para comunicar el back y el front
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Permite peticiones desde tu frontend de React
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos los métodos (GET, POST, etc.)
    allow_headers=["*"],  # Permite todos los headers
)


# 2. Creamos nuestro primer "endpoint".
# Un endpoint es como una URL específica que realiza una acción.
@app.get("/") #-> esto es lo que en FastAPI se llama decorador 
def read_root():
    return {"message": "¡Hola! Mi servidor SQL está funcionando:)."}
@app.get("/home")
def read_root():
    return {"Esto es una prueba de un nuevo endpoint con el método get."}

# buscamos que FastAPI entienda la lógica de los filtros, por lo que se crea un nuevo modelo pydantic para una condición de filtro, lo añadimos al QueryRequest
#    Usamos Literal para restringir los valores a solo los que permitimos
class FilterCondition(BaseModel):
    column: str
    operator: Literal['=', '!=', '>', '>=', '<', '<=', 'startswith', 'endswith', 'contains']
    value: str
    logical: Optional[Literal['AND', 'OR']] = 'AND' # AND por defecto

# 3. Este es nuestro "molde" o "contrato" definido con Pydantic.
class QueryRequest(BaseModel): # al heredar de BaseModel se le dan poderes de validacion de datos que indica los errores automáticamente sin escribir las validaciones manualmente
    columns: List[str] = Field(default_factory=list) # esperamos una lista de textos (columnas), si no se envía hacemos una vacía
    table : str
    filters: List[FilterCondition] = Field(default_factory=list) # <-- Añadimos para los filtros
    file_type: str = 'xlsx' # tipo de archivo que da para descarga por defecto


# 4. Nos conectamos con PostgreSQL para ejecutar consultas
# se crea la lógica para hablar con la base de datos, lo hacemos en una función separada como buena práctica
DB_NAME = 'app_sql' # Complejo
DB_USER = 'app_ri_user' # postgres
DB_PASS = '1234'  # ComplejoC5
DB_HOST = 'localhost' # localhost del c5 si es en la misma máquina
DB_PORT = '5432' # 5432

# --- Función para ejecutar la consulta ---
def run_query(sql_query: str, params=None):
    try:
        # 1. Establece la conexión con la base de datos
        conn = psycopg2.connect(
            database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        # 2. Usa Pandas para ejecutar la consulta y cargar los resultados en una tabla (DataFrame)
        df = pd.read_sql_query(sql_query, conn, params=params) # pasamos la consulta y la conexión como parámetros
        # 3. Cierra la conexión para liberar recursos
        conn.close()
        return df
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e} \n Intente nuevamente.")
        return pd.DataFrame() # Devuelve una tabla vacía si hay un error
    
#función para construir la cláusula WHERE de los filtros
def build_where_clause(filters: List[FilterCondition], table_schema: List[dict]):
    """
    Construye de forma segura una cláusula WHERE y una lista de parámetros para evitar inyección SQL.
    Esta versión es consciente de los tipos de datos.
    """
    if not filters:
        return "", []

    column_type_map = {col["column_name"]: col["data_type"] for col in table_schema}
    
    query_parts = []
    params = []
    
    for i, f in enumerate(filters):
        allowed_columns = column_type_map.keys()
        if f.column not in allowed_columns:
            print(f"Advertencia: Se intentó filtrar por una columna no permitida: {f.column}")
            continue

        operator_map = {
            '=': '=', '!=': '!=', '>': '>', '>=': '>=', '<': '<', '<=': '<=',
            'startswith': 'LIKE', 'endswith': 'LIKE', 'contains': 'LIKE'
        }
        sql_operator = operator_map[f.operator]
        
        value_to_process = f.value
        column_type = column_type_map.get(f.column)
        final_value = value_to_process

        try:
            if column_type in ('integer', 'bigint', 'smallint', 'numeric', 'real', 'double precision'):
                final_value = float(value_to_process) if '.' in value_to_process else int(value_to_process)
            elif column_type in ('date', 'timestamp', 'timestamp without time zone'):
                final_value = datetime.date.fromisoformat(value_to_process)
        except (ValueError, TypeError):
            print(f"Advertencia: no se pudo convertir '{value_to_process}' para la columna '{f.column}'")
            final_value = value_to_process
        
        if f.operator in ['startswith', 'endswith', 'contains']:
            if f.operator == 'startswith':
                final_value = f"{final_value}%"
            elif f.operator == 'endswith':
                final_value = f"%{final_value}"
            elif f.operator == 'contains':
                final_value = f"%{final_value}%"
        
        logical_connector = f" {f.logical}" if i > 0 else ""
        query_parts.append(f'{logical_connector} "{f.column}" {sql_operator} %s')
        params.append(final_value)

    if not query_parts:
        return "", []

    # Se construye con un espacio inicial para que se una correctamente a la consulta principal
    full_where_clause = "WHERE" + "".join(query_parts)
    return full_where_clause, params

# 5. Creamos el endpoint (ruta) para el preview de datos
# recibe los parámetros del front , usa run_query y devuelve las primeras 20 filas
@app.post("/api/query") # Como necesitas enviar un cuerpo de datos para que el servidor procese tu solicitud, se debe usar POST.
def handle_query(request: QueryRequest):
    # 1. Construimos la consulta SQL de forma segura
    cols = ", ".join(request.columns) if request.columns else "*" # columnas a extraer
    table_name = request.table # tabla objetivo
    # obtenemos columnas permitidas para validación
    schema = get_schema() # Reutilizamos la función del endpoint /api/schema
    table_schema_data = schema.get(request.table, []) # Esto es la lista de objetos con tipos de dato
    where_clause, params = build_where_clause(request.filters, table_schema_data)

    # consulta en lenguaje SQL
    query = f"SELECT {cols} FROM {table_name} {where_clause};"
    
    # 2. Ejecutamos la consulta usando nuestra función
    df = run_query(query, params)
    
    # 3. Preparamos y devolvemos el preview
    # .head(20) toma las primeras 20 filas
    # .to_dict(orient='records') convierte la tabla a un formato JSON ideal para el frontend
    preview_data = df.head(20).to_dict(orient='records')
    return preview_data

# 6. Crear el endpoint para descargar archivos
# aquí, en lugar de devolver un JSON devolvemos un archivo completo
@app.post("/api/download")
def download_file(request: QueryRequest):
    # 1. Construcción y ejecución de la consulta (igual que antes)
    cols = ", ".join(request.columns) if request.columns else "*"
    table_name = request.table
    schema = get_schema() # Reutilizamos la función del endpoint /api/schema
    table_schema_data = schema.get(request.table, []) # Esto es la lista de objetos con tipos de dato
    where_clause, params = build_where_clause(request.filters, table_schema_data)

    query = f"SELECT {cols} FROM {table_name} {where_clause};"
    df = run_query(query, params)
    
    # 2. Creamos un "archivo virtual" en la memoria RAM del servidor para no escribir y luego leer un archivo desde el disco duro
    buffer = io.BytesIO()
    
    # 3. Guardamos el DataFrame en el buffer según el tipo de archivo
    if request.file_type == 'xlsx':
        df.to_excel(buffer, index=False, engine='openpyxl')
        media_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        filename = 'resultado.xlsx'
    else: # Por defecto es CSV
        df.to_csv(buffer, index=False, encoding='utf-8')
        media_type = 'text/csv'
        filename = 'resultado.csv'

        
    # 4. Rebobinamos el buffer al principio para que pueda ser leído
    buffer.seek(0)
    
    # 5. Devolvemos el buffer como una respuesta de archivo
    return StreamingResponse( # forma de FastAPI para enviar archivos o grandes cantidades de datos
        buffer,
        media_type=media_type,
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )

# 7. Endpoint para crar la lista de tablas y columnas para mostrar en el frontend
@app.get("/api/schema")
def get_schema():
    conn = psycopg2.connect(
        database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()
    # Obtener todas las tablas del esquema app_sql
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'app_sql'
    """)
    tables = [row[0] for row in cursor.fetchall()]
    
    schema = {}
    for table in tables:
        # Obtener las columnas para cada tabla
        cursor.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'app_sql' AND table_name = '{table}'
        """)
        columns_with_types = [{"column_name": row[0], "data_type": row[1]} for row in cursor.fetchall()]
        schema[table] = columns_with_types
        
    cursor.close()
    conn.close()
    return schema

