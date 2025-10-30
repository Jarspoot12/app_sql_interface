# -*- coding: utf-8 -*-
# archivo que contiene toda la lógica del backend
from fastapi import FastAPI
from pydantic import BaseModel, Field
from typing import List, Literal, Optional, Union, Tuple
import pandas as pd
import psycopg2
import io
import datetime
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

# 1. Creamos una "instancia" de FastAPI.
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Modelos de Datos Pydantic ---
class FilterCondition(BaseModel):
    column: str
    operator: Literal['=', '!=', '>', '>=', '<', '<=', 'startswith', 'endswith', 'contains', 'between']
    value: Union[str, List[str]]
    logical: Optional[Literal['AND', 'OR']] = 'AND'

class QueryRequest(BaseModel):
    table: str
    columns: List[str] = Field(default_factory=list)
    filters: List[FilterCondition] = Field(default_factory=list)
    file_type: str = 'xlsx'

# --- Configuración de la Base de Datos ---
DB_NAME = 'app_sql'
DB_USER = 'app_ri_user'
DB_PASS = '1234'
DB_HOST = 'localhost'
DB_PORT = '5432'

# --- Funciones Auxiliares de Lógica ---

def _process_single_condition(f: FilterCondition, col_type: str) -> Tuple[Optional[str], List, Optional[str]]:
    """
    Función auxiliar interna.
    Procesa una única condición de filtro.
    Devuelve: (fragmento_sql, lista_de_parametros, texto_legible_para_case)
    """
    sql_snippet = None
    params_snippet = []
    case_string = None
    
    try:
        # 1. Crear el texto legible (ej. "folio: 11111")
        if f.operator == 'between':
            if isinstance(f.value, list) and len(f.value) == 2:
                case_string = f"{f.column}: {f.value[0]} / {f.value[1]}"
            else:
                return None, [], None # 'between' mal formado
        else:
            case_string = f"{f.column}: {f.value}"

        # 2. Construir el fragmento de SQL y los parámetros
        if f.operator == 'between':
            val1, val2 = f.value
            if col_type in ('integer', 'bigint', 'numeric', 'real', 'double precision'):
                param1, param2 = (float(val1) if '.' in val1 else int(val1)), (float(val2) if '.' in val2 else int(val2))
            elif 'date' in col_type or 'timestamp' in col_type:
                param1, param2 = datetime.date.fromisoformat(val1), datetime.date.fromisoformat(val2)
            else:
                param1, param2 = val1, val2 # 'between' para texto
            
            sql_snippet = f'"{f.column}" BETWEEN %s AND %s'
            params_snippet = [param1, param2]
        
        else: # Todos los demás operadores
            operator_map = {'=': '=', '!=': '!=', '>': '>', '>=': '>=', '<': '<', '<=': '<=', 'startswith': 'LIKE', 'endswith': 'LIKE', 'contains': 'LIKE'}
            sql_operator = operator_map.get(f.operator)
            if not sql_operator: return None, [], None

            value_to_process = str(f.value)
            final_value = value_to_process
            is_text_type = 'char' in col_type or 'text' in col_type
            
            if is_text_type:
                final_value = value_to_process.upper() # Normalización a mayúsculas
                if f.operator == 'startswith': final_value = f"{final_value}%"
                elif f.operator == 'endswith': final_value = f"%{final_value}"
                elif f.operator == 'contains': final_value = f"%{final_value}%"
                sql_snippet = f'UPPER("{f.column}") {sql_operator} %s'
            else:
                if col_type in ('integer', 'bigint', 'numeric', 'real', 'double precision'):
                    final_value = float(value_to_process) if '.' in value_to_process else int(value_to_process)
                elif 'date' in col_type or 'timestamp' in col_type:
                    final_value = datetime.date.fromisoformat(value_to_process)
                sql_snippet = f'"{f.column}" {sql_operator} %s'
            
            params_snippet = [final_value]
    
    except (ValueError, TypeError, AttributeError, IndexError):
        print(f"Advertencia: Valor de filtro no válido para {f.column}")
        return None, [], None

    return sql_snippet, params_snippet, case_string


def build_filter_logic(filters: List[FilterCondition], table_schema: List[dict]):
    """
    Construye la lógica de filtrado completa para WHERE y CASE.
    Devuelve: (where_clause_sql, case_clause_sql, case_params_list, where_only_params)
    """
    if not filters:
        return "", "", [], []

    column_type_map = {col["column_name"]: col["data_type"] for col in table_schema}
    
    case_params = []        # Parámetros para la consulta SELECT (incluye CASE)
    where_only_params = []  # Parámetros solo para la consulta WHERE (para la descarga)
    where_groups_sql = []   # Grupos 'OR' de 'AND's. Ej: ["(A AND B)", "(C)"]
    case_whens_sql = []     # Lista de 'WHEN ... THEN ...'
    
    current_and_sql = []
    current_and_params = []
    current_and_case_strings = []

    def finalize_group(sql_parts, params, case_strings):
        """Función interna para procesar un grupo de ANDs"""
        if not sql_parts:
            return
        
        group_sql = f"({' AND '.join(sql_parts)})"
        where_groups_sql.append(group_sql)
        
        case_match_string = '; '.join(case_strings)
        case_whens_sql.append(f"WHEN {group_sql} THEN %s")
        
        case_params.extend(params)
        case_params.append(case_match_string)

    for i, f in enumerate(filters):
        if f.column not in column_type_map:
            continue
        
        col_type = column_type_map.get(f.column)
        sql_part, params_part, case_str = _process_single_condition(f, col_type)
        
        if not sql_part:
            continue
            
        where_only_params.extend(params_part)

        if f.logical == 'OR' and i > 0:
            finalize_group(current_and_sql, current_and_params, current_and_case_strings)
            current_and_sql = [sql_part]
            current_and_params = params_part
            current_and_case_strings = [case_str]
        else:
            current_and_sql.append(sql_part)
            current_and_params.extend(params_part)
            current_and_case_strings.append(case_str)
            
    finalize_group(current_and_sql, current_and_params, current_and_case_strings)

    if not where_groups_sql:
        return "", "", [], []

    final_where_clause = "WHERE " + " OR ".join(where_groups_sql)
    final_case_clause = f"(CASE {' '.join(case_whens_sql)} ELSE 'Coincidencia no agrupada' END) AS \"Coincidencia de Filtro\""
    
    return final_where_clause, final_case_clause, case_params, where_only_params

def run_query(sql_query: str, params=None):
    try:
        conn = psycopg2.connect(
            database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        df = pd.read_sql_query(sql_query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e} \n Intente nuevamente.")
        return pd.DataFrame()

# --- Endpoints de la API ---

@app.get("/")
def read_root():
    return {"message": "¡Hola! Mi servidor SQL está funcionando:)."}

@app.get("/api/schema")
def get_schema():
    conn = psycopg2.connect(
        database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'app_sql'
    """)
    tables = [row[0] for row in cursor.fetchall()]
    
    schema = {}
    for table in tables:
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

@app.post("/api/query")
def handle_query(request: QueryRequest):
    # Usar comillas dobles para nombres de columnas y tablas
    cols_list = [f'"{c}"' for c in request.columns] if request.columns else ["*"]
    cols = ", ".join(cols_list)
    table_name = f'"{request.table}"'
    
    schema = get_schema()
    table_schema_data = schema.get(request.table, [])
    
    # --- INICIO DE LA CORRECCIÓN ---
    # 1. Capturar TODOS los parámetros devueltos
    where_sql, case_sql, case_params, where_only_params = build_filter_logic(request.filters, table_schema_data)

    if case_sql:
        # Si hay filtros, construimos la consulta completa
        
        # Manejar el SELECT * correctamente con la nueva columna
        if cols == "*":
            query = f"SELECT {table_name}.*, {case_sql} FROM {table_name} {where_sql};"
        else:
            query = f"SELECT {cols}, {case_sql} FROM {table_name} {where_sql};"
        
        # 2. Combinar las DOS listas de parámetros correctas
        #    Los parámetros del CASE (WHEN...THEN...) + los parámetros del WHERE
        params_totales = case_params + where_only_params
        df = run_query(query, params_totales)
    else:
        # Sin filtros, consulta normal
        query = f"SELECT {cols} FROM {table_name};"
        df = run_query(query) # No hay parámetros
    
    # --- FIN DE LA CORRECCIÓN ---
    
    total_count = len(df)
    preview_data = df.head(20).to_dict(orient='records')
    
    return {
        "totalCount": total_count,
        "previewData": preview_data
    }

@app.post("/api/download")
def download_file(request: QueryRequest):
    cols_list = [f'"{c}"' for c in request.columns] if request.columns else ["*"]
    cols = ", ".join(cols_list)
    table_name = f'"{request.table}"'
    
    schema = get_schema()
    table_schema_data = schema.get(request.table, [])
    
    # Solo necesitamos la lógica del WHERE para la descarga
    where_sql, _, _, where_only_params = build_filter_logic(request.filters, table_schema_data)

    # Consulta de descarga SIN la columna de coincidencia
    query = f"SELECT {cols} FROM {table_name} {where_sql};"
    
    df = run_query(query, where_only_params)
    
    buffer = io.BytesIO()
    if request.file_type == 'xlsx':
        df.to_excel(buffer, index=False, engine='openpyxl')
        media_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        filename = 'resultado.xlsx'
    else:
        df.to_csv(buffer, index=False, encoding='utf-8')
        media_type = 'text/csv'
        filename = 'resultado.csv'
        
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type=media_type,
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )