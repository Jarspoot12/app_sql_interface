# Base de datos
## 1. Conexión con postgreSQL para la inserción de datos
Se deben especificar los siguientes parámetros para la ruta de acceso:
**Configuración de la conexión a PostgreSQL local (mi pc) | pc servidor del c5**
- DB_USER = 'app_ri_user' # postgres
- DB_PASSWORD = '1234'  # ComplejoC5
- DB_HOST = 'localhost' # localhost del c5 si es en la misma máquina
- DB_PORT = '5432' # 5432
- DB_NAME = 'app_sql' # Complejo

**Crear la cadena de conexión**

- connection_string = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
- engine = create_engine(connection_string)

Si el servidor es remoto, entonces:
- El cliente abre una conexión TCP hacia esa IP, puerto 5432.
- El router/firewall de la red debe permitir el tráfico.
- En el servidor, PostgreSQL debe estar escuchando en listen_addresses = '*' y permitir tu IP en pg_hba.conf. Considerando que mi servidor está en *la misma red*.
- O si es remoto y en otra red, se debe considerar algo como un túnel SSH.

## 2. Subir datos desde la carpeta DATA
Se colocan en la carpeta DATA los archivos a subir a la base en postgreSQL. Para el script actual fue necesario corregir el código fuente para hacer coincidir el dataframe de trabajo y la base en postgreSQL, cambiando principalmente el nombre de las columnas y la forma de acceder a columnas.

## 3. Revisar nuevos datos en la base
Una vez ejecutado el script con archivos nuevos, deben aparecer nuevos datos en la base.

# Backend
lo que hice al probar en localhost/docs desde el servidor uvicorn fue simular que presioné el botón de "enviar consulta", donde envié a la ruta /api/query como si fuera un fetch apuntando con el método POST, de ahí me devolvió lo que produjo el backend de hablar con la base de datos

# Frontend
Instalaremos Node.js para usar React, se instala la versión LTS sin marcar la casilla de elementos adicionales, creamos un proyecto con npx create-react-app frontend y navegamos dentro de él. Instalamos dos librerías adicionales: MUI para tener componentes UI listos para usar y Axios para comunicarnos con el backend en FastAPI.

Nuestro primer requisito nos pide crear un nuevo endpoint en la base de datos, lo llamaremos @app.get("/api/schema") que nos será útil para poblar las listas desplegables de tablas y columnas.
Una vez establecido el middleware para permitir la conexión, ejecutamos cada capa en una terminal distinta para levartarlas al mismo tiempo.
