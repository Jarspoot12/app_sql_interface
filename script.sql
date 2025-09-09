-- Database: app_sql

-- DROP DATABASE IF EXISTS app_sql;

CREATE DATABASE app_sql
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Spanish_Mexico.1252'
    LC_CTYPE = 'Spanish_Mexico.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
	
-- en la BASE de trabajo (p.ej., app_sql)
CREATE ROLE app_ri_user LOGIN PASSWORD '1234';

-- si vas a usar el ESQUEMA app_sql (que ya creaste):
GRANT USAGE ON SCHEMA app_sql TO app_ri_user;

-- permiso para crear tablas en el esquema
GRANT CREATE ON SCHEMA app_sql TO app_ri_user;

-- dar permisos por defecto a nuevas tablas del esquema
ALTER DEFAULT PRIVILEGES IN SCHEMA app_sql
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_ri_user;

-- si ya existen tablas y quieres otorgar permisos:
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA app_sql TO app_user;

-- (opcional) fijar search_path
ALTER ROLE app_ri_user SET search_path = app_sql, public;



------- Probamos consultas
SELECT * 
FROM app_sql.principal;

SELECT * 
FROM app_sql.corporaciones;

SELECT *
FROM app_sql.principal
WHERE id BETWEEN 295000 and 305000;