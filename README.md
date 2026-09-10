# ETL TT2

Pipeline centralizado de extracción y limpieza de datos multiplataforma (ETL) desarrollado en Python.

## Estructura
- \src/extract/\: Scripts de conexión y descarga desde APIs (YouTube, Instagram).
- \src/transform/\: Scripts de limpieza y estandarización usando Pandas.
- \src/load/\: Scripts de carga hacia Azure SQL Database.

## Instalación
1. Clonar el repositorio.
2. Crear un entorno virtual: \python -m venv venv3. Activar entorno e instalar dependencias: \pip install -r requirements.txt4. Copiar el archivo \.env.example\ y renombrarlo a \.env\, rellenando con tus credenciales reales. (NUNCA subas el archivo \.env\ a GitHub).
