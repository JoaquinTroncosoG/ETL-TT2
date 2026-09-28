# ETL
Pipeline centralizado de extracción y limpieza de datos multiplataforma (ETL) desarrollado en Python.

## Estructura
- `tools/`: Scripts de uso manual para gestionar credenciales y probar conexiones.
- `src/extract/`: Scripts de conexión y descarga desde APIs (YouTube, Instagram).
- `src/transform/`: Scripts de limpieza y estandarización usando Pandas.
- `src/load/`: Scripts de carga hacia Azure SQL Database.
- `main.py`: Punto de entrada único del pipeline.

## Instalación
1. Clonar el repositorio.
2. Crear un entorno virtual: `python -m venv venv`
3. Activar entorno e instalar dependencias: `pip install -r requirements.txt`
4. Copiar el archivo `.env.example` y renombrarlo a `.env`, rellenando con tus credenciales reales. (NUNCA subas el archivo `.env` a GitHub).

## Autenticación y Credenciales
Para facilitar la gestión de los tokens (OAuth 2.0 de YouTube y tokens de Meta), existen 3 scripts de acceso rápido en la carpeta `tools/`. **Recuerda ejecutarlos con el entorno virtual activado**:

* **Revisar el estado de las credenciales:**
  ```
  python tools/estado_credenciales.py
  ```

* **Renovar el acceso a YouTube** (Abre el navegador para re-autorizar):
  ```
  python tools/renovar_youtube.py
  ```

* **Canjear token de Instagram** (Cambia un token corto de Meta por uno de 60 días y lo guarda automáticamente en tu `.env`):
  ```
  python tools/canjear_instagram.py <TOKEN_CORTO_AQUI>
  ```
