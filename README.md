# Pipeline ETL Multi-Plataforma (ETL-TT2)

Pipeline centralizado de extracción, limpieza y carga de datos multiplataforma (ETL) desarrollado en Python para consolidar métricas de redes sociales (**YouTube Data API v3** e **Instagram Graph API**) en un Data Warehouse relacional sobre **Microsoft Azure SQL Database**.

---

## 📁 Estructura del Proyecto

```text
├── data/                      # Almacenamiento local (ignorado en git)
│   ├── raw/                   # Respuestas JSON crudas de las APIs (auditoría)
│   └── processed/             # Tablas limpias exportadas en CSV / Excel
├── sql/
│   └── crear_tablas.sql       # Script DDL del Modelo Dimensional (T-SQL)
├── src/
│   ├── extract/               # FASE 1: Extracción desde APIs
│   │   ├── extract_youtube.py
│   │   └── extract_instagram.py
│   ├── transform/             # FASE 2: Limpieza y normalización
│   │   ├── cargar_json.py     # Lectura y aplanado de JSONs crudos
│   │   ├── transform_data.py  # Tratamiento de nulos, deduplicación y casteo
│   │   └── normalizar.py      # Estandarización a esquema común y UTC
│   ├── load/                  # FASE 3: Persistencia en Azure SQL
│   │   ├── crear_tablas.py    # Ejecutor automático del script DDL
│   │   └── cargar_datos.py    # Inserción incremental desde Pandas con SQLAlchemy
│   └── utils/                 # Módulos de autenticación y utilidades
│       ├── auth.py            # Gestión unificada de tokens OAuth 2.0
│       ├── auth_youtube.py    # Renovación de tokens de Google
│       └── auth_instagram.py  # Canje y validación de tokens de Meta
├── tests/                     # Pruebas automatizadas (pytest y unittest)
│   ├── test_cargar_datos.py
│   ├── test_normalizar.py
│   ├── test_exportar.py
│   └── prueba_cargar_json.py
├── tools/                     # Scripts de diagnóstico y utilidades de consola
│   ├── probar_conexion_db.py  # Diagnóstico de conexión a Azure y firewall
│   ├── estado_credenciales.py # Chequeo rápido de vigencia de tokens
│   ├── renovar_youtube.py     # Flujo OAuth de YouTube
│   ├── canjear_instagram.py   # Canje de token de 60 días para Instagram
│   └── verificar_credenciales.py
├── main.py                    # Orquestador integral del pipeline (End-to-End)
├── requirements.txt           # Dependencias del proyecto
└── .env.example               # Plantilla de variables de entorno
```

---

## ⚙️ Requisitos Previos

1. **Python 3.10 o 3.11** instalado.
2. **Microsoft ODBC Driver 18 for SQL Server**:
   - Requerido para la conexión cifrada TLS con Azure SQL Database.
   - [Descargar instalador oficial de Microsoft](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server).
3. **Reglas de Firewall en Azure**:
   - Tu IP pública debe estar autorizada en la sección *Redes / Firewall* del servidor SQL en Azure Portal.
   - El servidor de Azure SQL debe permitir autenticación mixta (SQL y Microsoft Entra).

---

## 🚀 Instalación y Configuración

1. **Clonar el repositorio:**
   ```bash
   git clone https://github.com/JoaquinTroncosoG/ETL-TT2.git
   cd ETL-TT2
   ```

2. **Crear y activar el entorno virtual:**
   - En Windows (PowerShell):
     ```powershell
     python -m venv venv
     .\venv\Scripts\Activate.ps1
     ```
   - En Linux / macOS:
     ```bash
     python3 -m venv venv
     source venv/bin/activate
     ```

3. **Instalar dependencias:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configurar variables de entorno (`.env`):**
   - Copia la plantilla `.env.example` y renómbrala a `.env`:
     ```bash
     cp .env.example .env
     ```
   - Abre `.env` y completa tus credenciales de Google, Meta y Azure SQL.
   > **Nota de seguridad:** El archivo `.env` está protegido en `.gitignore` y **nunca** debe subirse a Git. Si tu contraseña de Azure contiene `#`, colócala entre comillas dobles (ej: `AZURE_PASSWORD="MiPassword123#"`).

---

## 🛠️ Herramientas de Diagnóstico (`tools/`)

Antes de correr el pipeline, puedes verificar el estado de tus conexiones con los scripts de acceso rápido:

* **Probar conexión con Azure SQL y validar Firewall / Drivers:**
  ```bash
  python tools/probar_conexion_db.py
  ```
* **Revisar vigencia de tokens de API (Google y Meta):**
  ```bash
  python tools/estado_credenciales.py
  ```
* **Renovar acceso a YouTube (OAuth 2.0 con navegador):**
  ```bash
  python tools/renovar_youtube.py
  ```
* **Canjear token de Instagram por uno de larga duración (60 días):**
  ```bash
  python tools/canjear_instagram.py <TOKEN_CORTO_AQUI>
  ```

---

## 🔄 Ejecución del Pipeline ETL

### 1. Orquestador Central Unificado (`main.py`)

El script `main.py` es el punto de entrada principal para correr el pipeline de punta a punta (**Extracción $\rightarrow$ Transformación $\rightarrow$ Carga en Azure SQL**) en una sola instrucción:

```bash
python main.py
```

#### Comandos de Flexibilidad y Parámetros:

* **Pruebas rápidas (con límite de registros):**
  Extrae únicamente un número acotado de elementos por plataforma para validar el flujo completo sin consumir cuotas innecesarias:
  ```bash
  python main.py --limite 5
  ```

* **Ejecutar únicamente una fase específica:**
  Si ya descargaste los datos o si solo quieres probar una etapa aislada del pipeline:
  ```bash
  # Solo Fase 1: Descarga JSONs crudos en data/raw/
  python main.py --solo-extraer

  # Solo Fase 2: Limpieza, deduplicacion y exportacion en data/processed/
  python main.py --solo-transformar

  # Solo Fase 3: Persistencia incremental en Azure SQL Database
  python main.py --solo-cargar
  ```

* **Filtrar por una sola plataforma:**
  Puedes combinar la ejecución completa o modular enfocada en una única red social:
  ```bash
  python main.py --plataforma youtube
  python main.py --plataforma instagram
  python main.py --plataforma youtube --solo-extraer --limite 10
  ```

---

### 2. Ejecución Modular de Scripts Internos (Avanzado)

Si deseas interactuar directamente con los scripts especializados de cada fase:

#### Fase 1: Extracción
```bash
python src/extract/extract_youtube.py --limite 10
python src/extract/extract_instagram.py --limite 10
```

#### Fase 2: Transformación
Aplica tratamiento de nulos, deduplicación de registros, conversión de duraciones ISO a segundos, normalización horaria a UTC y exportación:
```bash
# Exportar en CSV y Excel (.xlsx) en data/processed/
python src/transform/transform_data.py --formatos csv xlsx

# Filtrar solo una plataforma
python src/transform/transform_data.py --solo youtube
python src/transform/transform_data.py --solo instagram
```

#### Fase 3: Carga al Data Warehouse (Azure SQL Database)
1. **Crear tablas e índices en Azure SQL (DDL):**
   *(Solo se ejecuta la primera vez o cuando se reconstruye el modelo dimensional)*
   ```bash
   python src/load/crear_tablas.py
   ```
   Crea `DIM_TIEMPO`, `DIM_PLATAFORMA`, `DIM_CUENTA`, `DIM_PUBLICACION`, `DIM_EJECUCION` y `HECHOS_METRICAS_PUBLICACION`.

2. **Cargar los datos transformados a la base de datos:**
   ```bash
   python src/load/cargar_datos.py
   ```
   - El proceso es **100% idempotente**: no duplica observaciones si se ejecuta múltiples veces.
   - Puebla automáticamente la dimensión de calendario `DIM_TIEMPO` según las fechas detectadas.

---

## 🧪 Pruebas Automatizadas

El proyecto cuenta con una suite de pruebas unitarias implementadas con `pytest`:
```bash
pytest
```
Para ver el detalle de cada prueba individual:
```bash
pytest -v
```
Las pruebas cubren validaciones de zonas horarias, cambio de hora oficial en Chile, tratamiento de nulos, exportaciones de archivos Excel y persistencia de claves foráneas dimensionales.
