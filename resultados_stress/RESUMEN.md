# Stress test – Fase 5

Datos sinteticos (`tools/generar_sinteticos.py`), mitad YouTube y mitad Instagram, pasados por las mismas funciones de la carga real (`tools/stress_test.py`). Ejecutado el 2026-11-09 desde Chile contra Azure SQL serverless (Central US), con ODBC Driver 18 y `fast_executemany` activo.

| Publicaciones | Transformacion | Carga a Azure | Memoria pico transf. |
|---:|---:|---:|---:|
| 100 | 0,09 s | 16,96 s (3 rep.) | 0,5 MB |
| 1.000 | 0,18 s | 17,29 s (3 rep.) | 3,4 MB |
| 10.000 | 0,61 s | 20,68 s (1 rep.) | 32,2 MB |
| 100.000 | 5,05 s | no medida | 321,6 MB |

Graficos: `tiempo_vs_volumen.png`, `registros_por_segundo.png`, `memoria_vs_volumen.png`. Detalle por repeticion en `resultados_stress_detalle.csv`.

## Cuello de botella

La **carga a Azure**. Con 10.000 publicaciones tarda 34 veces mas que la transformacion.

Su tiempo se comporta como `~17 s fijos + ~0,37 ms por publicacion`: pasar de 100 a 10.000 registros solo suma 3,7 s. El costo fijo no depende del volumen sino de la cantidad de viajes de ida y vuelta a la base. Por cada plataforma la carga lee tablas completas para saber que existe, inserta con `to_sql` (que ademas consulta la estructura de la tabla) y vuelve a leer la tabla para obtener las claves generadas.

## Escalamiento

- **Transformacion:** crece de forma aproximadamente lineal desde los 10.000 registros (unos 20.000 registros/s con 100.000). A volumen alto domina la lectura del JSON (3,9 de 5,0 s). La memoria crece linealmente, unos 3,2 KB por publicacion.
- **Carga:** casi plana entre 100 y 10.000 por el costo fijo. Extrapolando, 100.000 publicaciones tomarian cerca de 1 minuto, aunque las lecturas de tablas completas y el uso de `iterrows` crecen con el tamano de la base, asi que la cifra real podria ser mayor.

## Que se podria optimizar

1. **Reducir viajes a la base** (mayor impacto): hacer toda la carga en una sola conexion y transaccion, y leer solo los IDs del lote (`WHERE id IN ...`) en vez de tablas completas.
2. **Carga por lotes con tabla de staging + `MERGE`**: insertar el lote en una tabla temporal y resolver duplicados y claves en el servidor en una sola sentencia.
3. **`to_sql` con `chunksize`** (por ejemplo 5.000) para volumenes grandes y no armar un solo envio gigante.
4. **Reemplazar `iterrows`** en `cargar_dim_publicacion` y en los hechos por operaciones vectorizadas de Pandas (`merge`/`map`).
5. **Lectura del JSON:** usar `orjson` o procesar archivo por archivo para bajar tiempo y memoria a gran escala.

`fast_executemany` ya esta activado, por lo que no explica la lentitud.

## Limitaciones

- Datos sinteticos: las cuentas reales tienen 0 publicaciones.
- Para cuidar el computo gratuito de Azure, la carga de 10.000 se midio una vez y la de 100.000 no se ejecuto.
- El tiempo de carga incluye la latencia de red Chile – Central US.
- `tracemalloc` mide solo memoria reservada por Python, no la del proceso completo.
