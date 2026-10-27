#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pruebas del extractor de Instagram: paginacion, manejo de errores y cuotas.
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)

No se conecta a la Graph API ni consume cuota: reemplaza la sesion HTTP por
una sesion falsa que devuelve respuestas preparadas. Asi se pueden provocar a
voluntad situaciones que en la API real son dificiles de reproducir (cuota
agotada, token vencido, metrica rechazada) y comprobar que el extractor
reacciona como corresponde.

USO
---
    python tests/prueba_extraccion_instagram.py

Codigos de salida: 0 todas las pruebas pasaron, 1 alguna fallo.
"""

import importlib.util
import io
import json
import logging
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
MODULO = RAIZ / "src" / "extract" / "extract_instagram.py"

_spec = importlib.util.spec_from_file_location("extract_instagram", MODULO)
ig = importlib.util.module_from_spec(_spec)
sys.modules["extract_instagram"] = ig
_spec.loader.exec_module(ig)

TOKEN_FALSO = "EAAtoken-de-prueba-que-no-debe-aparecer-nunca"


# --------------------------------------------------------------------------
# Dobles de prueba: reemplazan a requests sin tocar la red
# --------------------------------------------------------------------------

class RespuestaFalsa:
    """Imita lo que el extractor usa de una respuesta de requests."""

    def __init__(self, cuerpo, estado=200, cabeceras=None):
        self.status_code = estado
        self.headers = cabeceras or {}
        self._cuerpo = cuerpo
        self.text = json.dumps(cuerpo)

    @property
    def ok(self):
        return self.status_code < 400

    def json(self):
        return self._cuerpo


class SesionFalsa:
    """Devuelve, en orden, las respuestas del guion y anota cada peticion."""

    def __init__(self, guion):
        self.guion = list(guion)
        self.peticiones = []

    def get(self, url, params=None, timeout=None):
        self.peticiones.append({"url": url, "params": dict(params or {})})
        if not self.guion:
            raise AssertionError(f"La prueba pidio mas respuestas de las previstas: {url}")
        return self.guion.pop(0)


def error(codigo, mensaje, estado=400, subcodigo=None):
    cuerpo = {"error": {"code": codigo, "message": mensaje, "fbtrace_id": "TRAZA_FALSA"}}
    if subcodigo is not None:
        cuerpo["error"]["error_subcode"] = subcodigo
    return RespuestaFalsa(cuerpo, estado)


def uso_de_cuota(porcentaje, minutos=0):
    """Cabecera X-Business-Use-Case-Usage como la manda Meta."""
    return {"X-Business-Use-Case-Usage": json.dumps({
        "17841422722303323": [{
            "call_count": porcentaje,
            "total_cputime": 0,
            "total_time": 0,
            "estimated_time_to_regain_access": minutos,
        }]
    })}


def cliente_con(guion):
    """ClienteGraph con la sesion reemplazada y sin esperas reales."""
    cliente = ig.ClienteGraph(TOKEN_FALSO)
    cliente._sesion = SesionFalsa(guion)
    return cliente


# Las esperas por backoff se anotan en vez de dormirse: la prueba corre en
# segundos en lugar de en horas, pero se comprueba cuanto habria esperado.
ESPERAS = []
ig.time.sleep = lambda segundos: ESPERAS.append(segundos)


# --------------------------------------------------------------------------
# Pruebas
# --------------------------------------------------------------------------

def prueba_paginacion_recorre_todas_las_paginas():
    """La extraccion sigue paging.next hasta que la API deja de enviarlo."""
    def pagina(ids, siguiente=None):
        cuerpo = {"data": [{"id": i, "media_type": "IMAGE"} for i in ids]}
        if siguiente:
            cuerpo["paging"] = {
                "next": f"https://graph.facebook.com/v21.0/IG/media"
                        f"?after={siguiente}&access_token={TOKEN_FALSO}"
            }
        return RespuestaFalsa(cuerpo)

    cliente = cliente_con([
        pagina(["m1", "m2"], siguiente="CURSOR_A"),
        pagina(["m3", "m4"], siguiente="CURSOR_B"),
        pagina(["m5"]),                                 # ultima: sin paging.next
    ])

    paginas = []
    publicaciones = ig.extraer_publicaciones(cliente, "IG", limite=None, paginas=paginas)

    assert len(publicaciones) == 5, f"se esperaban 5 publicaciones, llegaron {len(publicaciones)}"
    assert len(paginas) == 3, f"se esperaban 3 paginas, se guardaron {len(paginas)}"
    assert [p["id"] for p in publicaciones] == ["m1", "m2", "m3", "m4", "m5"]

    # El cursor de la segunda peticion es el que vino en paging.next de la primera
    assert cliente._sesion.peticiones[1]["params"].get("after") == "CURSOR_A"

    # El token viaja aparte y nunca queda escrito en el JSON guardado
    guardado = json.dumps(paginas)
    assert TOKEN_FALSO not in guardado, "el token quedo escrito en la pagina guardada"
    assert "access_token=***" in guardado, "la URL de paginacion no quedo enmascarada"
    return "3 paginas recorridas, 5 publicaciones, token enmascarado"


def prueba_cuenta_vacia_no_falla():
    """Una cuenta sin publicaciones no debe caerse: devuelve 0 y termina."""
    cliente = cliente_con([RespuestaFalsa({"data": []})])

    paginas = []
    publicaciones = ig.extraer_publicaciones(cliente, "IG", limite=None, paginas=paginas)

    assert publicaciones == [], "devolvio publicaciones de una cuenta vacia"
    assert len(paginas) == 1, "no dejo constancia de la pagina vacia"
    assert len(cliente._sesion.peticiones) == 1, "siguio pidiendo paginas sin datos"

    # La etapa de insights tampoco debe fallar con la lista vacia
    insights = {}
    ig.extraer_insights(cliente, publicaciones, insights, {})
    assert insights == {}
    return "0 publicaciones, 1 llamada, sin excepciones"


def prueba_limite_corta_la_paginacion():
    """Con --limite N no se piden mas paginas de las necesarias."""
    cliente = cliente_con([
        RespuestaFalsa({
            "data": [{"id": f"m{i}"} for i in range(3)],
            "paging": {"next": "https://graph.facebook.com/v21.0/IG/media?after=X"},
        }),
    ])
    paginas = []
    publicaciones = ig.extraer_publicaciones(cliente, "IG", limite=3, paginas=paginas)

    assert len(publicaciones) == 3
    assert len(cliente._sesion.peticiones) == 1, "se pidio una pagina de mas"
    return "el limite detuvo la paginacion tras 1 peticion"


def prueba_cuota_agotada_reintenta_con_backoff():
    """Ante el codigo 4 el extractor espera y reintenta, no se cae."""
    ESPERAS.clear()
    cliente = cliente_con([
        error(4, "Application request limit reached", estado=429),
        RespuestaFalsa({"id": "IG", "username": "leoricdeastora"}),
    ])

    datos = cliente.get("IG", {"fields": "id,username"})

    assert datos["username"] == "leoricdeastora"
    assert len(cliente._sesion.peticiones) == 2, "no reintento la peticion"
    assert len(ESPERAS) == 1, "no espero antes de reintentar"
    assert ESPERAS[0] >= ig.ESPERA_BASE_CUOTA, \
        f"la espera fue de {ESPERAS[0]:.0f}s, se esperaba al menos {ig.ESPERA_BASE_CUOTA}s"
    return f"reintento tras esperar {ESPERAS[0]:.0f} s y recupero el dato"


def prueba_backoff_es_exponencial():
    """Cada reintento espera aproximadamente el doble que el anterior."""
    ESPERAS.clear()
    cliente = cliente_con([error(4, "Application request limit reached", estado=429)
                           for _ in range(ig.MAX_REINTENTOS_CUOTA + 1)])

    try:
        cliente.get("IG")
    except ig.CuotaAgotada:
        pass
    else:
        raise AssertionError("debio rendirse con CuotaAgotada tras agotar los reintentos")

    assert len(ESPERAS) == ig.MAX_REINTENTOS_CUOTA, \
        f"se esperaban {ig.MAX_REINTENTOS_CUOTA} reintentos, hubo {len(ESPERAS)}"
    for anterior, siguiente in zip(ESPERAS, ESPERAS[1:]):
        assert siguiente > anterior, "la espera no crecio entre reintentos"
    return "esperas " + " -> ".join(f"{s:.0f}s" for s in ESPERAS)


def prueba_cuota_respeta_el_tiempo_que_informa_meta():
    """Si Meta dice cuanto falta para recuperar la cuota, se respeta ese plazo."""
    ESPERAS.clear()
    cliente = cliente_con([
        RespuestaFalsa({"error": {"code": 4, "message": "limite"}}, 429,
                       uso_de_cuota(100, minutos=45)),
        RespuestaFalsa({"id": "IG"}),
    ])
    cliente.get("IG")

    assert ESPERAS[0] >= 45 * 60, \
        f"espero {ESPERAS[0]:.0f}s en vez de los 45 min que informo Meta"
    return f"espero los {ESPERAS[0] / 60:.0f} min informados por Meta"


def prueba_token_vencido_no_reintenta():
    """El codigo 190 no se reintenta: el token no se arregla solo."""
    ESPERAS.clear()
    cliente = cliente_con([error(190, "Error validating access token: Session has expired")])

    try:
        cliente.get("IG")
    except ig.TokenInvalido as exc:
        assert "190" in str(exc), "el mensaje no orienta sobre como renovar el token"
    else:
        raise AssertionError("debio lanzar TokenInvalido")

    assert len(cliente._sesion.peticiones) == 1, "reintento un error que no es recuperable"
    assert not ESPERAS, "espero inutilmente ante un token vencido"
    return "corto a la primera, sin reintentos ni esperas"


def prueba_error_de_servidor_reintenta():
    """Un 500 es culpa de Meta: se reintenta."""
    ESPERAS.clear()
    cliente = cliente_con([
        RespuestaFalsa({"error": {"code": 2, "message": "An unexpected error"}}, 500),
        RespuestaFalsa({"id": "IG"}),
    ])
    assert cliente.get("IG")["id"] == "IG"
    assert len(cliente._sesion.peticiones) == 2
    return f"reintento el HTTP 500 tras {ESPERAS[0]:.0f} s"


def prueba_metrica_rechazada_se_descarta_y_reintenta():
    """Si la API rechaza una metrica, se quita y se piden las demas."""
    media = {"id": "m1", "media_type": "IMAGE", "media_product_type": "FEED"}
    solicitadas = ig.metricas_para(media)          # reach, saved, likes, comments, shares
    assert solicitadas[2] == "likes"

    cliente = cliente_con([
        error(100, "(#100) metric[2] must be one of the following values: "
                   "reach, saved, comments, shares"),
        RespuestaFalsa({"data": [{"name": "reach", "values": [{"value": 120}]}]}),
    ])

    descartadas = {}
    resultado = ig.insights_de_publicacion(cliente, media, descartadas)

    assert resultado["error"] is None, "la publicacion se quedo sin metricas"
    assert "likes" not in resultado["metricas_solicitadas"]
    assert resultado["metricas_descartadas"] == ["likes"]
    enviadas = cliente._sesion.peticiones[1]["params"]["metric"]
    assert "likes" not in enviadas, f"volvio a pedir la metrica rechazada: {enviadas}"
    return f"descarto 'likes' y reintento con: {enviadas}"


def prueba_metrica_rechazada_se_recuerda_por_tipo():
    """La metrica rechazada no se vuelve a pedir para el mismo tipo de publicacion."""
    descartadas = {}
    media1 = {"id": "m1", "media_type": "IMAGE", "media_product_type": "FEED"}
    media2 = {"id": "m2", "media_type": "IMAGE", "media_product_type": "FEED"}

    cliente = cliente_con([
        error(100, "(#100) metric[2] must be one of the following values: reach, saved"),
        RespuestaFalsa({"data": []}),
        RespuestaFalsa({"data": []}),
    ])

    ig.insights_de_publicacion(cliente, media1, descartadas)
    ig.insights_de_publicacion(cliente, media2, descartadas)

    # 2 peticiones para la primera (fallida + reintento) y 1 sola para la segunda
    assert len(cliente._sesion.peticiones) == 3, \
        f"se hicieron {len(cliente._sesion.peticiones)} peticiones en vez de 3"
    assert "likes" not in cliente._sesion.peticiones[2]["params"]["metric"], \
        "repitio el error en la segunda publicacion"
    return "la segunda publicacion ya no pidio la metrica rechazada: 3 llamadas en vez de 4"


def prueba_publicacion_sin_insights_no_detiene_la_extraccion():
    """Un error propio de una publicacion se registra y la extraccion sigue."""
    media = {"id": "m1", "media_type": "IMAGE", "media_product_type": "FEED"}
    cliente = cliente_con([
        error(100, "Unsupported get request. Object with ID 'm1' does not exist",
              subcodigo=33),
    ])
    resultado = ig.insights_de_publicacion(cliente, media, {})

    assert resultado["error"] is not None, "no dejo constancia del error"
    assert resultado["error"]["codigo"] == 100
    return "el error quedo registrado en el JSON y la extraccion continuo"


def prueba_uso_de_cuota_queda_registrado():
    """La cabecera de uso de Meta se guarda para auditar el consumo."""
    cliente = cliente_con([
        RespuestaFalsa({"id": "IG"}, 200, uso_de_cuota(32)),
        RespuestaFalsa({"id": "IG"}, 200, uso_de_cuota(64)),
    ])
    cliente.get("IG")
    cliente.get("IG")

    assert len(cliente.historial_uso) == 2, "no se registro el uso de cuota"
    valores = [r["valor"]["17841422722303323"][0]["call_count"] for r in cliente.historial_uso]
    assert valores == [32, 64]
    assert cliente.llamadas == 2
    return f"uso registrado en 2 llamadas: {valores[0]}% -> {valores[1]}%"


def prueba_aviso_al_superar_el_umbral_de_cuota():
    """Se avisa al pasar el umbral, pero no en cada llamada."""
    registro = io.StringIO()
    manejador = logging.StreamHandler(registro)
    manejador.addFilter(ig.FiltroSecretos())
    ig.log.addHandler(manejador)
    ig.log.setLevel(logging.INFO)
    try:
        cliente = cliente_con([RespuestaFalsa({"id": "IG"}, 200, uso_de_cuota(p))
                               for p in (50, 82, 83, 90)])
        for _ in range(4):
            cliente.get("IG")
    finally:
        ig.log.removeHandler(manejador)

    avisos = [l for l in registro.getvalue().splitlines() if "Uso de cuota" in l]
    assert len(avisos) == 2, f"se esperaban 2 avisos (82% y 90%), hubo {len(avisos)}: {avisos}"
    return "aviso al 82% y al 90%; el 50% y el 83% no generaron ruido"


def prueba_el_token_nunca_aparece_en_los_logs():
    """El filtro de logging enmascara el token aunque venga dentro de un error."""
    ig._SECRETOS.clear()
    ig._SECRETOS.append(TOKEN_FALSO)

    registro = io.StringIO()
    manejador = logging.StreamHandler(registro)
    manejador.addFilter(ig.FiltroSecretos())
    ig.log.addHandler(manejador)
    ig.log.setLevel(logging.INFO)
    try:
        ig.log.warning("Fallo la URL https://graph.facebook.com/IG?access_token=%s", TOKEN_FALSO)
    finally:
        ig.log.removeHandler(manejador)

    salida = registro.getvalue()
    assert TOKEN_FALSO not in salida, "EL TOKEN QUEDO ESCRITO EN EL LOG"
    assert "***" in salida
    return "el token se reemplazo por *** en el log"


# --------------------------------------------------------------------------
# Ejecucion
# --------------------------------------------------------------------------

GRUPOS = [
    ("Paginacion", [
        prueba_paginacion_recorre_todas_las_paginas,
        prueba_limite_corta_la_paginacion,
        prueba_cuenta_vacia_no_falla,
    ]),
    ("Manejo de errores", [
        prueba_token_vencido_no_reintenta,
        prueba_error_de_servidor_reintenta,
        prueba_metrica_rechazada_se_descarta_y_reintenta,
        prueba_metrica_rechazada_se_recuerda_por_tipo,
        prueba_publicacion_sin_insights_no_detiene_la_extraccion,
    ]),
    ("Limite de cuotas", [
        prueba_cuota_agotada_reintenta_con_backoff,
        prueba_backoff_es_exponencial,
        prueba_cuota_respeta_el_tiempo_que_informa_meta,
        prueba_uso_de_cuota_queda_registrado,
        prueba_aviso_al_superar_el_umbral_de_cuota,
    ]),
    ("Proteccion del token", [
        prueba_el_token_nunca_aparece_en_los_logs,
    ]),
]


def main():
    # Mismo formato de log que en produccion: los avisos de reintento y de
    # cuota que aparecen entre las pruebas son los reales del extractor.
    ig.configurar_logging()
    print()
    print("=" * 78)
    print("  PRUEBAS DEL EXTRACTOR DE INSTAGRAM  (sin conexion: no consume cuota)")
    print("=" * 78)

    total = fallidas = 0
    for titulo, pruebas in GRUPOS:
        print(f"\n{titulo}")
        print("-" * 78)
        for prueba in pruebas:
            total += 1
            nombre = prueba.__name__.replace("prueba_", "").replace("_", " ").capitalize()
            try:
                detalle = prueba()
                print(f"  [OK]    {nombre}")
                if detalle:
                    print(f"          -> {detalle}")
            except AssertionError as exc:
                fallidas += 1
                print(f"  [FALLO] {nombre}")
                print(f"          -> {exc}")
            except Exception as exc:                      # noqa: BLE001
                fallidas += 1
                print(f"  [ERROR] {nombre}")
                print(f"          -> {type(exc).__name__}: {exc}")

    print()
    print("=" * 78)
    if fallidas:
        print(f"  RESULTADO: {total - fallidas}/{total} pruebas pasaron, {fallidas} fallaron")
    else:
        print(f"  RESULTADO: {total}/{total} pruebas pasaron")
    print("=" * 78)
    print()
    return 1 if fallidas else 0


if __name__ == "__main__":
    sys.exit(main())
