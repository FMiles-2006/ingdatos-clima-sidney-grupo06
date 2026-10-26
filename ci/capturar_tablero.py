"""
Saca capturas del tablero de Metabase local (para docs/capturas/: Metabase no se versiona,
las capturas sí). Inicia sesión por API y le pasa la cookie de sesión al navegador.

Uso: python ci/capturar_tablero.py   (requiere playwright y el entorno local levantado)
"""
import os
import time

import requests
from playwright.sync_api import sync_playwright

MB = os.environ.get("MB_URL", "http://localhost:3000")
EMAIL = os.environ.get("MB_ADMIN_EMAIL", "admin@grupo06.local")
CLAVE = os.environ.get("MB_ADMIN_PASSWORD", "ClimaSidney-2026!")
SALIDA = os.environ.get("CAPTURAS_DIR", "docs/capturas")

sesion = requests.post(f"{MB}/api/session", json={"username": EMAIL, "password": CLAVE}, timeout=30).json()["id"]
h = {"X-Metabase-Session": sesion}
tablero = next(d for d in requests.get(f"{MB}/api/dashboard", headers=h, timeout=30).json()
               if d["name"] == "Clima de Sídney" and not d.get("archived"))
os.makedirs(SALIDA, exist_ok=True)

with sync_playwright() as p:
    nav = p.chromium.launch()
    ctx = nav.new_context(viewport={"width": 1600, "height": 1300}, device_scale_factor=1, locale="es-AR")
    ctx.add_cookies([{"name": "metabase.SESSION", "value": sesion, "url": MB}])
    pag = ctx.new_page()
    pag.goto(f"{MB}/dashboard/{tablero['id']}", wait_until="networkidle")
    time.sleep(8)                                      # que terminen de dibujarse los gráficos
    pag.screenshot(path=f"{SALIDA}/metabase_tablero.png", full_page=True)
    print("ok ->", f"{SALIDA}/metabase_tablero.png")
    nav.close()
