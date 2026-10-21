"""
Genera docs/capturas/linaje_dbt.png a partir del manifest.json que produce dbt
(`dbt docs generate` o `dbt build`). El diagrama sale de los metadatos de dbt: si el
proyecto cambia, se regenera; nunca se dibuja a mano.

Uso:  python docs/generar_linaje.py [dbt/target/manifest.json]      (requiere graphviz: dot)
"""
import json
import subprocess
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
manifest = Path(sys.argv[1]) if len(sys.argv) > 1 else RAIZ / "dbt" / "target" / "manifest.json"
m = json.loads(manifest.read_text())
padres = {k: v for k, v in m["parent_map"].items() if k.startswith(("model.", "seed.", "source."))}
n_tests = sum(1 for k in m["parent_map"] if k.startswith("test."))

ESTILO = {"source": ("#2e7d32", "bronze · Delta"), "seed": ("#6a1b9a", "seed"),
          "silver": ("#1565c0", "staging · silver"), "gold": ("#ef6c00", "marts · gold")}


def tipo(nodo):
    if nodo.startswith("source."):
        return "source"
    if nodo.startswith("seed."):
        return "seed"
    capa = m["nodes"][nodo]["fqn"][1] if nodo in m["nodes"] else ""
    return "gold" if capa in ("marts", "gold") else "silver"


def etiqueta(nodo):
    partes = nodo.split(".")
    return ".".join(partes[2:]) if nodo.startswith("source.") else partes[-1]


lineas = ["digraph linaje {", "rankdir=LR; bgcolor=white; pad=0.3; nodesep=0.5; ranksep=0.9;",
          'node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=13, fontcolor=white, penwidth=0];',
          'edge [color="#78909c", arrowsize=0.8];',
          f'labelloc=t; fontname="Helvetica"; fontsize=15; label="Linaje dbt · proyecto clima_sidney · {n_tests} tests";']
for nodo in padres:
    color, rol = ESTILO[tipo(nodo)]
    lineas.append(f'"{nodo}" [label="{etiqueta(nodo)}\\n({rol})", fillcolor="{color}"];')
for nodo, ps in padres.items():
    for p in ps:
        if p in padres:
            lineas.append(f'"{p}" -> "{nodo}";')
lineas.append("}")
salida = RAIZ / "docs" / "capturas" / "linaje_dbt.png"
salida.parent.mkdir(parents=True, exist_ok=True)
subprocess.run(["dot", "-Tpng", "-Gdpi=130", "-o", str(salida)], input="\n".join(lineas), text=True, check=True)
print("ok ->", salida)
