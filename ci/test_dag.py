"""
Verifica que el DAG se importe en Airflow sin errores y tenga la forma esperada.
Corre en CI con Airflow 2.9.3 (la misma versión del servidor de la cátedra).
"""
import os

from airflow.models import DagBag

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_dag_importa_sin_errores():
    bag = DagBag(dag_folder=os.path.join(RAIZ, "dags"), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    assert len(bag.dags) == 1


def test_dag_forma_y_reintentos():
    bag = DagBag(dag_folder=os.path.join(RAIZ, "dags"), include_examples=False)
    dag = next(iter(bag.dags.values()))
    orden = ["preparar_entorno", "extraer_bronze", "dbt_build", "publicar_gold"]
    assert sorted(dag.task_ids) == sorted(orden)
    for a, b in zip(orden, orden[1:]):                      # cadena lineal, en ese orden
        assert dag.get_task(b).upstream_task_ids == {a}
    assert dag.max_active_runs == 1 and dag.catchup is False
    for t in dag.tasks:
        assert 1 <= t.retries <= 3, f"{t.task_id}: reintentos sin criterio ({t.retries})"


def test_extraccion_usa_el_intervalo_y_no_el_reloj():
    bag = DagBag(dag_folder=os.path.join(RAIZ, "dags"), include_examples=False)
    dag = next(iter(bag.dags.values()))
    cmd = dag.get_task("extraer_bronze").bash_command
    assert "data_interval_start" in cmd and "data_interval_end" in cmd
