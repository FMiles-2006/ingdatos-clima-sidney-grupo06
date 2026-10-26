-- Se ejecuta solo en el primer arranque del Postgres local (volumen vacío).
-- Imita al servidor de la cátedra: base "warehouse" con un schema por grupo.
CREATE SCHEMA IF NOT EXISTS grupo06;
-- Base aparte para la metadata interna de Metabase (preguntas, tableros, usuarios),
-- así el tablero sobrevive a reinicios del contenedor.
CREATE DATABASE metabase_app;
