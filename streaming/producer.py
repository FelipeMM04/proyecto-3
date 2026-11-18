import os
import json
import time
import uuid
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")
POSTGRES_DB = os.getenv("POSTGRES_DB", "dw_turismo_ods8_v2")
POSTGRES_USER = os.getenv("POSTGRES_USER", "etl_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "etl_password")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "rnt_turismo_eventos")


def get_db_connection():
    """Establece conexión con el Data Warehouse en PostgreSQL."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        cursor_factory=RealDictCursor
    )


def fetch_relevant_rnt_records(limit=50):
    """
    Extrae registros operativos relevantes desde fact_turismo_anual.
    Filtra prestadores con actividad real (empleados > 0 o habitaciones > 0)
    para evitar publicar eventos vacíos o sin impacto analítico.
    """
    query = """
        SELECT 
            f.id_fact,
            f.codigo_rnt,
            u.codigo_departamento,
            u.departamento,
            u.municipio,
            c.categoria,
            c.sub_categoria,
            f.anio,
            f.numero_de_habitaciones,
            f.numero_de_camas,
            f.numero_de_empleados
        FROM fact_turismo_anual f
        JOIN dim_ubicacion u ON f.codigo_municipio = u.codigo_municipio
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        WHERE f.numero_de_empleados > 0 OR f.numero_de_habitaciones > 0
        ORDER BY f.id_fact ASC
        LIMIT %s;
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (limit,))
            return cur.fetchall()
    finally:
        conn.close()


def create_kafka_producer():
    """Inicializa el productor de Kafka con serialización JSON y llave en UTF-8."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all"
    )


def run_producer(interval_seconds=2.0, limit=50):
    print("=" * 70)
    print("INICIANDO KAFKA PRODUCER - SIMULACIÓN DE REPLAY HISTÓRICO RNT (ODS 8)")
    print(f"Broker: {KAFKA_BOOTSTRAP_SERVERS} | Tópico: {KAFKA_TOPIC}")
    print("=" * 70)

    records = fetch_relevant_rnt_records(limit=limit)
    if not records:
        print("[ADVERTENCIA] No se encontraron registros relevantes en fact_turismo_anual.")
        return

    producer = create_kafka_producer()
    print(f"[INFO] {len(records)} registros operativos listos para transmitir.\n")

    try:
        for idx, row in enumerate(records, start=1):
            # Construcción del evento cumpliendo estrictamente el Event Contract
            event_payload = {
                "event_id": str(uuid.uuid4()),
                "event_timestamp": datetime.now(timezone.utc).isoformat(),
                "event_key": str(row["codigo_departamento"]).zfill(2),
                "simulation_mode": "HISTORICAL_REPLAY_RNT",
                "codigo_rnt": int(row["codigo_rnt"]),
                "departamento": str(row["departamento"]).strip().upper(),
                "municipio": str(row["municipio"]).strip().upper(),
                "categoria": str(row["categoria"]).strip().upper(),
                "anio": int(row["anio"]),
                "numero_de_habitaciones": int(row["numero_de_habitaciones"]),
                "numero_de_camas": int(row["numero_de_camas"]),
                "numero_de_empleados": int(row["numero_de_empleados"])
            }

            event_key = event_payload["event_key"]

            # Publicar evento en Kafka
            future = producer.send(KAFKA_TOPIC, key=event_key, value=event_payload)
            record_metadata = future.get(timeout=10)

            print(
                f"[{idx}/{len(records)}] Evento enviado | ID: {event_payload['event_id'][:8]}... | "
                f"Key(Depto): {event_key} ({event_payload['departamento']}) | "
                f"RNT: {event_payload['codigo_rnt']} | Empleados: {event_payload['numero_de_empleados']} | "
                f"Partición: {record_metadata.partition} | Offset: {record_metadata.offset}"
            )

            time.sleep(interval_seconds)

        producer.flush()
        print("\n[OK] Transmisión de eventos finalizada correctamente.")

    except KeyboardInterrupt:
        print("\n[INFO] Producer detenido manualmente por el usuario.")
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer(interval_seconds=2.0, limit=50)