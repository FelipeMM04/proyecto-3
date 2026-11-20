import os
import json
import time
import uuid
import random
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")
POSTGRES_DB = os.getenv("POSTGRES_DB", "dw_turismo_ods8_v2")
POSTGRES_USER = os.getenv("POSTGRES_USER", "etl_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "etl_password")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "rnt_turismo_eventos")


def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        cursor_factory=RealDictCursor,
    )


def fetch_relevant_rnt_records(limit=30):
    query = """
        SELECT 
            f.id_fact,
            f.codigo_rnt,
            u.codigo_departamento,
            u.departamento,
            u.municipio,
            c.categoria,
            f.anio,
            f.numero_de_habitaciones,
            f.numero_de_camas,
            f.numero_de_empleados
        FROM fact_turismo_anual f
        JOIN dim_ubicacion u ON f.codigo_municipio = u.codigo_municipio
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        WHERE f.numero_de_empleados > 0 OR f.numero_de_habitaciones > 0
        ORDER BY RANDOM()
        LIMIT %s;
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()
        conn.close()
        return rows
    except Exception:
        return []


def generate_synthetic_rnt_records(limit=30):
    """Genera eventos válidos realistas si fact_turismo_anual aún no tiene carga batch."""
    catalogo = [
        ("05", "ANTIOQUIA", "MEDELLIN", "ESTABLECIMIENTOS DE ALOJAMIENTO TURISTICO"),
        ("11", "BOGOTA, D.C.", "BOGOTA, D.C.", "AGENCIAS DE VIAJES"),
        ("13", "BOLIVAR", "CARTAGENA DE INDIAS", "ESTABLECIMIENTOS DE ALOJAMIENTO TURISTICO"),
        ("76", "VALLE DEL CAUCA", "CALI", "ESTABLECIMIENTOS DE GASTRONOMIA Y SIMILARES"),
        ("47", "MAGDALENA", "SANTA MARTA", "ESTABLECIMIENTOS DE ALOJAMIENTO TURISTICO"),
        ("63", "QUINDIO", "SALENTO", "VIVIENDAS TURISTICAS"),
        ("68", "SANTANDER", "BARICHARA", "OPERADORES PROFESIONALES DE CONGRESOS"),
        ("08", "ATLANTICO", "BARRANQUILLA", "EMPRESAS DE TRANSPORTE TERRESTRE AUTOMOTOR"),
    ]
    records = []
    for _ in range(limit):
        cod_depto, depto, muni, cat = random.choice(catalogo)
        emp = random.choice([4, 8, 12, 18, 25, 34, 48, 65])
        hab = random.randint(5, 45) if "ALOJAMIENTO" in cat or "VIVIENDAS" in cat else 0
        records.append({
            "codigo_rnt": random.randint(10000, 99999),
            "codigo_departamento": cod_depto,
            "departamento": depto,
            "municipio": muni,
            "categoria": cat,
            "anio": 2026,
            "numero_de_habitaciones": hab,
            "numero_de_camas": hab * 2,
            "numero_de_empleados": emp,
        })
    return records


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
    )


def run_producer(interval_seconds=1.5, limit=30):
    print("=" * 70)
    print("INICIANDO KAFKA PRODUCER - FLUJO EN TIEMPO REAL RNT (ODS 8)")
    print(f"Broker: {KAFKA_BOOTSTRAP_SERVERS} | Tópico: {KAFKA_TOPIC}")
    print("=" * 70)

    records = fetch_relevant_rnt_records(limit=limit)
    modo = "HISTORICAL_REPLAY_RNT"
    if not records:
        records = generate_synthetic_rnt_records(limit=limit)
        modo = "LIVE_SIMULATION_RNT"

    producer = create_kafka_producer()
    print(f"[INFO] Modo: {modo} | {len(records)} eventos válidos listos (1 cada {interval_seconds}s).\n")

    try:
        for idx, row in enumerate(records, start=1):
            event_payload = {
                "event_id": str(uuid.uuid4()),
                "event_timestamp": datetime.now(timezone.utc).isoformat(),
                "event_key": str(row["codigo_departamento"]).zfill(2),
                "simulation_mode": modo,
                "codigo_rnt": int(row["codigo_rnt"]),
                "departamento": str(row["departamento"]).strip().upper(),
                "municipio": str(row["municipio"]).strip().upper(),
                "categoria": str(row["categoria"]).strip().upper(),
                "anio": int(row["anio"]),
                "numero_de_habitaciones": int(row["numero_de_habitaciones"]),
                "numero_de_camas": int(row["numero_de_camas"]),
                "numero_de_empleados": int(row["numero_de_empleados"]),
            }

            future = producer.send(KAFKA_TOPIC, key=event_payload["event_key"], value=event_payload)
            meta = future.get(timeout=10)

            print(
                f"[{idx}/{len(records)}] Enviado | {event_payload['departamento']} | "
                f"RNT: {event_payload['codigo_rnt']} | +{event_payload['numero_de_empleados']} emp | "
                f"Offset: {meta.offset}"
            )
            time.sleep(interval_seconds)

        producer.flush()
        print("\n[OK] Transmisión finalizada.")
    except KeyboardInterrupt:
        print("\n[INFO] Producer detenido manualmente.")
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer(interval_seconds=1.5, limit=30)