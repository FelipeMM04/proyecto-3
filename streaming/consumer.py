import os
import json
from datetime import datetime
import psycopg2
from kafka import KafkaConsumer
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
KAFKA_GROUP_ID = "rnt_streaming_metrics_group"

# Registro en memoria de event_ids procesados para evitar doble conteo en la sesión
processed_event_ids = set()


def get_db_connection():
    """Establece conexión con PostgreSQL."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def validate_event_contract(raw_value):
    """
    Valida el contrato de eventos (Event Contract).
    Retorna: (es_valido: bool, datos_o_error: dict | str)
    """
    # 1. Validar que sea JSON decodificable y tipo diccionario
    try:
        event = json.loads(raw_value.decode("utf-8"))
        if not isinstance(event, dict):
            return False, "El payload JSON no es un objeto válido."
    except Exception as e:
        return False, f"Error de formato JSON: {str(e)}"

    # 2. Validar presencia de campos obligatorios del contrato
    required_fields = {
        "event_id": str,
        "event_timestamp": str,
        "event_key": str,
        "codigo_rnt": int,
        "departamento": str,
        "categoria": str,
        "numero_de_habitaciones": int,
        "numero_de_camas": int,
        "numero_de_empleados": int
    }

    for field, expected_type in required_fields.items():
        if field not in event or event[field] is None:
            return False, f"Campo obligatorio faltante o nulo: '{field}'"
        # En Python bool es subclase de int, se excluye explícitamente
        if isinstance(event[field], bool) or not isinstance(event[field], expected_type):
            return False, (
                f"Tipo de dato inválido en '{field}': "
                f"se esperaba {expected_type.__name__}, llegó {type(event[field]).__name__}"
            )

    # 3. Validar formato ISO-8601 en event_timestamp
    try:
        datetime.fromisoformat(event["event_timestamp"].replace("Z", "+00:00"))
    except ValueError:
        return False, f"Formato de timestamp inválido (no es ISO-8601): '{event['event_timestamp']}'"

    # 4. Validar reglas de negocio básicas (cadenas no vacías y métricas >= 0)
    if not event["event_id"].strip() or not event["event_key"].strip() or not event["departamento"].strip():
        return False, "event_id, event_key o departamento no pueden estar vacíos."

    if event["codigo_rnt"] <= 0:
        return False, f"codigo_rnt inválido: {event['codigo_rnt']}"

    if (
        event["numero_de_empleados"] < 0
        or event["numero_de_habitaciones"] < 0
        or event["numero_de_camas"] < 0
    ):
        return False, "Métricas numéricas negativas no permitidas por el contrato."

    # 5. Validar deduplicación por event_id
    if event["event_id"] in processed_event_ids:
        return False, f"Evento duplicado detectado: event_id={event['event_id']}"

    return True, event


def log_invalid_event(conn, raw_payload_str, reason):
    """Persiste el evento inválido en la tabla de auditoría streaming_invalid_events."""
    query = """
        INSERT INTO streaming_invalid_events (raw_payload, motivo_rechazo)
        VALUES (%s, %s);
    """
    with conn.cursor() as cur:
        cur.execute(query, (raw_payload_str, reason))
    conn.commit()


def update_streaming_metrics(conn, event):
    """
    Actualiza de forma incremental las métricas de streaming en PostgreSQL:
    1. total_pst_procesados
    2. total_empleados_stream
    3. total_habitaciones_stream y total_camas_stream
    4. alertas_alto_impacto_laboral (PSTs con >= 20 empleados directos)
    """
    depto = event["departamento"].strip().upper()
    empleados = event["numero_de_empleados"]
    habitaciones = event["numero_de_habitaciones"]
    camas = event["numero_de_camas"]
    es_alto_impacto = 1 if empleados >= 20 else 0

    upsert_query = """
        INSERT INTO streaming_metrics (
            departamento,
            total_pst_procesados,
            total_empleados_stream,
            total_habitaciones_stream,
            total_camas_stream,
            alertas_alto_impacto_laboral,
            ultimo_event_id,
            ultima_actualizacion
        )
        VALUES (%s, 1, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (departamento) DO UPDATE SET
            total_pst_procesados = streaming_metrics.total_pst_procesados + 1,
            total_empleados_stream = streaming_metrics.total_empleados_stream + EXCLUDED.total_empleados_stream,
            total_habitaciones_stream = streaming_metrics.total_habitaciones_stream + EXCLUDED.total_habitaciones_stream,
            total_camas_stream = streaming_metrics.total_camas_stream + EXCLUDED.total_camas_stream,
            alertas_alto_impacto_laboral = streaming_metrics.alertas_alto_impacto_laboral + EXCLUDED.alertas_alto_impacto_laboral,
            ultimo_event_id = EXCLUDED.ultimo_event_id,
            ultima_actualizacion = CURRENT_TIMESTAMP
        RETURNING 
            total_pst_procesados, 
            total_empleados_stream, 
            total_camas_stream, 
            alertas_alto_impacto_laboral;
    """
    with conn.cursor() as cur:
        cur.execute(
            upsert_query,
            (depto, empleados, habitaciones, camas, es_alto_impacto, event["event_id"])
        )
        updated_row = cur.fetchone()
    conn.commit()
    return updated_row


def run_consumer():
    print("=" * 75)
    print("INICIANDO KAFKA CONSUMER - PROCESAMIENTO DE MÉTRICAS RNT EN TIEMPO REAL")
    print(f"Broker: {KAFKA_BOOTSTRAP_SERVERS} | Tópico: {KAFKA_TOPIC} | Group: {KAFKA_GROUP_ID}")
    print("Esperando eventos... (Presiona Ctrl + C para detener)")
    print("=" * 75)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )

    conn = get_db_connection()

    try:
        for message in consumer:
            raw_bytes = message.value
            is_valid, result = validate_event_contract(raw_bytes)

            if not is_valid:
                raw_str = raw_bytes.decode("utf-8", errors="replace")
                print(
                    f"\n[RECHAZADO - CONTRATO INVÁLIDO] Offset: {message.offset} | "
                    f"Motivo: {result}"
                )
                log_invalid_event(conn, raw_str, result)
                # Confirmamos el offset para no quedar atrapados en un mensaje corrupto
                consumer.commit()
                continue

            event = result
            updated = update_streaming_metrics(conn, event)
            processed_event_ids.add(event["event_id"])

            # Confirmación explícita de offset solo después de persistir en PostgreSQL
            consumer.commit()

            print(
                f"[OK] Offset: {message.offset} | Depto: {event['departamento']} | "
                f"RNT: {event['codigo_rnt']} (+{event['numero_de_empleados']} emp) -> "
                f"Acumulado Depto [PSTs: {updated[0]}, Empleados: {updated[1]}, "
                f"Camas: {updated[2]}, Alertas >=20 emp: {updated[3]}]"
            )

    except KeyboardInterrupt:
        print("\n[INFO] Consumer detenido temporalmente por el usuario.")
    finally:
        consumer.close()
        conn.close()
        print("[INFO] Conexiones de Kafka y PostgreSQL cerradas correctamente.")


if __name__ == "__main__":
    run_consumer()