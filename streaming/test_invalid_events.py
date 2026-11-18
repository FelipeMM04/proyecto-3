import os
import json
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "rnt_turismo_eventos")


def run_invalid_events_test():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8")
    )

    print("=" * 70)
    print("PRUEBA DE RESILIENCIA A: INYECCIÓN DE EVENTOS INVÁLIDOS AL TÓPICO")
    print("=" * 70)

    # Caso 1: JSON malformado (texto corrupto)
    print("[1/5] Enviando payload corrupto (no es JSON válido)...")
    producer.send(KAFKA_TOPIC, key="99", value=b"{corrupted_json_payload_sin_cierre")
    time.sleep(1.5)

    # Caso 2: Campo obligatorio faltante (falta 'event_id' y 'departamento')
    print("[2/5] Enviando evento sin campos obligatorios ('event_id', 'departamento')...")
    producer.send(KAFKA_TOPIC, key="76", value={
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_key": "76",
        "codigo_rnt": 99901,
        "categoria": "HOTEL",
        "numero_de_habitaciones": 10,
        "numero_de_camas": 20,
        "numero_de_empleados": 5
    })
    time.sleep(1.5)

    # Caso 3: Tipo de dato incorrecto ('numero_de_empleados' llega como texto en vez de int)
    print("[3/5] Enviando evento con tipo de dato erróneo ('numero_de_empleados' como string)...")
    producer.send(KAFKA_TOPIC, key="76", value={
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_key": "76",
        "codigo_rnt": 99902,
        "departamento": "VALLE DEL CAUCA",
        "categoria": "HOTEL",
        "numero_de_habitaciones": 10,
        "numero_de_camas": 20,
        "numero_de_empleados": "CINCUENTA"
    })
    time.sleep(1.5)

    # Caso 4: Regla de negocio violada (empleados negativos)
    print("[4/5] Enviando evento con métricas negativas ('numero_de_empleados': -15)...")
    producer.send(KAFKA_TOPIC, key="76", value={
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_key": "76",
        "codigo_rnt": 99903,
        "departamento": "VALLE DEL CAUCA",
        "categoria": "HOTEL",
        "numero_de_habitaciones": 10,
        "numero_de_camas": 20,
        "numero_de_empleados": -15
    })
    time.sleep(1.5)

    # Caso 5: Evento válido de control (demuestra que el Consumer sigue operando tras los errores)
    print("[5/5] Enviando evento VÁLIDO de control para confirmar continuidad...")
    producer.send(KAFKA_TOPIC, key="76", value={
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_key": "76",
        "simulation_mode": "CONTROL_VALID_EVENT",
        "codigo_rnt": 99904,
        "departamento": "VALLE DEL CAUCA",
        "municipio": "CALI",
        "categoria": "ESTABLECIMIENTOS DE ALOJAMIENTO Y HOSPEDAJE",
        "anio": 2026,
        "numero_de_habitaciones": 10,
        "numero_de_camas": 20,
        "numero_de_empleados": 5
    })

    producer.flush()
    producer.close()
    print("\n[OK] Prueba de inyección finalizada.")


if __name__ == "__main__":
    run_invalid_events_test()