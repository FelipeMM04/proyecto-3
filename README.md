### 2.2 Data Sources

Adicional a la ingesta batch histórica del Registro Nacional de Turismo (RNT) de Colombia, el sistema incorpora un flujo de eventos en tiempo real orientado al monitoreo continuo del **ODS 8 (Meta 8.9: Turismo sostenible y creación de empleo local)**.

* **Generación de eventos:** Los eventos se generan a partir de registros operativos verificados en la tabla de hechos del Data Warehouse (`fact_turismo_anual`), filtrando exclusivamente prestadores de servicios turísticos (PST) con actividad económica y laboral efectiva (`numero_de_empleados > 0` o `numero_de_habitaciones > 0`). Dado que el dataset abierto del RNT es de actualización periódica, el productor opera mediante una **simulación controlada de replay histórico** (`simulation_mode: "HISTORICAL_REPLAY_RNT"`), emitiendo actualizaciones individuales cada 2 segundos para emular la llegada continua de reportes de capacidad y empleo.
* **Justificación de baja latencia:** Procesar estas actualizaciones con baja latencia permite a las entidades territoriales y observatorios de turismo detectar en tiempo real cambios en la capacidad instalada (habitaciones y camas disponibles), identificar de inmediato establecimientos de alto impacto laboral ($\ge 20$ empleados formales directos) y generar alertas operativas por departamento sin esperar al cierre del ciclo batch diario o semanal.

---

### 8. Streaming Pipeline

El componente de streaming está implementado en Python sobre **Apache Kafka** (modo KRaft) y persiste sus resultados agregados directamente en PostgreSQL (`dw_turismo_ods8_v2`).

#### 8.1 Configuración del Tópico y Estrategia de Particionado
* **Broker:** `localhost:9092` (Apache Kafka 3.7.0).
* **Tópico:** `rnt_turismo_eventos`.
* **Llave de particionado (`event_key`):** Código DANE del departamento (`codigo_departamento`, ej. `"76"` para Valle del Cauca, `"13"` para Bolívar). Esto garantiza que todos los eventos pertenecientes a un mismo departamento sean enrutados a la misma partición de Kafka, preservando el orden cronológico de actualización de sus métricas.

#### 8.2 Kafka Producer 
* Extrae registros operativos relevantes desde `fact_turismo_anual` cruzados con `dim_ubicacion` y `dim_categoria`.
* Evita publicar registros vacíos o redundantes sin impacto analítico.
* Serializa cada mensaje en formato JSON UTF-8 con confirmación de escritura en el broker (`acks="all"`).

#### 8.3 Contrato de Eventos (Event Contract) y Reglas de Validación
Todo evento publicado en `rnt_turismo_eventos` debe cumplir estrictamente la siguiente estructura y tipos de datos:

```json
{
  "event_id": "a3a7e6fe-1234-4abc-9def-567890abcdef",
  "event_timestamp": "2026-11-17T21:15:30.120450+00:00",
  "event_key": "76",
  "simulation_mode": "HISTORICAL_REPLAY_RNT",
  "codigo_rnt": 10245,
  "departamento": "VALLE DEL CAUCA",
  "municipio": "CALI",
  "categoria": "ESTABLECIMIENTOS DE ALOJAMIENTO Y HOSPEDAJE",
  "anio": 2025,
  "numero_de_habitaciones": 45,
  "numero_de_camas": 80,
  "numero_de_empleados": 28
}
```
---

El **Kafka Consumer (`streaming/consumer.py`)** ejecuta 5 reglas de validación antes de procesar cualquier mensaje:
1. **Integridad JSON:** El payload debe ser un objeto JSON válido decodificable en UTF-8.
2. **Campos obligatorios:** Presencia no nula de `event_id`, `event_timestamp`, `event_key`, `codigo_rnt`, `departamento`, `categoria`, `numero_de_habitaciones`, `numero_de_camas` y `numero_de_empleados`.
3. **Tipado estricto:** Validación de tipos (`str` para identificadores/texto, `int` estricto excluyendo booleanos para métricas numéricas y `codigo_rnt`).
4. **Estándar temporal:** `event_timestamp` debe cumplir el formato estándar **ISO-8601**.
5. **Reglas de dominio e idempotencia:** Valores numéricos $\ge 0$ (`codigo_rnt > 0`) y control de duplicados mediante registro en memoria de `event_id` procesados.

Los mensajes que incumplen el contrato son rechazados sin detener el servicio, registrados en la tabla de auditoría `streaming_invalid_events` y confirmados en offset para evitar bloqueos del consumidor.

#### 8.4 Métricas de Streaming (`streaming_metrics`)
Por cada evento válido, el consumidor ejecuta una actualización incremental (`UPSERT` vía `ON CONFLICT DO UPDATE`) en la tabla `streaming_metrics` de PostgreSQL, manteniendo 4 indicadores clave por departamento:
1. **`total_pst_procesados`:** Conteo acumulado de prestadores turísticos procesados en el flujo.
2. **`total_empleados_stream`:** Suma acumulada de empleos formales directos reportados (indicador directo ODS 8).
3. **`total_habitaciones_stream` y `total_camas_stream`:** Capacidad instalada hotelera/turística acumulada en tiempo real.
4. **`alertas_alto_impacto_laboral`:** Conteo de establecimientos procesados que generan $\ge 20$ empleos directos.

#### 8.5 Comportamiento ante Fallos y Limitaciones
* **Eventos inválidos:** Ante payloads corruptos, campos faltantes, tipos erróneos o valores negativos, el consumidor captura la excepción de contrato, persiste el detalle del error en `streaming_invalid_events` y continúa consumiendo los siguientes offsets sin interrupción.
* **Interrupción temporal del Consumer:** Al utilizar `enable_auto_commit=False` y confirmar el offset únicamente después de completar el `COMMIT` en PostgreSQL, si el consumidor se detiene temporalmente mientras el productor sigue publicando, los mensajes quedan retenidos de forma durable en el log de Kafka y son procesados automáticamente al reiniciar el consumidor.
* **Limitaciones conocidas:** La deduplicación por `event_id` se mantiene en memoria durante el ciclo de vida del proceso del consumidor; ante un reinicio completo del proceso, la idempotencia a nivel de fila dependería de persistir los `event_id` procesados en una tabla dedicada en PostgreSQL.

---

### 11. Functional Evidence (Componente Streaming)

#### 11.1 Ejecución en vivo: Kafka Producer 
![Ejecución Kafka Producer](assets/evidenciaTerminal2.png)

#### 11.2 Ejecución en vivo: Kafka Consumer
![Ejecución Kafka Consumer](assets/evidenciaTerminal1.png)

#### 11.3 Actualización incremental de métricas en PostgreSQL 
![Métricas en PostgreSQL](assets/evidencia_postgresql_metrics.png)

#### 11.4 Experimento A: Detección y rechazo de eventos inválidos 
![Prueba de Eventos Inválidos](assets/evidencia_evento_invalido.png)

#### 11.5 Experimento B: Interrupción temporal y recuperación del Consumer
**1. Publicación de eventos en Kafka mientras el Consumer está fuera de línea (Offsets 80–94):**
![Envío con Consumer Apagado](assets/evidencia_caida_producer.png)

**2. Reinicio del Consumer y recuperación automática de eventos retenidos sin pérdida de datos:**
![Recuperación del Consumer](assets/evidencia_caida_consumer.png)