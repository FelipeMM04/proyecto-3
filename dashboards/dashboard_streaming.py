import os
import time
from datetime import datetime
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

st.set_page_config(
    page_title="Monitoreo Streaming RNT — ODS 8",
    page_icon="⚡",
    layout="wide",
)

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5433"),
        dbname=os.getenv("POSTGRES_DB", "dw_turismo_ods8_v2"),
        user=os.getenv("POSTGRES_USER", "etl_user"),
        password=os.getenv("POSTGRES_PASSWORD", "etl_password"),
    )

def cargar_datos():
    conn = get_connection()
    try:
        df_metrics = pd.read_sql(
            """
            SELECT 
                departamento,
                total_pst_procesados,
                total_empleados_stream,
                total_habitaciones_stream,
                total_camas_stream,
                alertas_alto_impacto_laboral,
                ultimo_event_id,
                ultima_actualizacion
            FROM streaming_metrics
            ORDER BY ultima_actualizacion DESC, total_empleados_stream DESC;
            """,
            conn,
        )
        df_invalid = pd.read_sql(
            """
            SELECT 
                id_error,
                motivo_rechazo,
                fecha_rechazo,
                raw_payload
            FROM streaming_invalid_events
            ORDER BY fecha_rechazo DESC
            LIMIT 20;
            """,
            conn,
        )
        total_invalid = int(
            pd.read_sql("SELECT COUNT(*) AS n FROM streaming_invalid_events;", conn)["n"][0]
        )
        return df_metrics, df_invalid, total_invalid
    finally:
        conn.close()

# Inicializar estado de sesión para historial en vivo y deltas
if "history" not in st.session_state:
    st.session_state.history = []
if "prev_totals" not in st.session_state:
    st.session_state.prev_totals = None
if "prev_dept_state" not in st.session_state:
    st.session_state.prev_dept_state = {}
if "live_feed" not in st.session_state:
    st.session_state.live_feed = []

# Encabezado y controles
st.title("⚡ Centro de Monitoreo en Tiempo Real — Turismo y Empleo (ODS 8)")
st.caption("Arquitectura Event-Driven: Apache Kafka (`rnt_turismo_eventos`) → Consumer Python → PostgreSQL (`dw_turismo_ods8_v2`)")

c1, c2, c3 = st.columns([1, 1, 4])
with c1:
    auto_refresh = st.toggle("Auto-refrescar (2s)", value=True)
with c2:
    if st.button("Actualizar ahora"):
        st.rerun()
with c3:
    if st.button("Reiniciar historial visual"):
        st.session_state.history = []
        st.session_state.live_feed = []
        st.session_state.prev_totals = None
        st.session_state.prev_dept_state = {}
        st.rerun()

try:
    df_metrics, df_invalid, total_invalid = cargar_datos()

    pst_total = int(df_metrics["total_pst_procesados"].sum()) if not df_metrics.empty else 0
    emp_total = int(df_metrics["total_empleados_stream"].sum()) if not df_metrics.empty else 0
    hab_total = int(df_metrics["total_habitaciones_stream"].sum()) if not df_metrics.empty else 0
    alert_total = int(df_metrics["alertas_alto_impacto_laboral"].sum()) if not df_metrics.empty else 0
    total_eventos = pst_total + total_invalid
    tasa_calidad = (pst_total / total_eventos * 100) if total_eventos > 0 else 100.0

    # Calcular deltas desde el último refresco
    prev = st.session_state.prev_totals
    d_pst = pst_total - prev["pst"] if prev else 0
    d_emp = emp_total - prev["emp"] if prev else 0
    d_hab = hab_total - prev["hab"] if prev else 0
    d_alert = alert_total - prev["alert"] if prev else 0
    d_inv = total_invalid - prev["inv"] if prev else 0

    st.session_state.prev_totals = {
        "pst": pst_total,
        "emp": emp_total,
        "hab": hab_total,
        "alert": alert_total,
        "inv": total_invalid,
    }

    # Registrar punto en la serie de tiempo en vivo
    ahora_str = datetime.now().strftime("%H:%M:%S")
    st.session_state.history.append({
        "hora": ahora_str,
        "Empleos Formales": emp_total,
        "PST Procesados": pst_total,
        "Alertas ODS 8 (≥20 emp.)": alert_total,
    })
    if len(st.session_state.history) > 40:
        st.session_state.history = st.session_state.history[-40:]

    # Detectar eventos entrantes por departamento para el feed en vivo
    if not df_metrics.empty:
        new_dept_state = {}
        for _, row in df_metrics.iterrows():
            depto = row["departamento"]
            cur_pst = int(row["total_pst_procesados"])
            cur_emp = int(row["total_empleados_stream"])
            cur_hab = int(row["total_habitaciones_stream"])
            new_dept_state[depto] = (cur_pst, cur_emp, cur_hab)

            if depto in st.session_state.prev_dept_state:
                old_pst, old_emp, old_hab = st.session_state.prev_dept_state[depto]
                if cur_pst > old_pst or cur_emp > old_emp:
                    st.session_state.live_feed.insert(0, {
                        "Hora": ahora_str,
                        "Departamento": depto,
                        "Nuevos PST": f"+{cur_pst - old_pst}",
                        "Nuevos Empleos": f"+{cur_emp - old_emp}",
                        "Nuevas Habitaciones": f"+{cur_hab - old_hab}",
                        "Último Event ID": str(row["ultimo_event_id"])[:12] + "...",
                    })
        st.session_state.prev_dept_state = new_dept_state
        st.session_state.live_feed = st.session_state.live_feed[:10]

    # Indicador de estado en vivo
    if d_pst > 0 or d_emp > 0:
        st.success(f"🟢 FLUJO ACTIVO ({ahora_str}) — Entraron +{d_pst} PST y +{d_emp} empleos en el último ciclo.")
    elif d_inv > 0:
        st.warning(f"🟠 ALERTA DE CONTRATO ({ahora_str}) — Se bloquearon +{d_inv} eventos inválidos.")
    else:
        st.info(f"🔵 EN ESCUCHA ({ahora_str}) — Esperando nuevos mensajes en Kafka | Calidad de contrato: {tasa_calidad:.1f}%")

    # KPIs con indicadores de cambio en tiempo real
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("PST Procesados", f"{pst_total:,}", delta=f"+{d_pst}" if d_pst else None)
    k2.metric("Empleos Formales", f"{emp_total:,}", delta=f"+{d_emp}" if d_emp else None)
    k3.metric("Habitaciones Activas", f"{hab_total:,}", delta=f"+{d_hab}" if d_hab else None)
    k4.metric("Alertas Alto Impacto (≥20)", f"{alert_total:,}", delta=f"+{d_alert}" if d_alert else None)
    k5.metric("Eventos Rechazados", f"{total_invalid:,}", delta=f"+{d_inv}" if d_inv else None, delta_color="inverse")

    st.divider()

    # Fila 1: Curva temporal en vivo + Bitácora de eventos entrantes
    col_live1, col_live2 = st.columns([3, 2])

    with col_live1:
        st.subheader("📈 Evolución de Empleos Formales en Vivo")
        df_hist = pd.DataFrame(st.session_state.history)
        fig_line = go.Figure()
        fig_line.add_trace(go.Scatter(
            x=df_hist["hora"],
            y=df_hist["Empleos Formales"],
            mode="lines+markers",
            name="Empleos Formales",
            line=dict(color="#00CC96", width=3),
            marker=dict(size=7),
        ))
        fig_line.update_yaxes(autorange=True, rangemode="normal")
        fig_line.update_layout(
            height=320,
            margin=dict(l=10, r=10, t=20, b=10),
            xaxis_title="Hora de captura",
            yaxis_title="Empleos acumulados",
        )
        st.plotly_chart(fig_line, use_container_width=True)

    with col_live2:
        st.subheader("⚡ Últimos Eventos Procesados")
        if st.session_state.live_feed:
            st.dataframe(pd.DataFrame(st.session_state.live_feed), use_container_width=True, hide_index=True, height=300)
        else:
            st.caption("Ejecuta `producer.py` para ver aquí cada evento nuevo entrando segundo a segundo.")

    st.divider()

    # Fila 2: Comparativas departamentales
    if not df_metrics.empty:
        g1, g2 = st.columns(2)

        with g1:
            df_sorted = df_metrics.sort_values("total_empleados_stream", ascending=True)
            fig_emp = px.bar(
                df_sorted,
                x="total_empleados_stream",
                y="departamento",
                orientation="h",
                title="Empleos Formales Acumulados por Departamento",
                labels={"total_empleados_stream": "Empleos Directos", "departamento": "Departamento"},
                color="total_empleados_stream",
                color_continuous_scale="Teal",
                text_auto=True,
            )
            fig_emp.update_layout(showlegend=False, height=360)
            st.plotly_chart(fig_emp, use_container_width=True)

        with g2:
            fig_alert = px.bar(
                df_metrics,
                x="departamento",
                y=["total_pst_procesados", "alertas_alto_impacto_laboral"],
                barmode="group",
                title="PST Procesados vs. Alertas de Alto Impacto (≥ 20 empleados)",
                labels={"value": "Cantidad", "departamento": "Departamento", "variable": "Métrica"},
                text_auto=True,
            )
            fig_alert.update_layout(height=360)
            st.plotly_chart(fig_alert, use_container_width=True)

        with st.expander("Ver tabla consolidada completa por departamento", expanded=False):
            st.dataframe(df_metrics, use_container_width=True, hide_index=True)

    # Sección de auditoría de calidad (eventos inválidos)
    st.subheader("🛡️ Auditoría de Calidad en Streaming (Contratos Rechazados)")
    if df_invalid.empty:
        st.success("No se han registrado eventos inválidos.")
    else:
        c_inv1, c_inv2 = st.columns([1, 2])
        with c_inv1:
            resumen_rechazos = df_invalid["motivo_rechazo"].value_counts().reset_index()
            resumen_rechazos.columns = ["Motivo de Rechazo", "Cantidad"]
            st.dataframe(resumen_rechazos, use_container_width=True, hide_index=True)
        with c_inv2:
            st.dataframe(df_invalid, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error conectando a PostgreSQL (`localhost:5433`): {e}")

if auto_refresh:
    time.sleep(2)
    st.rerun()