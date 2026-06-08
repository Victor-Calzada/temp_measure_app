"""
Dashboard Streamlit para monitorización de temperaturas en tiempo real.
Diseñado para ejecutarse en Raspberry Pi.
"""

import streamlit as st
import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from datetime import datetime, timedelta
import os
from pathlib import Path
from config import USB_PATH, FALLBACK_PATH, DATA_DIR

from data_acquisition import DataAcquisition

# ============================================================
# Configuración de la página
# ============================================================
st.set_page_config(
    page_title="Monitor de Temperatura",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ============================================================
# Inicialización del estado de sesión y Recursos
# ============================================================
LOG_FILE = os.path.join(DATA_DIR, "active_measurement.csv")

@st.cache_resource
def get_acquisition_engine(log_path=None):
    """Retorna el motor de adquisición persistente entre sesiones."""
    return DataAcquisition(log_path=log_path)

if "connected" not in st.session_state:
    # Si existe el archivo de log activo, es posible que vengamos de una desconexión
    st.session_state.connected = os.path.exists(LOG_FILE)

if "df" not in st.session_state:
    st.session_state.df = pl.DataFrame()

if "start_time" not in st.session_state:
    # Intentar recuperar start_time desde el log si existe
    if os.path.exists(LOG_FILE):
        try:
            mtime = os.path.getctime(LOG_FILE)
            st.session_state.start_time = datetime.fromtimestamp(mtime)
        except:
            st.session_state.start_time = None
    else:
        st.session_state.start_time = None

if "timer_active" not in st.session_state:
    st.session_state.timer_active = False

if "timer_end_time" not in st.session_state:
    st.session_state.timer_end_time = None

if "timer_duration_hrs" not in st.session_state:
    st.session_state.timer_duration_hrs = 1.0

if "last_export_path" not in st.session_state:
    st.session_state.last_export_path = None

if "sampling_interval" not in st.session_state:
    from config import DEFAULT_SAMPLING_INTERVAL
    st.session_state.sampling_interval = DEFAULT_SAMPLING_INTERVAL


# ============================================================
# Funciones auxiliares
# ============================================================
@st.cache_data(ttl=1)
def get_device_colors():
    """Colores para cada dispositivo."""
    return {
        "Dev 0": "#FF6B6B",
        "Dev 1": "#4ECDC4",
        "Dev 2": "#45B7D1",
        "Dev 3": "#96CEB4",
        "Dev 4": "#FFEAA7",
    }


def create_temperature_chart(df: pl.DataFrame, time_col: str = "segundos_desde_inicio") -> go.Figure:
    """Crea gráfico de temperaturas vs tiempo."""
    colors = get_device_colors()
    fig = go.Figure()

    device_cols = [col for col in df.columns if col.startswith("Dev")]

    for col in device_cols:
        fig.add_trace(go.Scatter(
            x=df[time_col] if time_col in df.columns else df["Time"],
            y=df[col],
            mode="lines",
            name=col,
            line=dict(color=colors.get(col, "#888888"), width=2),
            hovertemplate=f"{col}: %{{y:.2f}} °C<br>{time_col}: %{{x:.1f}}<extra></extra>",
        ))

    fig.update_layout(
        height=280,
        hovermode="x unified",
        xaxis=dict(
            title=dict(text="Tiempo (s)" if time_col in df.columns else "Tiempo", font=dict(size=10)),
            tickfont=dict(size=9),
            gridcolor="#E2E8F0"
        ),
        yaxis=dict(
            title=dict(text="Temp (°C)", font=dict(size=10)),
            tickfont=dict(size=9),
            gridcolor="#E2E8F0"
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=9),
        ),
        margin=dict(l=30, r=10, t=25, b=25),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
    )

    return fig


def create_temperature_gauge(temp: float, name: str, color: str) -> go.Figure:
    """Crea un gauge (indicador circular) para mostrar temperatura actual."""
    fig = go.Figure()

    fig.add_trace(go.Indicator(
        mode="gauge+number",
        value=temp,
        domain={"x": [0, 1], "y": [0, 1]},
        gauge={
            "axis": {"range": [15, 35], "tickwidth": 1, "tickfont": {"size": 8}},
            "bar": {"color": color},
            "borderwidth": 0,
            "bordercolor": "#333",
            "steps": [
                {"range": [15, 20], "color": "#74b9ff"},
                {"range": [20, 25], "color": "#55efc4"},
                {"range": [25, 30], "color": "#fdcb6e"},
                {"range": [30, 35], "color": "#e17055"},
            ],
        },
        number={"suffix": " °C", "font": {"size": 13}, "valueformat": ".1f"},
        title={"text": name, "font": {"size": 10}, "position": "top center"},
    ))

    fig.update_layout(
        height=95,
        margin=dict(l=5, r=5, t=10, b=5),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
    )

    return fig


def create_stats_panel(df: pl.DataFrame) -> dict:
    """Calcula estadísticas para cada dispositivo."""
    stats = {}
    device_cols = [col for col in df.columns if col.startswith("Dev")]

    for col in device_cols:
        if col in df.columns:
            temps = df[col].drop_nulls()
            if len(temps) > 0:
                stats[col] = {
                    "actual": temps.tail(1)[0] if len(temps) > 0 else 0,
                    "min": temps.min(),
                    "max": temps.max(),
                    "mean": temps.mean(),
                    "std": temps.std() if len(temps) > 1 else 0,
                }

    return stats


def handle_auto_export(df: pl.DataFrame):
    """Guarda el dataframe en USB o carpeta fallback."""
    target_dir = USB_PATH if os.path.exists(USB_PATH) else os.path.expanduser(FALLBACK_PATH)

    # Crear directorio si no existe (especialmente para fallback)
    os.makedirs(target_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"measurements_{timestamp}.csv"
    full_path = os.path.join(target_dir, filename)

    df.write_csv(full_path)
    return full_path


# ============================================================
# Adquisición de datos en tiempo real
# ============================================================
# Recuperar el motor de adquisición persistente
acq = get_acquisition_engine(log_path=LOG_FILE)
# Fijado a 5 segundos según el requerimiento
acq.sampling_interval = 5.0

if st.session_state.connected:
    # Sincronizar estado del temporizador y exportación con session_state
    st.session_state.timer_active = acq.timer_active
    st.session_state.timer_end_time = acq.timer_end_time
    st.session_state.last_export_path = acq.last_export_path
    
    # Sincronizar estado de conexión (por si se detuvo solo)
    st.session_state.connected = acq._running
    
    # Recuperar el dataframe acumulado
    current_data = acq.dataframe
    if current_data is not None:
        if not current_data.is_empty():
            # Procesar tiempos para visualización
            processed_data = acq._calculate_seconds_from_start(current_data)
            # Fijado a 1000 puntos
            st.session_state.df = processed_data.tail(1000)
        else:
            st.session_state.df = pl.DataFrame()
            
    # Si se ha desconectado solo (por temporizador) y teníamos el log, ya se habrá borrado/movido en el motor
    if not acq._running:
        st.session_state.connected = False
        st.rerun()

# ============================================================
# Cabecera de control (Reemplaza la barra lateral y título original)
# ============================================================
col_header, col_btn = st.columns([3, 1])

with col_header:
    st.markdown("<h3 style='margin: 0; padding: 0;'>🌡️ Monitor de Temperatura</h3>", unsafe_allow_html=True)
    
    # Mostrar estado de conexión y detalles en una sola línea compacta
    if st.session_state.connected:
        elapsed = (datetime.now() - st.session_state.start_time).total_seconds() if st.session_state.start_time else 0
        total_pts = len(st.session_state.df)
        st.markdown(
            f"<div style='font-size: 13px; margin-top: 2px;'>"
            f"<span style='color:#4ECDC4; font-weight:bold;'>🟢 LEYENDO</span> | "
            f"Tiempo activo: <b>{elapsed:.0f}s</b> | "
            f"Puntos: <b>{total_pts}/1000</b>"
            f"</div>",
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            "<div style='font-size: 13px; margin-top: 2px;'>"
            "<span style='color:#FF6B6B; font-weight:bold;'>🔴 DETENIDO</span> | "
            "Listo para iniciar"
            "</div>",
            unsafe_allow_html=True
        )

with col_btn:
    # Botón para Iniciar/Detener lectura
    if not st.session_state.connected:
        if st.button("▶️ Iniciar Lectura", type="primary", use_container_width=True):
            try:
                if acq.connect():
                    acq.sampling_interval = 5.0
                    acq.start_streaming()
                    st.session_state.connected = True
                    st.session_state.start_time = datetime.now()
                    st.rerun()
                else:
                    st.error("❌ No se detectaron sensores físicos (1-Wire o CPU).")
            except Exception as e:
                st.error(f"Error: {e}")
    else:
        if st.button("⏹️ Detener Lectura", type="secondary", use_container_width=True):
            acq.disconnect()
            st.session_state.connected = False
            
            # Al detener manualmente, si el archivo de log existe, lo movemos a histórico o lo borramos
            if os.path.exists(LOG_FILE):
                try:
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    final_path = os.path.join(DATA_DIR, f"manual_stop_{timestamp}.csv")
                    os.rename(LOG_FILE, final_path)
                    st.info(f"💾 Datos guardados en {final_path}")
                except:
                    try:
                        os.remove(LOG_FILE)
                    except:
                        pass
            
            # Forzar recreación del motor la próxima vez
            st.cache_resource.clear()
            st.rerun()

st.markdown("<hr style='margin: 10px 0 15px 0;'>", unsafe_allow_html=True)

# ============================================================
# Contenido principal con Pestañas
# ============================================================
tab_dash, tab_timer = st.tabs(["📈 Dashboard", "⏱️ Temporizador"])

with tab_dash:
    if len(st.session_state.df) > 0:
        df = st.session_state.df

        # ============================================================
        # Métricas y Gauges (Temperatura actual)
        # ============================================================
        stats = create_stats_panel(df)
        num_gauges = len(stats)
        if num_gauges > 0:
            cols = st.columns(num_gauges)
            colors = get_device_colors()

            for i, (col, col_stats) in enumerate(stats.items()):
                with cols[i]:
                    st.plotly_chart(
                        create_temperature_gauge(col_stats["actual"], col, colors.get(col, "#888888")),
                        use_container_width=True,
                    )

        # ============================================================
        # Gráfico principal
        # ============================================================
        st.plotly_chart(
            create_temperature_chart(df),
            use_container_width=True,
        )

        # ============================================================
        # Estadísticas detalladas y Exportar (Compactado)
        # ============================================================
        col_detail, col_export = st.columns([2, 1])
        
        with col_detail:
            with st.expander("📈 Estadísticas Detalladas", expanded=False):
                stat_cols = st.columns(len(stats))
                for i, (col, col_stats) in enumerate(stats.items()):
                    with stat_cols[i]:
                        st.metric(col, f"{col_stats['actual']:.1f} °C")
                        st.caption(f"Min: {col_stats['min']:.1f} | Max: {col_stats['max']:.1f}")

        with col_export:
            with st.expander("📥 Descargar CSV", expanded=False):
                display_cols = ["Time"] + [c for c in df.columns if c.startswith("Dev")]
                available_cols = [c for c in display_cols if c in df.columns]
                
                if available_cols:
                    @st.cache_data(ttl=30)
                    def convert_df_to_csv(df_to_convert, columns):
                        return df_to_convert.select(columns).write_csv()
                    
                    csv_data = convert_df_to_csv(df, available_cols)
                    st.download_button(
                        "⬇️ Descargar Datos",
                        csv_data,
                        file_name=f"temperaturas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
    else:
        # Estado vacío
        st.info("ℹ️ Presione el botón 'Iniciar Lectura' arriba para comenzar a medir desde los sensores físicos.")
        st.markdown(
            """
            ### Instrucciones:
            1. Conecte los sensores de temperatura 1-Wire (DS18B20) a los pines correspondientes de la Raspberry Pi.
            2. Presione el botón **Iniciar Lectura** en la esquina superior derecha.
            3. Los datos comenzarán a mostrarse en tiempo real cada 5 segundos.
            4. El sistema mantiene los últimos 1000 puntos en pantalla.
            """
        )

with tab_timer:
    col_graph, col_opts = st.columns([2, 1])

    with col_graph:
        if st.session_state.timer_active and len(st.session_state.df) > 0:
            st.plotly_chart(
                create_temperature_chart(st.session_state.df),
                use_container_width=True,
                key="timer_chart"
            )
        else:
            st.info("📈 La gráfica de evolución temporal se mostrará aquí una vez que el temporizador comience a registrar datos.")

    with col_opts:
        st.markdown("#### ⏱️ Configuración")
        
        duration = st.number_input(
            "Duración (horas)",
            min_value=0.01,
            max_value=170.0,
            value=st.session_state.get('timer_duration_hrs', 1.0),
            step=0.1
        )
        st.session_state.timer_duration_hrs = duration

        # Intervalo de muestreo fijo a 5s
        st.session_state.sampling_interval = 5.0
        st.caption("⏱️ Intervalo de muestreo fijado a 5 segundos.")

        st.divider()
        
        # Iniciar/Detener temporizador
        if not st.session_state.timer_active:
            start_btn_disabled = not st.session_state.connected
            if st.button("🚀 Iniciar Temporizador", type="primary", use_container_width=True, disabled=start_btn_disabled):
                acq.timer_active = True
                acq.timer_end_time = datetime.now() + timedelta(hours=duration)
                acq.sampling_interval = 5.0
                
                st.session_state.timer_active = True
                st.session_state.timer_end_time = acq.timer_end_time
                st.rerun()
        else:
            if st.button("⏹️ Cancelar Temporizador", use_container_width=True):
                acq.timer_active = False
                acq.timer_end_time = None
                st.session_state.timer_active = False
                st.session_state.timer_end_time = None
                st.rerun()

        # Estado del temporizador
        if st.session_state.timer_active and st.session_state.timer_end_time:
            remaining = st.session_state.timer_end_time - datetime.now()
            if remaining.total_seconds() > 0:
                st.metric("Tiempo Restante", str(remaining - timedelta(0)).split('.')[0])
                st.progress(max(0.0, min(1.0, 1.0 - (remaining.total_seconds() / (duration * 3600)))))
            else:
                st.warning("⏳ Expirado. Procesando...")

        if acq.last_export_path:
            st.success(f"✅ Exportado a:\n`{acq.last_export_path}`")


# ============================================================
# Auto-actualización
# ============================================================
if st.session_state.connected:
    time.sleep(5)
    st.rerun()
