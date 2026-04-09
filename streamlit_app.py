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
from config import USB_PATH, FALLBACK_PATH

from data_acquisition import DataAcquisition

# ============================================================
# Configuración de la página
# ============================================================
st.set_page_config(
    page_title="Monitor de Temperatura",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# Inicialización del estado de sesión
# ============================================================
if "acquisition" not in st.session_state:
    st.session_state.acquisition = None

if "connected" not in st.session_state:
    st.session_state.connected = False

if "df" not in st.session_state:
    st.session_state.df = pl.DataFrame()

if "start_time" not in st.session_state:
    st.session_state.start_time = None

if "timer_active" not in st.session_state:
    st.session_state.timer_active = False

if "timer_end_time" not in st.session_state:
    st.session_state.timer_end_time = None

if "timer_duration_hrs" not in st.session_state:
    st.session_state.timer_duration_hrs = 1.0

if "last_export_path" not in st.session_state:
    st.session_state.last_export_path = None


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
        title="Temperaturas en Tiempo Real",
        xaxis_title="Tiempo (segundos desde inicio)" if time_col in df.columns else "Tiempo",
        yaxis_title="Temperatura (°C)",
        height=450,
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        margin=dict(l=40, r=40, t=60, b=40),
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
            "axis": {"range": [15, 35], "tickwidth": 1},
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
        number={"suffix": " °C", "font": {"size": 20}},
        title={"text": name, "font": {"size": 14}},
    ))

    fig.update_layout(
        height=150,
        margin=dict(l=20, r=20, t=30, b=20),
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
# Sidebar - Configuración
# ============================================================
with st.sidebar:
    st.header("⚙️ Configuración")

    # Selección de modo
    mode = st.radio(
        "Modo de operación",
        ["📁 Simulación (CSV)", "📡 Sensores Internos (Tiempo Real)"],
        captions=[
            "Carga datos desde archivo CSV",
            "Lee hardware local en Raspberry Pi",
        ],
    )

    if mode == "📁 Simulación (CSV)":
        csv_file = st.text_input(
            "Ruta al archivo CSV",
            value="data/Medida_nueva.txt",
            help="Ruta absoluta o relativa al archivo de datos",
        )

        if st.button("📂 Cargar Datos", type="primary", use_container_width=True):
            try:
                with st.spinner("Cargando datos..."):
                    acq = DataAcquisition(csv_path=csv_file)
                    df = acq.read_csv_file(csv_file)
                    st.session_state.df = df
                    st.session_state.start_time = datetime.now()
                    st.success(f"✅ Cargados {len(df)} registros")
            except Exception as e:
                st.error(f"❌ Error: {e}")

    else:
        # Configuración local
        if not st.session_state.connected:
            if st.button("▶️ Iniciar Lectura", type="primary", use_container_width=True):
                try:
                    acq = DataAcquisition()
                    acq.connect()
                    acq.start_streaming()
                    st.session_state.acquisition = acq
                    st.session_state.connected = True
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")
        else:
            st.success("🟢 Leyendo Sensores")

            if st.button("⏹️ Detener Lectura", use_container_width=True):
                if st.session_state.acquisition:
                    st.session_state.acquisition.disconnect()
                st.session_state.connected = False
                st.session_state.acquisition = None
                st.rerun()

    st.divider()

    # Configuración de visualización
    st.subheader("📊 Visualización")

    refresh_rate = st.slider(
        "Intervalo de actualización (s)",
        min_value=1,
        max_value=60,
        value=5,
    )

    max_points = st.slider(
        "Máximo de puntos a mostrar",
        min_value=100,
        max_value=5000,
        value=1000,
        step=100,
    )

    show_stats = st.checkbox("Mostrar estadísticas", value=True)

    st.divider()

    # Información del sistema
    st.subheader("ℹ️ Sistema")
    st.caption(f"Inicio: {st.session_state.start_time.strftime('%H:%M:%S') if st.session_state.start_time else 'N/A'}")

    if st.session_state.connected and st.session_state.acquisition:
        elapsed = (datetime.now() - st.session_state.start_time).total_seconds() if st.session_state.start_time else 0
        st.caption(f"Tiempo de conexión: {elapsed:.0f}s")

        if len(st.session_state.df) > 0:
            st.caption(f"Total lecturas: {len(st.session_state.df)}")


# ============================================================
# Título principal
# ============================================================
col_title, col_status = st.columns([4, 1])

with col_title:
    st.title("🌡️ Monitor de Temperatura en Tiempo Real")

with col_status:
    if mode == "📡 Sensores Internos (Tiempo Real)":
        if st.session_state.connected:
            st.success("LEYENDO")
        else:
            st.info("DETENIDO")

# ============================================================
# Adquisición de datos en tiempo real y Lógica del Temporizador
# ============================================================
if mode == "📡 Sensores Internos (Tiempo Real)" and st.session_state.connected:
    if st.session_state.acquisition:
        # Recuperar el dataframe acumulado en el thread secundario
        acq = st.session_state.acquisition
        current_data = acq.dataframe
        if current_data is not None and not current_data.is_empty():
            st.session_state.df = current_data.tail(max_points)

    # Verificación del Temporizador
    if st.session_state.timer_active and st.session_state.timer_end_time:
        if datetime.now() >= st.session_state.timer_end_time:
            # Exportar datos completos antes de detener
            full_df = st.session_state.acquisition.dataframe
            if full_df is not None and not full_df.is_empty():
                st.session_state.last_export_path = handle_auto_export(full_df)

            # Detener lectura
            st.session_state.acquisition.disconnect()
            st.session_state.connected = False
            st.session_state.timer_active = False
            st.rerun()

# ============================================================
# Contenido principal con Pestañas
# ============================================================
tab_dash, tab_timer = st.tabs(["📈 Dashboard", "⏱️ Temporizador"])

with tab_dash:
    if len(st.session_state.df) > 0:
        df = st.session_state.df

        # ============================================================
        # Métricas y Gauges
        # ============================================================
        if show_stats:
            stats = create_stats_panel(df)

            cols = st.columns(5)
            colors = get_device_colors()

            for i, (col, col_stats) in enumerate(stats.items()):
                with cols[i]:
                    st.plotly_chart(
                        create_temperature_gauge(col_stats["actual"], col, colors[col]),
                        use_container_width=True,
                    )

            # Subtítulo con rango de datos
            if "segundos_desde_inicio" in df.columns:
                t_min = df["segundos_desde_inicio"].min()
                t_max = df["segundos_desde_inicio"].max()
                st.caption(f"⏱️ Rango temporal: {t_min:.0f}s - {t_max:.0f}s ({t_max - t_min:.0f}s de duración)")

        # ============================================================
        # Gráfico principal
        # ============================================================
        st.plotly_chart(
            create_temperature_chart(df),
            use_container_width=True,
        )

        # ============================================================
        # Estadísticas detalladas
        # ============================================================
        with st.expander("📈 Estadísticas Detalladas"):
            if show_stats:
                stats = create_stats_panel(df)

                stat_cols = st.columns(len(stats))
                for i, (col, col_stats) in enumerate(stats.items()):
                    with stat_cols[i]:
                        st.metric(col, f"{col_stats['actual']:.2f} °C")
                        st.caption(f"Mín: {col_stats['min']:.2f} | Máx: {col_stats['max']:.2f}")
                        st.caption(f"Media: {col_stats['mean']:.2f} | σ: {col_stats['std']:.2f}")

        # ============================================================
        # Tabla de datos recientes
        # ============================================================
        with st.expander("📋 Datos Recientes"):
            display_cols = ["Time"] + [c for c in df.columns if c.startswith("Dev")]
            available_cols = [c for c in display_cols if c in df.columns]

            if available_cols:
                st.dataframe(
                    df.select(available_cols).tail(20),
                    use_container_width=True,
                    hide_index=True,
                )

                csv_data = df.select(available_cols).write_csv()
                st.download_button(
                    "⬇️ Descargar CSV",
                    csv_data,
                    file_name=f"temperaturas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )
    else:
        # Estado vacío
        st.info("👈 Configure la conexión y cargue datos para comenzar")
        st.markdown(
            """
            ### Instrucciones:
            1. **Modo Simulación**: Ingrese la ruta a un archivo CSV con formato `Time;Dev 0;Dev 1;...`
            2. **Modo Sensores Internos**: Lea directamente el hardware local 1-Wire.

            El formato esperado del archivo CSV es:
            ```
            Time;Dev 0;Dev 1;Dev 2;Dev 3;Dev 4
            16:59:25;22.062;22.375;23.312;22.187;22.125
            ```
            """
        )

with tab_timer:
    st.header("⏱️ Temporizador de Medida")
    st.info("Permite programar la detención automática de la lectura y el guardado de datos.")

    col_input, col_status = st.columns([2, 3])

    with col_input:
        duration = st.number_input(
            "Duración de la toma (horas)",
            min_value=0.01,
            max_value=100.0,
            value=st.session_state.timer_duration_hrs,
            step=0.1
        )
        st.session_state.timer_duration_hrs = duration

        start_btn_disabled = not st.session_state.connected
        if st.button("🚀 Iniciar Temporizador", type="primary", use_container_width=True, disabled=start_btn_disabled):
            st.session_state.timer_active = True
            st.session_state.timer_end_time = datetime.now() + timedelta(hours=duration)
            st.rerun()

        if st.session_state.timer_active:
            if st.button("⏹️ Cancelar Temporizador", use_container_width=True):
                st.session_state.timer_active = False
                st.session_state.timer_end_time = None
                st.rerun()

    with col_status:
        if st.session_state.timer_active and st.session_state.timer_end_time:
            remaining = st.session_state.timer_end_time - datetime.now()
            if remaining.total_seconds() > 0:
                st.metric("Tiempo Restante", str(remaining - timedelta(0)).split('.')[0])
                st.progress(max(0.0, min(1.0, 1.0 - (remaining.total_seconds() / (duration * 3600)))))
            else:
                st.warning("⏳ Temporizador expirado. Procesando exportación...")
        else:
            st.write("El temporizador no está activo.")

    if st.session_state.last_export_path:
        st.divider()
        st.success(f"✅ Última exportación automática guardada en:\n`{st.session_state.last_export_path}`")


# ============================================================
# Auto-actualización
# ============================================================
if mode == "📡 Sensores Internos (Tiempo Real)" and st.session_state.connected:
    time.sleep(refresh_rate)
    st.rerun()
