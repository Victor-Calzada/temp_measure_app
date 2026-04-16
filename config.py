"""
Configuración centralizada del proyecto.
Edita este archivo para personalizar el comportamiento.
"""

# ============================================================
# CONFIGURACIÓN DE DISPOSITIVOS
# ============================================================
DEVICE_COLS = [f"Dev {i}" for i in range(5)]  # Nombres de dispositivos
TIME_COL = "Time"  # Nombre de columna de tiempo

# ============================================================
# CONFIGURACIÓN DE SENSORES HARDWARE
# ============================================================
W1_DEVICES_DIR = "/sys/bus/w1/devices/"
CPU_TEMP_FILE = "/sys/class/thermal/thermal_zone0/temp"

# ============================================================
# CONFIGURACIÓN DE VISUALIZACIÓN
# ============================================================
REFRESH_RATE = 5  # Segundos entre actualizaciones de la UI
DEFAULT_SAMPLING_INTERVAL = 1.0  # Segundos entre lecturas de sensores
MAX_DATA_POINTS = 10000  # Máximo de puntos en memoria
CHART_HEIGHT = 450  # Altura del gráfico principal en px

# ============================================================
# CONFIGURACIÓN DE TEMPERATURA
# ============================================================
TEMP_MIN = 15.0  # Rango mínimo para gauges
TEMP_MAX = 35.0  # Rango máximo para gauges
TEMP_WARNING_LOW = 20.0  # Umbral bajo
TEMP_WARNING_HIGH = 30.0  # Umbral alto

# ============================================================
# RUTAS
# ============================================================
DATA_DIR = "data"
DEFAULT_CSV = "data/Medida_nueva.txt"
USB_PATH = "/media/ladicim/3A1D-0E89"
FALLBACK_PATH = "~/Downloads"

# ============================================================
# COLORES (Dispositivos)
# ============================================================
DEVICE_COLORS = {
    "Dev 0": "#FF6B6B",
    "Dev 1": "#4ECDC4",
    "Dev 2": "#45B7D1",
    "Dev 3": "#96CEB4",
    "Dev 4": "#FFEAA7",
}
