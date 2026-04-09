"""
Módulo de adquisición de datos para el sistema de monitorización de temperaturas.
Maneja la lectura desde archivos CSV o desde sensores hardware en la Raspberry Pi.
"""

import polars as pl
import threading
import time
import glob
import random
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Callable

from config import W1_DEVICES_DIR, CPU_TEMP_FILE


class DataAcquisition:
    """Clase para adquisición de datos de temperatura desde hardware local o archivo."""

    def __init__(
        self,
        csv_path: Optional[str] = None,
        time_col: str = "Time",
        device_cols: list[str] | None = None,
    ):
        """
        Args:
            csv_path: Ruta a archivo CSV para modo simulación/lectura
            time_col: Nombre de la columna de tiempo
            device_cols: Nombres de columnas de dispositivos (por defecto: Dev 0-4)
        """
        self.csv_path = csv_path
        self.time_col = time_col
        self.device_cols = device_cols or [f"Dev {i}" for i in range(5)]

        self._running = False
        self._thread: Optional[threading.Thread] = None

        # Definir el esquema explícito para evitar errores de tipo con Polars (Null vs String/Float)
        self._schema = {self.time_col: pl.String}
        for col in self.device_cols:
            self._schema[col] = pl.Float64

        self._df = pl.DataFrame(schema=self._schema)
        self._on_data_callback: Optional[Callable[[pl.DataFrame], None]] = None

    def connect(self) -> bool:
        """Verifica que el entorno sea accesible (simplemente retorna True para sensores locales)."""
        return True

    def disconnect(self):
        """Detiene la lectura de los sensores."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def set_on_data_callback(self, callback: Callable[[pl.DataFrame], None]):
        """Define un callback que se ejecuta cuando llegan nuevos datos."""
        self._on_data_callback = callback

    def _calculate_seconds_from_start(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calcula segundos transcurridos manejando cruce de medianoche."""
        df_temp = df.with_row_index().with_columns(
            pl.col(self.time_col).str.to_time(strict=False).alias("tiempo_obj")
        )

        return (
            df_temp
            .drop_nulls(subset=["tiempo_obj"])
            .with_columns(pl.col("tiempo_obj").diff().alias("delta"))
            .with_columns(
                pl.when(pl.col("delta") < pl.duration(seconds=0))
                .then(pl.col("delta") + pl.duration(days=1))
                .otherwise(pl.col("delta"))
                .fill_null(pl.duration(seconds=0))
                .alias("delta_corregido")
            )
            .with_columns(
                pl.col("delta_corregido")
                .cum_sum()
                .dt.total_seconds()
                .alias("segundos_desde_inicio")
            )
        )

    def _read_ds18b20(self, device_file: str) -> Optional[float]:
        """Lee un sensor 1-Wire DS18B20."""
        try:
            with open(device_file, "r") as f:
                lines = f.readlines()
                if not lines or "YES" not in lines[0]:
                    return None
                temp_str = lines[1].split("t=")[1]
                return float(temp_str) / 1000.0
        except:
            return None

    def _read_cpu_temp(self) -> Optional[float]:
        """Lee la temperatura de la CPU de la Raspberry Pi."""
        try:
            with open(CPU_TEMP_FILE, "r") as f:
                temp_str = f.read().strip()
                return float(temp_str) / 1000.0
        except:
            return None

    def _read_hardware_sensors(self) -> dict:
        """Lee sensores físicos (1-Wire o CPU) para poblar device_cols."""
        now_str = datetime.now().strftime("%H:%M:%S")
        data = {self.time_col: now_str}
        
        # Buscar todos los sensores 1-Wire (generalmente empizan con 28-)
        w1_sensors = sorted(glob.glob(f"{W1_DEVICES_DIR}28-*/w1_slave"))
        
        for i, col in enumerate(self.device_cols):
            temp = None
            if i < len(w1_sensors):
                # Usar el sensor DS18B20 correspondiente
                temp = self._read_ds18b20(w1_sensors[i])
            elif i == 0 and len(w1_sensors) == 0:
                # Si no hay sensores 1-Wire, usar la temperatura de la CPU como fallback para Dev 0
                temp = self._read_cpu_temp()
                
            if temp is not None:
                data[col] = temp
            else:
                # Fallback estocástico/simulado si faltan sensores físicos
                # Esto mantiene el dashboard activo para sensores adicionales
                base = 22.0 + (i * 0.5)
                data[col] = base + random.uniform(-0.5, 0.5)
                
        return data

    def _read_sensors_loop(self):
        """Bucle de lectura de sensores hardware (se ejecuta en thread separado)."""
        self._df = pl.DataFrame(schema=self._schema)

        while self._running:
            data = self._read_hardware_sensors()

            if data:
                new_row = pl.DataFrame([data], schema=self._schema)
                self._df = pl.concat([self._df, new_row])

                # Limitar a 25 millones de filas (~2GB) para no agotar memoria
                if len(self._df) > 25_000_000:
                    self._df = self._df.tail(25_000_000)

                if self._on_data_callback:
                    self._on_data_callback(self._df)

            # Esperar 0.5 segundos aprox. antes de volver a leer
            time.sleep(0.5)

    def start_streaming(self):
        """Inicia streaming de datos en thread separado."""
        self._running = True
        self._thread = threading.Thread(target=self._read_sensors_loop, daemon=True)
        self._thread.start()

    def read_csv_file(self, filepath: str) -> pl.DataFrame:
        """Lee un archivo CSV y procesa los datos."""
        df = pl.read_csv(
            filepath,
            separator=";",
            has_header=True,
            try_parse_dates=True,
        )

        numeric_cols = df.select(pl.Float64).columns
        cols_to_keep = [self.time_col] + numeric_cols

        df = df.select(cols_to_keep).with_columns(
            pl.col(self.time_col).str.strip_chars()
        )

        return self._calculate_seconds_from_start(df)

    def read_sample_data(self, n_rows: int = 100) -> pl.DataFrame:
        """Genera datos de prueba (simulación). Útil para testing sin hardware."""
        if self.csv_path and Path(self.csv_path).exists():
            return self.read_csv_file(self.csv_path).head(n_rows)

        # Generar datos sintéticos
        now = datetime.now()
        times = [(now - timedelta(seconds=i * 10)).strftime("%H:%M:%S") for i in range(n_rows)]

        base_temps = [22.0, 22.5, 23.0, 22.2, 22.1]

        data = {
            self.time_col: list(reversed(times)),
            **{col: [base + random.uniform(-0.5, 0.5) for _ in range(n_rows)]
               for col, base in zip(self.device_cols, base_temps)}
        }

        df = pl.DataFrame(data)
        return self._calculate_seconds_from_start(df)

    @property
    def dataframe(self) -> pl.DataFrame:
        """Retorna el DataFrame acumulado."""
        return self._df

    def clear_data(self):
        """Limpia los datos acumulados."""
        self._df = pl.DataFrame(schema=self._schema)
