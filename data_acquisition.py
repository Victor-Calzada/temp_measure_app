"""
Módulo de adquisición de datos para el sistema de monitorización de temperaturas.
Maneja la lectura desde archivos CSV o desde conexión serial (Arduino/Raspberry Pi).
"""

import polars as pl
import serial
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Callable


class DataAcquisition:
    """Clase para adquisición de datos de temperatura desde serial o archivo."""

    def __init__(
        self,
        port: str = "/dev/ttyUSB0",
        baudrate: int = 9600,
        csv_path: Optional[str] = None,
        time_col: str = "Time",
        device_cols: list[str] | None = None,
    ):
        """
        Args:
            port: Puerto serial (ej. /dev/ttyUSB0, COM3)
            baudrate: Velocidad de comunicación serial
            csv_path: Ruta a archivo CSV para modo simulación/lectura
            time_col: Nombre de la columna de tiempo
            device_cols: Nombres de columnas de dispositivos (por defecto: Dev 0-4)
        """
        self.port = port
        self.baudrate = baudrate
        self.csv_path = csv_path
        self.time_col = time_col
        self.device_cols = device_cols or [f"Dev {i}" for i in range(5)]

        self._serial: Optional[serial.Serial] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None

        self._df = pl.DataFrame()
        self._on_data_callback: Optional[Callable[[pl.DataFrame], None]] = None

    def connect(self) -> bool:
        """Establece conexión serial."""
        try:
            self._serial = serial.Serial(self.port, self.baudrate, timeout=1)
            time.sleep(2)  # Esperar a que Arduino se reinicie
            return True
        except serial.SerialException as e:
            print(f"Error conectando al puerto {self.port}: {e}")
            return False

    def disconnect(self):
        """Cierra conexión serial."""
        if self._serial and self._serial.is_open:
            self._serial.close()
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

    def _parse_serial_line(self, line: str) -> Optional[dict]:
        """Parsea una línea del puerto serial."""
        try:
            parts = line.strip().split(";")
            if len(parts) < len(self.device_cols) + 1:
                return None

            result = {self.time_col: parts[0]}
            for i, col in enumerate(self.device_cols):
                result[col] = float(parts[i + 1])

            return result
        except (ValueError, IndexError):
            return None

    def _read_serial_loop(self):
        """Bucle de lectura serial (se ejecuta en thread separado)."""
        self._df = pl.DataFrame(
            {self.time_col: [], **{col: [] for col in self.device_cols}}
        )

        while self._running:
            if self._serial and self._serial.in_waiting > 0:
                line = self._serial.readline().decode("utf-8", errors="ignore")
                data = self._parse_serial_line(line)

                if data:
                    new_row = pl.DataFrame([data])
                    self._df = pl.concat([self._df, new_row])

                    # Limitar a 10000 filas para no agotar memoria
                    if len(self._df) > 10000:
                        self._df = self._df.tail(10000)

                    if self._on_data_callback:
                        self._on_data_callback(self._df)

            time.sleep(0.1)

    def start_streaming(self):
        """Inicia streaming de datos en thread separado."""
        if not self._serial or not self._serial.is_open:
            if not self.connect():
                raise RuntimeError("No se pudo conectar al puerto serial")

        self._running = True
        self._thread = threading.Thread(target=self._read_serial_loop, daemon=True)
        self._thread.start()

    def read_csv_file(self, filepath: str) -> pl.DataFrame:
        """Lee un archivo CSV y procesa los datos."""
        df = pl.read_csv(
            filepath,
            separator=";",
            has_header=True,
            try_parse_dates=True,
        )

        # Convertir columnas de texto a float
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

        import random
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
        self._df = pl.DataFrame()
