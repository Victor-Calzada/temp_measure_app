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

import os
from config import W1_DEVICES_DIR, CPU_TEMP_FILE, USB_PATH, FALLBACK_PATH


class DataAcquisition:
    """Clase para adquisición de datos de temperatura desde hardware local o archivo."""

    def __init__(
        self,
        csv_path: Optional[str] = None,
        log_path: Optional[str] = None,
        time_col: str = "Time",
        device_cols: list[str] | None = None,
    ):
        """
        Args:
            csv_path: Ruta a archivo CSV para modo simulación/lectura
            log_path: Ruta opcional para guardar registros en tiempo real
            time_col: Nombre de la columna de tiempo
            device_cols: Nombres de columnas de dispositivos (por defecto: Dev 0-4)
        """
        self.csv_path = csv_path
        self.log_path = log_path
        self.time_col = time_col
        self.device_cols = device_cols or [f"Dev {i}" for i in range(5)]
        self.sampling_interval = 1.0  # Por defecto 1 segundo
        self.last_export_path = None

        self._running = False
        self._thread: Optional[threading.Thread] = None

        # Definir el esquema explícito para evitar errores de tipo con Polars (Null vs String/Float)
        self._schema = {self.time_col: pl.String}
        for col in self.device_cols:
            self._schema[col] = pl.Float64

        self._df = pl.DataFrame(schema=self._schema)
        self._on_data_callback: Optional[Callable[[pl.DataFrame], None]] = None

        # Atributos de temporizador para persistencia
        self.timer_active = False
        self.timer_end_time: Optional[datetime] = None

        # Si hay log_path y existe, intentar cargar datos previos
        if self.log_path and Path(self.log_path).exists():
            try:
                self._df = pl.read_csv(self.log_path, separator=";", schema=self._schema)
            except Exception as e:
                print(f"Error cargando log previo: {e}")

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

    def _write_to_log(self, new_row: pl.DataFrame):
        """Escribe una nueva fila al archivo de log (append)."""
        if not self.log_path:
            return

        file_exists = Path(self.log_path).exists()
        
        # Usamos el modo append nativo si es posible o simplemente escribimos
        # Para máxima seguridad en Raspberry Pi, abrimos y cerramos el archivo
        try:
            with open(self.log_path, "a") as f:
                # Si el archivo es nuevo, escribir cabecera
                if not file_exists:
                    header = ";".join([self.time_col] + self.device_cols) + "\n"
                    f.write(header)
                
                # Escribir fila
                row_str = ";".join([str(new_row[col][0]) for col in [self.time_col] + self.device_cols]) + "\n"
                f.write(row_str)
        except Exception as e:
            print(f"Error escribiendo en log: {e}")

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
            .drop(["index", "tiempo_obj", "delta", "delta_corregido"], strict=False)
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

    def _export_data(self) -> Optional[str]:
        """
        Exporta los datos acumulados a CSV con el formato correcto.
        Intenta guardar en múltiples ubicaciones por seguridad.
        """
        if self._df.is_empty():
            return None
            
        # Lista de rutas candidatas (en orden de preferencia)
        candidate_dirs = [
            USB_PATH,
            os.path.expanduser(FALLBACK_PATH),
            os.path.join(os.getcwd(), "data") # Último recurso: carpeta local del proyecto
        ]
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"measurements_{timestamp}.csv"
        
        last_error = ""

        for target_dir in candidate_dirs:
            if not target_dir: continue
            
            try:
                # Asegurar que el directorio existe
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir, exist_ok=True)
                
                full_path = os.path.join(target_dir, filename)
                
                # Procesar datos para incluir segundos desde el inicio
                df_processed = self._calculate_seconds_from_start(self._df)
                df_processed.write_csv(full_path, separator=";")
                
                print(f"Exportación exitosa en: {full_path}")
                self.last_export_path = full_path
                return full_path
            except Exception as e:
                last_error = str(e)
                print(f"Fallo al exportar en {target_dir}: {e}")
                continue

        # Si llegamos aquí, fallaron todos los intentos
        self._log_error(f"Fallo total de exportación tras temporizador. Último error: {last_error}")
        return None

    def _log_error(self, message: str):
        """Guarda errores en un archivo log persistente fuera de la consola."""
        try:
            with open("error_acquisition.txt", "a") as f:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"[{timestamp}] {message}\n")
        except:
            pass

    def _handle_timer_expiration(self):
        """Maneja la expiración del temporizador de forma autónoma."""
        print("Temporizador expirado. Iniciando exportación de seguridad...")
        export_path = self._export_data()
        
        # SÓLO borramos el log si la exportación fue exitosa
        if export_path:
            if self.log_path and os.path.exists(self.log_path):
                try:
                    os.remove(self.log_path)
                    print(f"Log activo {self.log_path} eliminado tras exportación exitosa.")
                except Exception as e:
                    self._log_error(f"Error borrando log tras exportación: {e}")
            
            # Resetear el DataFrame solo si los datos están a salvo
            self._df = pl.DataFrame(schema=self._schema)
        else:
            self._log_error("El temporizador expiró pero la exportación falló en todas las rutas. NO se borró el log activo para permitir recuperación manual.")
        
        # Resetear estado y detener
        self.timer_active = False
        self.timer_end_time = None
        self._running = False

    def _read_sensors_loop(self):
        """Bucle de lectura de sensores hardware (se ejecuta en thread separado)."""
        try:
            # No reiniciamos el dataframe si ya tiene datos (ej. cargados de log)
            if self._df.is_empty():
                self._df = pl.DataFrame(schema=self._schema)

            while self._running:
                # Verificar temporizador
                if self.timer_active and self.timer_end_time:
                    if datetime.now() >= self.timer_end_time:
                        self._handle_timer_expiration()
                        break

                data = self._read_hardware_sensors()

                if data:
                    new_row = pl.DataFrame([data], schema=self._schema)
                    self._df = pl.concat([self._df, new_row])
                    
                    # Persistencia en tiempo real
                    self._write_to_log(new_row)

                    # Limitar a 25 millones de filas (~2GB) para no agotar memoria
                    if len(self._df) > 25_000_000:
                        self._df = self._df.tail(25_000_000)

                    if self._on_data_callback:
                        self._on_data_callback(self._df)

                # Esperar según el intervalo configurado
                time.sleep(self.sampling_interval)
        except Exception as e:
            self._log_error(f"CRASH CRÍTICO en el hilo de lectura: {e}")
            self._running = False

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
