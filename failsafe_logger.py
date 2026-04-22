import os
import glob
import time
import csv
import argparse
import random
from datetime import datetime, timedelta

# Configuración de hardware (espejo de la lógica original)
W1_DEVICES_DIR = "/sys/bus/w1/devices/"
CPU_TEMP_FILE = "/sys/class/thermal/thermal_zone0/temp"
DATA_DIR = "data"

def get_cpu_temp():
    """Lee la temperatura de la CPU como fallback."""
    try:
        with open(CPU_TEMP_FILE, "r") as f:
            return float(f.read().strip()) / 1000.0
    except:
        return None

def read_ds18b20(device_file):
    """Lee un sensor físico 1-Wire."""
    try:
        with open(device_file, "r") as f:
            lines = f.readlines()
            if not lines or "YES" not in lines[0]:
                return None
            temp_str = lines[1].split("t=")[1]
            return float(temp_str) / 1000.0
    except:
        return None

def main():
    parser = argparse.ArgumentParser(description="Logger de temperatura de seguridad (Failsafe)")
    parser.add_argument("-i", "--interval", type=int, default=60, help="Intervalo de muestreo en segundos (def: 60)")
    parser.add_argument("-d", "--duration", type=float, default=24.0, help="Duración del ensayo en horas (def: 24)")
    args = parser.parse_args()

    # Preparar archivo de salida
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(DATA_DIR, f"failsafe_datosdeldia_{timestamp}.csv")
    
    device_cols = [f"Dev {i}" for i in range(5)]
    header = ["Time"] + device_cols

    print(f"Iniciando registro en: {filename}")
    print(f"Muestreo: cada {args.interval}s | Duración: {args.duration}h")
    
    start_time = datetime.now()
    end_time = start_time + timedelta(hours=args.duration)

    try:
        with open(filename, mode='w', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(header)
            
            while datetime.now() < end_time:
                now = datetime.now()
                now_str = now.strftime("%H:%M:%S")
                
                # Lógica de adquisición idéntica a streamlit_app
                w1_sensors = sorted(glob.glob(f"{W1_DEVICES_DIR}28-*/w1_slave"))
                row = [now_str]
                
                for i in range(5):
                    temp = None
                    if i < len(w1_sensors):
                        temp = read_ds18b20(w1_sensors[i])
                    elif i == 0 and len(w1_sensors) == 0:
                        temp = get_cpu_temp()
                    
                    if temp is None:
                        # Simulación si no hay hardware (mismo comportamiento que app original)
                        base = 22.0 + (i * 0.5)
                        temp = base + random.uniform(-0.5, 0.5)
                    
                    row.append(f"{temp:.3f}")
                
                writer.writerow(row)
                f.flush() # Asegurar que se escribe en disco
                
                # Calcular tiempo restante y mostrar progreso simple
                remaining = end_time - datetime.now()
                print(f"[{now_str}] Grabado. Restan: {str(remaining).split('.')[0]}", end='\r')
                
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n\nRegistro detenido por el usuario.")
    except Exception as e:
        print(f"\n\nError crítico: {e}")
    finally:
        print(f"\nProceso finalizado. Datos guardados en {filename}")

if __name__ == "__main__":
    main()
