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
    
    # Detectar sensores reales al inicio
    w1_sensors = sorted(glob.glob(f"{W1_DEVICES_DIR}28-*/w1_slave"))
    use_cpu = False
    
    if len(w1_sensors) > 0:
        device_cols = [f"Dev {i}" for i in range(len(w1_sensors))]
    else:
        cpu_temp = get_cpu_temp()
        if cpu_temp is not None:
            device_cols = ["Dev 0"]
            use_cpu = True
        else:
            print("Error: No se detectaron sensores físicos (1-Wire o CPU). Abortando.")
            return

    header = ["Time"] + device_cols

    print(f"Iniciando registro en: {filename}")
    print(f"Muestreo: cada {args.interval}s | Duración: {args.duration}h")
    print(f"Sensores detectados: {len(device_cols)} ({'CPU' if use_cpu else '1-Wire DS18B20'})")
    
    start_time = datetime.now()
    end_time = start_time + timedelta(hours=args.duration)

    try:
        with open(filename, mode='w', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(header)
            
            while datetime.now() < end_time:
                now = datetime.now()
                now_str = now.strftime("%H:%M:%S")
                
                row = [now_str]
                
                if use_cpu:
                    temp = get_cpu_temp()
                    row.append(f"{temp:.3f}" if temp is not None else "")
                else:
                    for sensor_file in w1_sensors:
                        temp = read_ds18b20(sensor_file)
                        row.append(f"{temp:.3f}" if temp is not None else "")
                
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
