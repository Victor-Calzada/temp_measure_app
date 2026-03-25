# Monitor de Temperatura en Tiempo Real

Dashboard para Raspberry Pi con visualización de temperaturas en tiempo real.

## Requisitos

- Raspberry Pi (3B+ o superior recomendado)
- Python 3.9+
- Dispositivo de medición de temperatura (Arduino, sensor, etc.) o archivo CSV

## Instalación

```bash
cd proyecto
pip install -r requirements.txt
```

## Ejecución

```bash
streamlit run streamlit_app.py --server.address 0.0.0.0 --server.port 8501
```

Accede desde cualquier navegador en `http://<IP-de-tu-pi>:8501`

## Estructura del Proyecto

```
proyecto/
├── streamlit_app.py      # Dashboard principal
├── data_acquisition.py   # Módulo de adquisición de datos
├── requirements.txt      # Dependencias
├── config.py            # Configuración
├── data/
│   └── Medida_nueva.txt # Archivo CSV de ejemplo
└── README.md
```

## Formato de Datos

### Archivo CSV
```
Time;Dev 0;Dev 1;Dev 2;Dev 3;Dev 4
16:59:25;22.062;22.375;23.312;22.187;22.125
17:00:10;22.062;22.375;23.375;22.125;22.062
```

## Configuración de Sensores 1-Wire (DS18B20) en Raspberry Pi

Para utilizar sensores de temperatura DS18B20 directamente en los pines GPIO de la Raspberry Pi, sigue estos pasos:

### 1. Conexión Física
- **VCC**: Pin 1 (3.3V)
- **GND**: Pin 6 (GND)
- **Data**: Pin 7 (GPIO4)
- **Resistencia**: Es necesario colocar una resistencia de **4.7kΩ** entre VCC y Data (Pull-up).

### 2. Habilitar Interfaz 1-Wire
Edita el archivo de configuración de arranque:
```bash
sudo nano /boot/config.txt
```
Añade la siguiente línea al final del archivo:
```text
dtoverlay=w1-gpio
```
*Nota: En versiones muy recientes de Raspberry Pi OS, el archivo puede estar en `/boot/firmware/config.txt`.*

Reinicia la Raspberry Pi:
```bash
sudo reboot
```

### 3. Verificar Sensores
Una vez reiniciado, los sensores deberían aparecer como directorios en el sistema de archivos:
```bash
ls /sys/bus/w1/devices/
```
Cada sensor tendrá un ID único (ej. `28-00000xxxxxxx`). Puedes leer la temperatura actual con:
```bash
cat /sys/bus/w1/devices/28-00000xxxxxxx/w1_slave
```

### 4. Integración con el Dashboard
Actualmente, el dashboard lee datos localmente a través de Python. Al iniciar la lectura, se examinará `/sys/bus/w1/devices/` para obtener los datos de los sensores conectados. No se necesita hardware externo adicional intermedio como un Arduino.

## Modo демо (sin hardware)

Selecciona "Simulación (CSV)" en el panel lateral y especifica la ruta al archivo CSV.

## Performance

- Memoria: ~50MB RAM con 10,000 puntos
- CPU: <5% en Raspberry Pi 3
- Actualización mínima: 1 segundo
