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

### Puerto Serial
Enviar datos en formato: `DEV0;DEV1;DEV2;DEV3;DEV4;TIME`
Ejemplo: `22.062;22.375;23.312;22.187;22.125;16:59:25`

## Configuración Serial (Arduino)

Ejemplo básico para Arduino:
```cpp
void setup() {
  Serial.begin(9600);
}

void loop() {
  float dev0 = leerSensor(0);
  float dev1 = leerSensor(1);
  float dev2 = leerSensor(2);
  float dev3 = leerSensor(3);
  float dev4 = leerSensor(4);

  Serial.print(dev0);
  Serial.print(";");
  Serial.print(dev1);
  Serial.print(";");
  Serial.print(dev2);
  Serial.print(";");
  Serial.print(dev3);
  Serial.print(";");
  Serial.print(dev4);
  Serial.println();

  delay(5000);
}
```

## Modo демо (sin hardware)

Selecciona "Simulación (CSV)" en el panel lateral y especifica la ruta al archivo CSV.

## Performance

- Memoria: ~50MB RAM con 10,000 puntos
- CPU: <5% en Raspberry Pi 3
- Actualización mínima: 1 segundo
