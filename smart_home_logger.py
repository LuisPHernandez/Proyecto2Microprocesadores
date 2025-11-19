import serial
import csv
import re
import time
import json
import subprocess
import threading
from datetime import datetime
import paho.mqtt.client as mqtt

# ==========================================================
# CONFIGURACIÓN GENERAL
# ==========================================================

TOKEN = "WDMTk4LR1zuQM_S7vXd2fjL7odvoTJqt"
BLYNK_HOST = "ny3.blynk.cloud"

client = mqtt.Client()                      
client.username_pw_set("device", TOKEN)     

PORT = "COM4"
BAUD = 115200

INTERVALO_REGISTRO = 2.0
VENTANA_SIZE = 90

CSV_FILENAME = "smart_home_historico.csv"
CSV_PROCESADO_FILENAME = "smart_home_procesado.csv"  # NUEVO: CSV con datos procesados de CUDA
BUFFER_FILENAME = "ventana.json"

CUDA_EXEC = "cuda_app.exe"

# Regex para leer datos del ESP
pattern = re.compile(
    r"sound:(?P<sound>\d+)\s+motion:(?P<motion>\d+)\s+temp:(?P<temp>-?\d+\.?\d*)\s+hum:(?P<hum>-?\d+\.?\d*)\s+dist[: ]+(?P<dist>sin eco|[-]?\d+)"
)

# Estados leídos desde Blynk
estado_actual = {
    "LED": 0,
    "MOTOR": 0,
    "BUZZER": 0
}

# ==========================================================
# helpers MQTT para datastreams
# ==========================================================

DATASTREAM_BY_PIN = {
    "V0":  "NLU input",
    "V1":  "LED control",
    "V2":  "Motor control",
    "V3":  "Buzzer control",   
    "V10": "Temp media",
    "V11": "Temp stdDev",   
    "V12": "Hum media",
    "V13": "Hum stdDev",
    "V14": "Sonido avg",
    "V15": "Sonido max",
    "V16": "Tiempo mov",      
    "V17": "Tiempo luz"        
}

def mqtt_enviar_datastream(pin, valor):
    ds_name = DATASTREAM_BY_PIN.get(pin, pin) 
    topic = f"ds/{ds_name}"                    
    payload = str(valor)
    print(f"[MQTT] publish -> {topic} = {payload}")
    client.publish(topic, payload)

# ==========================================================
# CALLBACKS MQTT
# ==========================================================

def on_connect(client_m, userdata, flags, rc, properties=None):
    print(f"[MQTT] Conectado a blynk.cloud, rc={rc}")
    client_m.subscribe("downlink/ds/#")
    client_m.publish("get/ds", "LED control,Motor control,Buzzer control,NLU input")

def on_message(client_m, userdata, msg):     # se llama cuando llega un mensaje de Blynk
    topic = msg.topic
    payload = msg.payload.decode(errors="ignore")
    if topic.endswith("/LED control"):
        # Equivalente a write V1 en blynklib (actualizar_led)
        try:
            estado_actual["LED"] = int(payload)
        except ValueError:
            pass
    elif topic.endswith("/Motor control"):
        # Equivalente a write V2 (actualizar_motor)
        try:
            estado_actual["MOTOR"] = int(payload)
        except ValueError:
            pass
    elif topic.endswith("/Buzzer control"):
        # Equivalente a write V3 (actualizar_buzzer)
        try:
            estado_actual["BUZZER"] = int(payload)
        except ValueError:
            pass
    elif topic.endswith("/NLU input"):
        # Equivalente a write V0 (recibir_comando_usuario)
        recibir_comando_usuario(payload) 

client.on_connect = on_connect
client.on_message = on_message 

# ==========================================================
# MANEJAR ACCIONES DEVUELTAS POR CUDA
# ==========================================================

def manejar_accion_de_cuda(salida):
    accion = salida.strip()

    # TOGGLE LED
    if accion == "TOGGLE_LED":
        nuevo = 1 - estado_actual["LED"]
        estado_actual["LED"] = nuevo
        mqtt_enviar_datastream("V1", nuevo)  

    # TOGGLE MOTOR
    elif accion == "TOGGLE_MOTOR":
        nuevo = 1 - estado_actual["MOTOR"]
        estado_actual["MOTOR"] = nuevo
        mqtt_enviar_datastream("V2", nuevo) 

    # TOGGLE BUZZER
    elif accion == "TOGGLE_BUZZER":
        nuevo = 1 - estado_actual["BUZZER"]
        estado_actual["BUZZER"] = nuevo
        mqtt_enviar_datastream("V3", nuevo)  

    else:
        print("CUDA devolvió una acción desconocida:", accion)

# ==========================================================
# BLYNK — EVENTO DE TEXTO DEL USUARIO
# ==========================================================

def recibir_comando_usuario(texto):
    print("\n>>> Comando recibido desde Blynk:", texto)

    # Guardar ventana actual para enviarla a CUDA
    with ventana_lock:
        with open(BUFFER_FILENAME, "w") as jf:
            json.dump(ventana, jf)

    cmd = [
        CUDA_EXEC,
        "--cmd",
        BUFFER_FILENAME,
        texto,
    ]

    try:
        salida = subprocess.check_output(cmd, universal_newlines=True)
        salida = salida.strip()
        print("CUDA dijo:", salida)
        manejar_accion_de_cuda(salida)
    except Exception as e:
        print("ERROR ejecutando cuda --cmd:", e)

# ==========================================================
# BUFFER DE VENTANA (3 MINUTOS)
# ==========================================================

ventana = []
csv_lock = threading.Lock()
ventana_lock = threading.Lock()

# ==========================================================
# HILO SERIAL: REGISTRO + VENTANA + CSV
# ==========================================================

def hilo_serial():
    global ventana

    ser = serial.Serial(PORT, BAUD, timeout=0.1)

    # Crear CSV si no existe
    try:
        open(CSV_FILENAME, "r")
        existe = True
    except:
        existe = False

    csv_file = open(CSV_FILENAME, "a", newline="", encoding="utf-8")
    writer = csv.writer(csv_file)

    if not existe:
        writer.writerow(["datetime", "sound_avg", "motion", "temp", "hum", "dist"])

    ultimo_registro = time.time()

    sonido_acumulado = []
    motion_acumulada = []
    dist_acumulada = []

    last_motion = 0
    last_temp = 0.0
    last_hum = 0.0
    last_dist = -1
    last_sound_raw = 0

    while True:
        line = ser.readline().decode(errors="ignore").strip()

        if line:
            m = pattern.search(line)
            if m:
                sound_raw = int(m.group("sound"))
                motion_raw = int(m.group("motion"))
                temp_raw = float(m.group("temp"))
                hum_raw = float(m.group("hum"))
                dist_raw = m.group("dist")
                dist = -1 if dist_raw == "sin eco" else int(dist_raw)

                sonido_acumulado.append(sound_raw)
                motion_acumulada.append(motion_raw)
                if dist > 0:
                    dist_acumulada.append(dist)

                last_temp = temp_raw
                last_hum = hum_raw
                
                last_sound_raw = sound_raw
                last_motion = motion_raw
                last_dist = dist

        if time.time() - ultimo_registro >= INTERVALO_REGISTRO:
            ultimo_registro = time.time()

            # calcular sound_avg
            if len(sonido_acumulado) > 0:
                sound_avg = sum(sonido_acumulado) / len(sonido_acumulado)
            else:
                sound_avg = last_sound_raw

            # calcular motion, promedio y redondeo
            if len(motion_acumulada) > 0:
                motion_avg = sum(motion_acumulada) / len(motion_acumulada)
                motion_final = 1 if motion_avg >= 0.5 else 0
            else:
                motion_final = last_motion

            # calcular dist promedio
            if len(dist_acumulada) > 0:
                dist_final = sum(dist_acumulada) / len(dist_acumulada)
            else:
                dist_final = last_dist

            temp_final = last_temp
            hum_final = last_hum

            # Resetear acumuladores
            sonido_acumulado = []
            motion_acumulada = []
            dist_acumulada = []

            timestamp = datetime.now().isoformat()

            # Guardar CSV
            with csv_lock:
                writer.writerow([timestamp, sound_avg, motion_final, temp_final, hum_final, dist_final])
            csv_file.flush()

            # Guardar ventana
            registro = {
                "timestamp": timestamp,
                "sound_avg": sound_avg,
                "motion": motion_final,
                "temp": temp_final,
                "hum": hum_final,
                "dist": dist_final
            }

            with ventana_lock:
                ventana.append(registro)
                if len(ventana) > VENTANA_SIZE:
                    ventana.pop(0)

            print(f"[{timestamp}] Registro añadido — {len(ventana)}/{VENTANA_SIZE}")

# ==========================================================
# HILO CUDA: ESTADÍSTICAS CADA 3 MINUTOS
# ==========================================================

def hilo_stat_cuda():
    # Intervalo de cálculo = 90 registros * 2.0 seg/registro = 180 seg
    INTERVALO_CALCULO_CUDA = VENTANA_SIZE * INTERVALO_REGISTRO
    
    # Iniciar el temporizador
    ultimo_calculo = time.time() - INTERVALO_CALCULO_CUDA + 10 # Permitir un primer cálculo rápido
    
    print(f"[CUDA] Hilo de estadísticas iniciado. Intervalo: {INTERVALO_CALCULO_CUDA} seg.")

    try:
        open(CSV_PROCESADO_FILENAME, "r")
        existe_proc = True
    except:
        existe_proc = False

    csv_proc_file = open(CSV_PROCESADO_FILENAME, "a", newline="", encoding="utf-8")
    writer_proc = csv.writer(csv_proc_file)

    if not existe_proc:
        # Cabecera para datos procesados por CUDA
        writer_proc.writerow([
            "datetime",
            "temp_mean", "temp_std",
            "hum_mean", "hum_std",
            "sound_mean", "sound_max",
            "motion_time", "dist_time"
        ])

    while True:
        ahora = time.time()
        
        # Esperar a que pasen 3 minutos desde el último cálculo
        if (ahora - ultimo_calculo) < INTERVALO_CALCULO_CUDA:
            time.sleep(1) # Revisar cada segundo si ya pasó el tiempo
            continue
            
        # Han pasado 3 minutos, reiniciar el temporizador 
        ultimo_calculo = ahora

        print("\n[CUDA] Iniciando cálculo de estadísticas de 3 minutos...")

        # Verificar si la ventana no está llena
        with ventana_lock:
            if len(ventana) < VENTANA_SIZE:
                print("[CUDA] ... Ventana no está llena. Omitiendo cálculo.")
                continue

            # La ventana está llena, copiarla para el análisis
            with open(BUFFER_FILENAME, "w") as jf:
                json.dump(ventana, jf)

        try:
            salida = subprocess.check_output(
                [CUDA_EXEC, "--window", BUFFER_FILENAME],
                universal_newlines=True
            )
            print(salida)

            temp_mean = temp_std = None
            hum_mean = hum_std = None
            sound_mean = sound_max = None
            motion_time = None
            dist_time = None

            # PARSEAR ESTADÍSTICAS Y ENVIARLAS A BLYNK (ahora MQTT)
            for line in salida.split("\n"):
                line = line.strip()

                # temp: mean=... std=...
                if line.startswith("temp:"):
                    parts = line.split()
                    mean_t = float(parts[1].split("=")[1])
                    std_t = float(parts[2].split("=")[1])
                    print(f"[DEBUG] Enviando Temp: {mean_t}, {std_t}")
                    mqtt_enviar_datastream("V10", mean_t)
                    mqtt_enviar_datastream("V11", std_t)
                    temp_mean = mean_t
                    temp_std = std_t
                    time.sleep(0.2)

                # hum: mean=... std=...
                if line.startswith("hum:"):
                    parts = line.split()
                    mean_h = float(parts[1].split("=")[1])
                    std_h = float(parts[2].split("=")[1])
                    print(f"[DEBUG] Enviando Hum: {mean_h}, {std_h}")
                    mqtt_enviar_datastream("V12", mean_h)
                    mqtt_enviar_datastream("V13", std_h)
                    hum_mean = mean_h
                    hum_std = std_h
                    time.sleep(0.2)

                # sound: mean=... max=...
                if line.startswith("sound:"):
                    parts = line.split()
                    mean_s = float(parts[1].split("=")[1])
                    max_s = float(parts[2].split("=")[1])
                    print(f"[DEBUG] Enviando Sound: {mean_s}, {max_s}")
                    mqtt_enviar_datastream("V14", mean_s)
                    mqtt_enviar_datastream("V15", max_s)
                    sound_mean = mean_s
                    sound_max = max_s
                    time.sleep(0.2)

                # motion: count=... 
                if line.startswith("motion:"):
                    parts = line.split()
                    count_m = float(parts[1].split("=")[1])
                    time_m = count_m * INTERVALO_REGISTRO 
                    print(f"[DEBUG] Enviando Motion: {time_m}")
                    mqtt_enviar_datastream("V16", time_m)
                    motion_time = time_m
                    time.sleep(0.2)

                # dist: count=...
                if line.startswith("dist:"):
                    parts = line.split()
                    count_d = float(parts[1].split("=")[1])
                    time_d = count_d * INTERVALO_REGISTRO
                    print(f"[DEBUG] Enviando Dist: {time_d}")
                    mqtt_enviar_datastream("V17", time_d)
                    dist_time = time_d
                    time.sleep(0.2)

            if temp_mean is not None or hum_mean is not None or sound_mean is not None:
                ts_proc = datetime.now().isoformat()
                writer_proc.writerow([
                    ts_proc,
                    temp_mean, temp_std,
                    hum_mean, hum_std,
                    sound_mean, sound_max,
                    motion_time, dist_time
                ])
                csv_proc_file.flush()
                print(f"[CUDA] Fila procesada guardada en {CSV_PROCESADO_FILENAME}")
        except Exception as e:
            print("ERROR ejecutando cuda --window:", e)

# ==========================================================
# MAIN
# ==========================================================

print("\n=== Smart Home – Python + CUDA + Blynk (MQTT) ===") 

# conectar al broker de Blynk antes de lanzar los hilos
client.connect(BLYNK_HOST, 1883, 60)
client.loop_start()

t1 = threading.Thread(target=hilo_serial, daemon=True)
t2 = threading.Thread(target=hilo_stat_cuda, daemon=True)

t1.start()
t2.start()

while True:
    time.sleep(1)