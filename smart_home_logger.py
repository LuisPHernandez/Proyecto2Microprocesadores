import serial
import csv
import re
import time
import json
import subprocess
import threading
from datetime import datetime
import blynklib

# ==========================================================
# CONFIGURACIÓN GENERAL
# ==========================================================

BLYNK_TOKEN = "WDMTk4LR1zuQM_S7vXd2fjL7odvoTJqt"

PORT = "COM4"
BAUD = 115200

INTERVALO_REGISTRO = 2.0
VENTANA_SIZE = 90

CSV_FILENAME = "smart_home_historico.csv"
BUFFER_FILENAME = "ventana.json"

CUDA_EXEC = "cuda_app.exe"

# Regex para leer datos del ESP
pattern = re.compile(
    r"sound:(?P<sound>\d+)\s+motion:(?P<motion>\d+)\s+temp:(?P<temp>-?\d+\.?\d*)\s+hum:(?P<hum>-?\d+\.?\d*)\s+dist[: ]+(?P<dist>sin eco|[-]?\d+)"
)

# Conexión Blynk
blynk = blynklib.Blynk(BLYNK_TOKEN)

# Estados leídos desde Blynk
estado_actual = {
    "LED": 0,
    "MOTOR": 0,
    "BUZZER": 0
}

# ==========================================================
# LEER ESTADOS DESDE BLYNK PARA PODER TOGGLEARLOS
# ==========================================================

@blynk.handle_event("read V1")
def leer_led_virtual(pin):
    blynk.virtual_write(1, estado_actual["LED"])

@blynk.handle_event("write V1")
def actualizar_led(pin, valores):
    estado_actual["LED"] = int(valores[0])

@blynk.handle_event("read V2")
def leer_motor_virtual(pin):
    blynk.virtual_write(2, estado_actual["MOTOR"])

@blynk.handle_event("write V2")
def actualizar_motor(pin, valores):
    estado_actual["MOTOR"] = int(valores[0])

@blynk.handle_event("read V3")
def leer_buzzer_virtual(pin):
    blynk.virtual_write(3, estado_actual["BUZZER"])

@blynk.handle_event("write V3")
def actualizar_buzzer(pin, valores):
    estado_actual["BUZZER"] = int(valores[0])

# ==========================================================
# MANEJAR ACCIONES DEVUELTAS POR CUDA
# ==========================================================

def manejar_accion_de_cuda(salida):
    accion = salida.strip()

    # TOGGLE LED
    if accion == "TOGGLE_LED":
        nuevo = 1 - estado_actual["LED"]
        estado_actual["LED"] = nuevo
        blynk.virtual_write(1, nuevo)

    # TOGGLE MOTOR
    elif accion == "TOGGLE_MOTOR":
        nuevo = 1 - estado_actual["MOTOR"]
        estado_actual["MOTOR"] = nuevo
        blynk.virtual_write(2, nuevo)

    # TOGGLE BUZZER
    elif accion == "TOGGLE_BUZZER":
        nuevo = 1 - estado_actual["BUZZER"]
        estado_actual["BUZZER"] = nuevo
        blynk.virtual_write(3, nuevo)

    else:
        print("CUDA devolvió una acción desconocida:", accion)

# ==========================================================
# BLYNK — EVENTO DE TEXTO DEL USUARIO
# ==========================================================

@blynk.handle_event("write V0")
def recibir_comando_usuario(pin, valores):
    texto = valores[0]
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
            print("\n=== CUDA WINDOW STATS ===\n")
            print(salida)

            # PARSEAR ESTADÍSTICAS Y ENVIARLAS A BLYNK
            for line in salida.split("\n"):
                line = line.strip()

                # temp: mean=... std=...
                if line.startswith("temp:"):
                    parts = line.split()
                    mean_t = float(parts[1].split("=")[1])
                    std_t = float(parts[2].split("=")[1])
                    print(f"[DEBUG] Enviando Temp: {mean_t}, {std_t}")
                    blynk.virtual_write(10, mean_t)
                    blynk.virtual_write(11, std_t)
                    time.sleep(0.2)

                # hum: mean=... std=...
                if line.startswith("hum:"):
                    parts = line.split()
                    mean_h = float(parts[1].split("=")[1])
                    std_h = float(parts[2].split("=")[1])
                    print(f"[DEBUG] Enviando Hum: {mean_h}, {std_h}")
                    blynk.virtual_write(12, mean_h)
                    blynk.virtual_write(13, std_h)
                    time.sleep(0.2)

                # sound: mean=... max=...
                if line.startswith("sound:"):
                    parts = line.split()
                    mean_s = float(parts[1].split("=")[1])
                    max_s = float(parts[2].split("=")[1])
                    print(f"[DEBUG] Enviando Sound: {mean_s}, {max_s}")
                    blynk.virtual_write(14, mean_s)
                    blynk.virtual_write(15, max_s)
                    time.sleep(0.2)

                # motion: count=... 
                if line.startswith("motion:"):
                    parts = line.split()
                    count_m = float(parts[1].split("=")[1])
                    time_m = count_m * INTERVALO_REGISTRO 
                    print(f"[DEBUG] Enviando Motion: {time_m}")
                    blynk.virtual_write(16, time_m)
                    time.sleep(0.2)

                # dist: count=...
                if line.startswith("dist:"):
                    parts = line.split()
                    count_d = float(parts[1].split("=")[1])
                    time_d = count_d * INTERVALO_REGISTRO
                    print(f"[DEBUG] Enviando Dist: {time_d}")
                    blynk.virtual_write(17, time_d)
                    time.sleep(0.2)

        except Exception as e:
            print("ERROR ejecutando cuda --window:", e)

# ==========================================================
# MAIN
# ==========================================================

print("\n=== Smart Home – Python + CUDA + Blynk ===")

t1 = threading.Thread(target=hilo_serial, daemon=True)
t2 = threading.Thread(target=hilo_stat_cuda, daemon=True)

t1.start()
t2.start()

while True:
    blynk.run()
    time.sleep(0.01)