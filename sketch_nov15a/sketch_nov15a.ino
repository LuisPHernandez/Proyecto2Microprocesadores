#define BLYNK_TEMPLATE_ID "TMPL2DST3r-Xc"
#define BLYNK_TEMPLATE_NAME "Proyecto02Microprocesadores"

#include <DHT.h>
#include <ESP8266WiFi.h>
#include <BlynkSimpleEsp8266.h>

#define DHTPIN D6
#define DHTTYPE DHT11
#define TRIG D4
#define ECHO D5

DHT dht(DHTPIN, DHTTYPE);

const int pirPin   = D1;
const int soundPin = A0;
const int motorPin = D7;
const int buzzerPin = D2;
const int ledPin = D0;
int v1_ledState = 0;
int v2_motorState = 0;
int v3_buzzerState = 0;

unsigned long lastDhtMillis = 0;
unsigned long lastPrintMillis = 0;

const unsigned long dhtInterval = 2000;
const unsigned long printInterval = 100;

float humidity = NAN;
float temperatureC = NAN;
int motion = 0;

char auth[] = "WDMTk4LR1zuQM_S7vXd2fjL7odvoTJqt";
char ssid[] = "WiFiFoFum";
char pass[] = "3H1HLR7ACBAA";

// ======================================================
// BLYNK
// ======================================================

// Luz
BLYNK_WRITE(V1) {
  int val = param.asInt();
  digitalWrite(ledPin, val);
  v1_ledState = val;
}

// Motor
BLYNK_WRITE(V2) {
  int val = param.asInt();
  digitalWrite(motorPin, val);
  v2_motorState = val;
}

// Buzzer
BLYNK_WRITE(V3) {
  int val = param.asInt();
  digitalWrite(buzzerPin, val);
  v3_buzzerState = val;
}

// ======================================================
// SETUP
// ======================================================

void setup() {
  Serial.begin(115200);

  // Sensores
  dht.begin();
  pinMode(pirPin, INPUT_PULLUP);
  pinMode(TRIG, OUTPUT);
  pinMode(ECHO, INPUT);

  // Actuadores
  pinMode(motorPin, OUTPUT);
  pinMode(buzzerPin, OUTPUT);
  pinMode(ledPin, OUTPUT);

  digitalWrite(TRIG, LOW);      // trigger del sensor ultrasónico apagado al inicio
  digitalWrite(motorPin, LOW);  // motor apagado al inicio
  digitalWrite(buzzerPin, LOW); // buzzer apagado al inicio
  digitalWrite(ledPin, LOW);    // LED apagado al inicio

  // Conectar a WiFi + Blynk
  Blynk.begin(auth, ssid, pass);
}

// ======================================================
// LOOP
// ======================================================

void loop() {
  Blynk.run();

  unsigned long now = millis();
  long duration, distance;

  // Leer sonido
  int sound = analogRead(soundPin);

  // Leer PIR
  motion = digitalRead(pirPin);

  // Sensor ultrasónico
  digitalWrite(TRIG, LOW);
  delayMicroseconds(2);
  digitalWrite(TRIG, HIGH);
  delayMicroseconds(10);
  digitalWrite(TRIG, LOW);

  duration = pulseIn(ECHO, HIGH, 30000);
  if (duration == 0) {
    distance = -1;
  } else {
    distance = duration * 0.034 / 2;
  }

  // Leer DHT11 cada 2s
  if (now - lastDhtMillis >= dhtInterval) {
    lastDhtMillis = now;
    float h = dht.readHumidity();
    float t = dht.readTemperature();

    if (!isnan(h) && !isnan(t)) {
      humidity = h;
      temperatureC = t;
    } else {
      Serial.println("Error leyendo datos del DHT11");
    }
  }

  // Enviar al Serial cada 0.1s
  if (now - lastPrintMillis >= printInterval) {
    lastPrintMillis = now;

    Serial.print("sound:");
    Serial.print(sound);
    Serial.print(" motion:");
    Serial.print(motion);
    Serial.print(" temp:");
    Serial.print(temperatureC);
    Serial.print(" hum:");
    Serial.print(humidity);
    Serial.print(" dist: ");
    if (distance < 0) {
      Serial.println("sin eco");
    } else {
      Serial.print(distance);
      Serial.println(" cm");
    }
  }

  // Lógica del motor
  if (v2_motorState == 0) { // Solo si Blynk no lo tiene encendido
    if (temperatureC > 25 || humidity > 85) {
      digitalWrite(motorPin, HIGH);
    } else {
      digitalWrite(motorPin, LOW);
    }
  }

  // Lógica del Buzzer
  if (v3_buzzerState == 0) { // Solo si Blynk no lo tiene encendido
    if (motion == 1 || sound > 95) {
      digitalWrite(buzzerPin, HIGH);
    } else {
      digitalWrite(buzzerPin, LOW);
    }
  }

  // Lógica del LED
  if (v1_ledState == 0) { 
    if (distance > 0 && distance < 20) {
      digitalWrite(ledPin, HIGH); // Sensor enciende el LED
    } else {
      digitalWrite(ledPin, LOW);  // Sensor apaga el LED
    }
  }
}