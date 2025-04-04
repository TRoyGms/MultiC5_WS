package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

// 🔵 Estructura del mensaje entrante desde MQTT
type SensorMessageInput struct {
	ID          int    `json:"id"`
	Title       string `json:"title"`
	Description int    `json:"description"`
	Emitter     string `json:"emitter"`
	Topic       string `json:"topic"`
	Serie       string `json:"serie"`         // <- importante
	CreatedAt   string `json:"createdAt"`
}

// 🟢 Estructura del mensaje que se envía al frontend
type SensorMessageOutput struct {
	ID          int    `json:"id"`
	Tittle      string `json:"tittle"`        // <- será igual a Emitter
	Description int    `json:"description"`
	Emitter     string `json:"emitter"`
	Topic       string `json:"topic"`
	Serie       string `json:"serie"`         // <- enviado al frontend
	CreatedAt   string `json:"created_at"`    // <- solo hora
}

// 🌐 WebSocket
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// 🔐 Clientes conectados con su `serie`
var clients = make(map[*websocket.Conn]string)
var mutex = sync.Mutex{}

// 🔁 MQTT handler
var mqttMessageHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	log.Println("--------------------------------------------------")
	log.Printf("📡 TOPIC RECIBIDO: %s", msg.Topic())
	log.Printf("🧾 Payload bruto: %s", string(msg.Payload()))

	if msg.Topic() != "notification" {
		log.Printf("📛 Mensaje ignorado (topic no es notification): %s", msg.Topic())
		return
	}

	// 🔄 Parsear JSON entrante
	var incoming SensorMessageInput
	err := json.Unmarshal(msg.Payload(), &incoming)
	if err != nil {
		log.Printf("❌ Error al parsear JSON entrante: %s", err)
		return
	}

	// ⏰ Extraer solo la hora
	createdAtTime := incoming.CreatedAt
	if strings.Contains(createdAtTime, " ") {
		parts := strings.Split(createdAtTime, " ")
		if len(parts) == 2 {
			createdAtTime = parts[1]
		}
	}

	// 🧾 Construir mensaje para frontend
	outgoing := SensorMessageOutput{
		ID:          incoming.ID,
		Tittle:      strings.TrimSpace(incoming.Emitter),
		Description: incoming.Description,
		Emitter:     strings.TrimSpace(incoming.Emitter),
		Topic:       incoming.Topic,
		Serie:       incoming.Serie,
		CreatedAt:   createdAtTime,
	}

	// 🔐 Enviar solo a los clientes con esa serie
	message, _ := json.Marshal(outgoing)

	mutex.Lock()
	for client, serieCliente := range clients {
		if serieCliente != incoming.Serie {
			continue
		}
		err := client.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			log.Printf("❌ Error enviando por WebSocket: %s", err)
			client.Close()
			delete(clients, client)
		}
	}
	mutex.Unlock()
}

// 🔌 Conexión al broker MQTT
func connectMQTT() mqtt.Client {
	broker := os.Getenv("MQTT_BROKER")
	clientID := os.Getenv("MQTT_CLIENT_ID") + "-" + fmt.Sprint(time.Now().Unix())
	username := os.Getenv("MQTT_USERNAME")
	password := os.Getenv("MQTT_PASSWORD")

	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(clientID).
		SetUsername(username).
		SetPassword(password).
		SetDefaultPublishHandler(mqttMessageHandler)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("❌ Error conectando a MQTT: %s", token.Error())
	}
	log.Println("✅ Conectado a MQTT correctamente")

	// Suscribirse a "notification"
	if token := client.Subscribe("notification", 0, nil); token.Wait() && token.Error() != nil {
		log.Printf("❌ Error al suscribirse al topic 'notification': %s", token.Error())
	} else {
		log.Println("📡 Suscrito al topic: notification")
	}

	return client
}

// 📲 Manejador WebSocket
func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("❌ Error al conectar WebSocket: %s", err)
		return
	}
	defer ws.Close()

	// 🕵️ Esperar mensaje de identificación
	_, msg, err := ws.ReadMessage()
	if err != nil {
		log.Printf("❌ Error leyendo la serie del cliente: %s", err)
		return
	}

	var identificacion struct {
		Tipo  string `json:"tipo"`
		Valor string `json:"valor"`
	}
	err = json.Unmarshal(msg, &identificacion)
	if err != nil || identificacion.Tipo != "serie" || identificacion.Valor == "" {
		log.Printf("❌ Cliente no envió su serie correctamente")
		return
	}

	mutex.Lock()
	clients[ws] = identificacion.Valor
	log.Printf("🟢 Cliente conectado con serie: %s", identificacion.Valor)
	mutex.Unlock()

	// 🧩 Mantener conexión activa
	for {
		if _, _, err := ws.ReadMessage(); err != nil {
			mutex.Lock()
			delete(clients, ws)
			log.Printf("🔴 Cliente desconectado")
			mutex.Unlock()
			break
		}
	}
}

// 🚀 Main
func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("❌ Error cargando el archivo .env")
	}

	mqttClient := connectMQTT()
	defer mqttClient.Disconnect(250)

	http.HandleFunc("/ws", handleConnections)
	log.Println("🌐 Servidor WebSocket escuchando en puerto 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
