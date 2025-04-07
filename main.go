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
	Serie       string `json:"serie"`
	CreatedAt   string `json:"createdAt"`
}

// 🟢 Estructura del mensaje que se envía al frontend
type SensorMessageOutput struct {
	ID          int    `json:"id"`
	Tittle      string `json:"tittle"` // <- será igual a Emitter
	Description int    `json:"description"`
	Emitter     string `json:"emitter"`
	Topic       string `json:"topic"`
	Serie       string `json:"serie"`
	CreatedAt   string `json:"created_at"` // <- solo hora
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

	topic := msg.Topic()
	if !strings.Contains(topic, "notification") && !strings.Contains(topic, "alert") {
		log.Printf("📛 Topic ignorado: %s", topic)
		return
	}

	var incoming map[string]interface{}
	if err := json.Unmarshal(msg.Payload(), &incoming); err != nil {
		log.Printf("❌ Error al parsear JSON genérico: %s", err)
		return
	}

	// Intentar construir SensorMessageInput con redondeo
	var parsed SensorMessageInput

	if idFloat, ok := incoming["id"].(float64); ok {
		parsed.ID = int(idFloat)
	}
	if title, ok := incoming["title"].(string); ok {
		parsed.Title = title
	}
	if descFloat, ok := incoming["description"].(float64); ok {
		parsed.Description = int(descFloat + 0.5) // redondeo
	}
	if em, ok := incoming["emitter"].(string); ok {
		parsed.Emitter = em
	}
	if t, ok := incoming["topic"].(string); ok {
		parsed.Topic = t
	} else {
		parsed.Topic = topic // usar el topic recibido si no viene en payload
	}
	if serie, ok := incoming["serie"].(string); ok {
		parsed.Serie = serie
	}
	if created, ok := incoming["createdAt"].(string); ok {
		if strings.Contains(created, " ") {
			parsed.CreatedAt = strings.Split(created, " ")[1]
		} else {
			parsed.CreatedAt = created
		}
	}

	// Mostrar en consola el JSON procesado
	output := SensorMessageOutput{
		ID:          parsed.ID,
		Tittle:      strings.TrimSpace(parsed.Emitter),
		Description: parsed.Description,
		Emitter:     strings.TrimSpace(parsed.Emitter),
		Topic:       parsed.Topic,
		Serie:       parsed.Serie,
		CreatedAt:   parsed.CreatedAt,
	}

	jsonMsg, _ := json.MarshalIndent(output, "", "  ")
	log.Printf("📤 Mensaje formateado:\n%s", string(jsonMsg))

	// Enviar por WebSocket solo a los clientes con misma serie
	mutex.Lock()
	for client, serieCliente := range clients {
		if serieCliente != parsed.Serie {
			continue
		}
		err := client.WriteMessage(websocket.TextMessage, jsonMsg)
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

	// Suscribirse a topics notification/# y alert/#
	topics := []string{"notification/#", "alert/#"}
	for _, t := range topics {
		if token := client.Subscribe(t, 0, nil); token.Wait() && token.Error() != nil {
			log.Printf("❌ Error al suscribirse a '%s': %s", t, token.Error())
		} else {
			log.Printf("📡 Suscrito al topic: %s", t)
		}
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
