package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

const (
	kafkaBroker = "localhost:9092"
	topic       = "chat"
	groupID     = "chat-server-group"
)

type ChatMessage struct {
	ID     string `json:"id"`
	RoomID string `json:"roomId"`
	UserID string `json:"userId"`
	Text   string `json:"text"`
	TS     string `json:"ts"`
}
type SSEClient struct {
	id      string
	room    string
	writer  http.ResponseWriter
	flusher http.Flusher
	closed  chan struct{}
}
type Server struct {
	clientsMu sync.RWMutex
	clients   map[string]*SSEClient

	producer *kafka.Writer
	consumer *kafka.Reader
}

func main() {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    topic,
		Balancer: &kafka.Hash{},
	})
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	srv := &Server{
		clients:  make(map[string]*SSEClient),
		producer: writer,
		consumer: reader,
	}
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	http.HandleFunc("/messages", srv.handleProduce)
	http.HandleFunc("/stream", srv.handleStream)
	ctx, cancel := context.WithCancel(context.Background())
	go srv.consumeLoop(ctx)
	port := "3000"
	srvHTTP := &http.Server{Addr: ":" + port}
	go func() {
		log.Printf("HTTP server listening on http://localhost:%s\n", port)
		if err := srvHTTP.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down...")
	cancel()
	_ = srv.consumer.Close()
	_ = srv.producer.Close()
	ctxShutdown, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = srvHTTP.Shutdown(ctxShutdown)
}
func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		RoomID string `json:"roomId"`
		UserID string `json:"userId"`
		Text   string `json:"text"`
	}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(body.RoomID) == "" || strings.TrimSpace(body.UserID) == "" || strings.TrimSpace(body.Text) == "" {
		http.Error(w, "roomId, userId and text required", http.StatusBadRequest)
		return
	}
	msg := ChatMessage{
		ID:     uuid.NewString(),
		RoomID: body.RoomID,
		UserID: body.UserID,
		Text:   body.Text,
		TS:     time.Now().UTC().Format(time.RFC3339),
	}
	b, _ := json.Marshal(msg)
	kMsg := kafka.Message{
		Key:   []byte(body.RoomID), // key = roomId ensures same-room messages land in the same partition by Hash balancer
		Value: b,
		Time:  time.Now(),
	}
	if err := s.producer.WriteMessages(r.Context(), kMsg); err != nil {
		log.Printf("producer write error: %v\n", err)
		http.Error(w, "failed to produce message", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(b)
}
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	room := r.URL.Query().Get("room")
	if strings.TrimSpace(room) == "" {
		http.Error(w, "room query param required", http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	clientID := uuid.NewString()
	client := &SSEClient{
		id:      clientID,
		room:    room,
		writer:  w,
		flusher: flusher,
		closed:  make(chan struct{}),
	}
	s.clientsMu.Lock()
	s.clients[clientID] = client
	s.clientsMu.Unlock()
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()
	notify := r.Context().Done()
	<-notify
	s.clientsMu.Lock()
	delete(s.clients, clientID)
	s.clientsMu.Unlock()
	close(client.closed)
}
func (s *Server) consumeLoop(ctx context.Context) {
	log.Println("Consumer loop starting...")
	for {
		m, err := s.consumer.FetchMessage(ctx)
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				log.Println("Consumer fetch canceled, exiting loop")
				return
			}
			log.Printf("FetchMessage error: %v\n", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		var msg ChatMessage
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			log.Printf("Failed to unmarshal message: %v\n", err)
			if err := s.consumer.CommitMessages(ctx, m); err != nil {
				log.Printf("Commit error: %v", err)
			}
			continue
		}
		s.broadcastToRoom(&msg)
		if err := s.consumer.CommitMessages(ctx, m); err != nil {
			log.Printf("Commit error: %v", err)
		}
	}
}
func (s *Server) broadcastToRoom(msg *ChatMessage) {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()

	data, _ := json.Marshal(msg)
	payload := fmt.Sprintf("data: %s\n\n", string(data))

	for _, c := range s.clients {
		if c.room != msg.RoomID {
			continue
		}
		go func(cl *SSEClient) {
			select {
			case <-cl.closed:
				return
			default:
			}
			_, err := cl.writer.Write([]byte(payload))
			if err != nil {
				log.Printf("Write to client %s failed: %v\n", cl.id, err)
				s.clientsMu.Lock()
				delete(s.clients, cl.id)
				s.clientsMu.Unlock()
				return
			}
			cl.flusher.Flush()
		}(c)
	}
}
