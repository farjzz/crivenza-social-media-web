package main
import (
	"net/http"
	"log"
)
func main() {
	http.HandleFunc("/jwt", JwtHandler)
	http.HandleFunc("/oauth/login", Login)     
	http.HandleFunc("/oauth/callback", Callback)
	log.Println("Server running at http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}
