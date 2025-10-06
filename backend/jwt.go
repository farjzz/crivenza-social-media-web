package main
import (
	"database/sql"         
	"encoding/json"         
	"net/http"              
	"time"                 
	"github.com/golang-jwt/jwt/v5"   
	"golang.org/x/crypto/bcrypt"     
	_ "github.com/lib/pq"            
)
var jwtKey = []byte("my_secret_key") 
type Claims struct {
	Email string `json:"email"` 
	Role  string `json:"role"`  
	jwt.RegisteredClaims       
}
func JwtHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var creds struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	db, err := sql.Open("postgres", "host=localhost port=5432 user=youruser password=yourpass dbname=xdbase sslmode=disable")
	if err != nil {
		http.Error(w, "DB connect fail", http.StatusInternalServerError)
		return
	}
	defer db.Close()
	table := "users"
	if creds.Role == "uploader" {
		table = "uploaders"
	}
	var hashedPwd string
	err = db.QueryRow("SELECT password FROM "+table+" WHERE email=$1", creds.Email).Scan(&hashedPwd)
	if err == sql.ErrNoRows {
		http.Error(w, "no such email", http.StatusUnauthorized)
		return
	} else if err != nil {
		http.Error(w, "DB error", http.StatusInternalServerError)
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hashedPwd), []byte(creds.Password)); err != nil {
		http.Error(w, "wrong password", http.StatusUnauthorized)
		return
	}
	claims := &Claims{
		Email: creds.Email,
		Role:  creds.Role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)), 
			IssuedAt:  jwt.NewNumericDate(time.Now()),                       
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(jwtKey)
	if err != nil {
		http.Error(w, "token fail", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"token": tokenString})
}
