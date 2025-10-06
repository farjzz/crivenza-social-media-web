package main
import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"time"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)
func cfg() *oauth2.Config {
	return &oauth2.Config{
		ClientID:     os.Getenv("GOOGLE_CLIENT_ID"),
		ClientSecret: os.Getenv("GOOGLE_CLIENT_SECRET"),
		RedirectURL:  os.Getenv("OAUTH_REDIRECT_URL"),
		Scopes:       []string{"openid", "email", "profile"},
		Endpoint:     google.Endpoint,
	}
}
func rndState() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
func Login(w http.ResponseWriter, r *http.Request) {
	c := cfg()
	s, err := rndState()
	if err != nil {
		http.Error(w, "state fail", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "s",
		Value:    s,
		Path:     "/",
		HttpOnly: true,
		Expires:  time.Now().Add(5 * time.Minute),
	})
	u := c.AuthCodeURL(s)
	http.Redirect(w, r, u, http.StatusFound)
}
func Callback(w http.ResponseWriter, r *http.Request) {
	c := cfg()
	
	cookie, err := r.Cookie("s")
	if err != nil || r.URL.Query().Get("state") != cookie.Value {
		http.Error(w, "bad state", http.StatusBadRequest)
		return
	}
	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "no code", http.StatusBadRequest)
		return
	}
	tok, err := c.Exchange(context.Background(), code)
	if err != nil {
		http.Error(w, "exchange fail", http.StatusInternalServerError)
		return
	}
	req, _ := http.NewRequest("GET", "https://openidconnect.googleapis.com/v1/userinfo", nil)
	req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, "userinfo fail", http.StatusInternalServerError)
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		http.Error(w, "userinfo fail", http.StatusInternalServerError)
		return
	}
	var u struct {
		Email string `json:"email"`
	}
	if err := json.NewDecoder(res.Body).Decode(&u); err != nil {
		http.Error(w, "userinfo parse fail", http.StatusInternalServerError)
		return
	}
	claims := &Claims{
		Email: u.Email,
		Role:  "user",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	tkn, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(jwtKey)
	if err != nil {
		http.Error(w, "token fail", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"token": tkn})
}
