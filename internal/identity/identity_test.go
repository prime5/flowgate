package identity

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

var secret = []byte("test-secret-at-least-32-bytes-long!!")

var t0 = time.Unix(1_800_000_000, 0)

func b64(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

func sign(secret []byte, hdr, payload string) string {
	signingInput := b64(hdr) + "." + b64(payload)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func addrKey(r *http.Request) string { return "1.2.3.4" }

func keyFor(t *testing.T, authz string, sec []byte) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/", nil)
	if authz != "" {
		req.Header.Set("Authorization", authz)
	}
	return KeyFunc(sec, addrKey, func() time.Time { return t0 })(req)
}

const goodHdr = `{"alg":"HS256","typ":"JWT"}`

func TestValidTokenKeysOnSubject(t *testing.T) {
	tok := sign(secret, goodHdr, `{"sub":"triage-agent-readonly","scope":"read","iat":1799999900,"exp":1800000200}`)
	if got := keyFor(t, "Bearer "+tok, secret); got != "sub:triage-agent-readonly" {
		t.Fatalf("got %q", got)
	}
	if got := keyFor(t, "bearer "+tok, secret); got != "sub:triage-agent-readonly" {
		t.Fatalf("scheme must be case-insensitive, got %q", got)
	}
}

func TestEverythingElseFallsBackToAddress(t *testing.T) {
	valid := `{"sub":"agent","exp":1800000200}`
	cases := map[string]string{
		"no header":        "",
		"wrong scheme":     "Basic abc",
		"not a jwt":        "Bearer abc",
		"two segments":     "Bearer a.b",
		"expired":          "Bearer " + sign(secret, goodHdr, `{"sub":"agent","exp":1799999999}`),
		"exactly expired":  "Bearer " + sign(secret, goodHdr, `{"sub":"agent","exp":1800000000}`),
		"no exp":           "Bearer " + sign(secret, goodHdr, `{"sub":"agent"}`),
		"no sub":           "Bearer " + sign(secret, goodHdr, `{"exp":1800000200}`),
		"empty sub":        "Bearer " + sign(secret, goodHdr, `{"sub":"","exp":1800000200}`),
		"wrong secret":     "Bearer " + sign([]byte("another-secret-another-secret-123"), goodHdr, valid),
		"alg none":         "Bearer " + b64(`{"alg":"none"}`) + "." + b64(valid) + ".",
		"alg HS512 label":  "Bearer " + sign(secret, `{"alg":"HS512"}`, valid),
		"garbage payload":  "Bearer " + sign(secret, goodHdr, `not json`),
		"bad sig encoding": "Bearer " + b64(goodHdr) + "." + b64(valid) + ".!!!",
	}
	for name, authz := range cases {
		if got := keyFor(t, authz, secret); got != "ip:1.2.3.4" {
			t.Errorf("%s: want address fallback, got %q", name, got)
		}
	}
}

// Editing the payload of a genuine token must not produce a usable
// identity: this is the "mint a bucket per request" attack.
func TestTamperedPayloadIsRejected(t *testing.T) {
	good := sign(secret, goodHdr, `{"sub":"alice","exp":1800000200}`)
	forged := sign(secret, goodHdr, `{"sub":"mallory","exp":1800000200}`)
	// graft alice's signature onto mallory's payload
	gp, fp := split3(good), split3(forged)
	tampered := fp[0] + "." + fp[1] + "." + gp[2]
	if got := keyFor(t, "Bearer "+tampered, secret); got != "ip:1.2.3.4" {
		t.Fatalf("tampered token accepted: %q", got)
	}
}

func TestEmptySecretDisablesIdentityKeys(t *testing.T) {
	tok := sign(secret, goodHdr, `{"sub":"agent","exp":1800000200}`)
	if got := keyFor(t, "Bearer "+tok, nil); got != "ip:1.2.3.4" {
		t.Fatalf("got %q", got)
	}
}

func TestSubjectCannotCollideWithAddressKey(t *testing.T) {
	tok := sign(secret, goodHdr, `{"sub":"1.2.3.4","exp":1800000200}`)
	if keyFor(t, "Bearer "+tok, secret) == keyFor(t, "", secret) {
		t.Fatal("a subject that looks like an IP must not share that IP's bucket")
	}
}

func split3(s string) [3]string {
	var out [3]string
	i := 0
	start := 0
	for j := 0; j < len(s) && i < 2; j++ {
		if s[j] == '.' {
			out[i] = s[start:j]
			i++
			start = j + 1
		}
	}
	out[2] = s[start:]
	return out
}
