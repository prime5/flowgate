// Package identity derives a rate-limit key from a verified bearer
// token instead of a network address.
//
// The upstream issues HS256 JWTs (aiops-triage-agent: claims sub,
// scope, iat, exp). Keying the limiter on the verified subject means
// one agent identity gets one bucket regardless of which IP it comes
// from, and many identities behind one NAT do not share a bucket.
//
// The one rule that matters: a claim is used as a key only after the
// signature and expiry check out. Anything else (no header, wrong
// scheme, bad signature, wrong algorithm, expired, no subject) falls
// back to the address key. Trusting an unverified "sub" would let a
// caller mint a fresh bucket per request by editing the token, which
// is the same hole as trusting a spoofable Fly-Client-IP header.
//
// This is fairness, not authentication: flowgate never rejects a
// request for a bad token, it just declines to use it as a key. The
// upstream still does its own auth. Only the standard library is used.
package identity

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// Key prefixes keep the two key spaces disjoint, so a token whose sub
// looks like an IP address cannot collide with that address's bucket.
const (
	subjectPrefix = "sub:"
	addrPrefix    = "ip:"
)

// KeyFunc returns a ratelimit key function. secret is the HS256 shared
// secret; an empty secret disables identity keys entirely (every
// request uses the fallback). fallback is the address-based key
// function. now is injectable for tests; nil means time.Now.
func KeyFunc(secret []byte, fallback func(*http.Request) string, now func() time.Time) func(*http.Request) string {
	if now == nil {
		now = time.Now
	}
	return func(r *http.Request) string {
		if len(secret) > 0 {
			if sub, ok := verifiedSubject(r.Header.Get("Authorization"), secret, now()); ok {
				return subjectPrefix + sub
			}
		}
		return addrPrefix + fallback(r)
	}
}

type header struct {
	Alg string `json:"alg"`
}

type claims struct {
	Sub string  `json:"sub"`
	Exp float64 `json:"exp"`
	Iat float64 `json:"iat"`
}

// verifiedSubject returns the token's subject only if the token is a
// well-formed HS256 JWT with a valid signature, a future expiry and a
// non-empty subject.
func verifiedSubject(authz string, secret []byte, now time.Time) (string, bool) {
	const scheme = "bearer "
	if len(authz) <= len(scheme) || !strings.EqualFold(authz[:len(scheme)], scheme) {
		return "", false
	}
	token := strings.TrimSpace(authz[len(scheme):])
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", false
	}

	var h header
	if !decodeJSON(parts[0], &h) || h.Alg != "HS256" { // rejects "none" and algorithm confusion
		return "", false
	}

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(sig, mac.Sum(nil)) {
		return "", false
	}

	var c claims
	if !decodeJSON(parts[1], &c) {
		return "", false
	}
	if c.Sub == "" || c.Exp == 0 || now.Unix() >= int64(c.Exp) {
		return "", false
	}
	return c.Sub, true
}

func decodeJSON(seg string, v any) bool {
	raw, err := base64.RawURLEncoding.DecodeString(seg)
	if err != nil {
		return false
	}
	return json.Unmarshal(raw, v) == nil
}
