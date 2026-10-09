// Package slack connects flowgate to Slack: a verified slash-command
// endpoint and a throttled alert notifier. It depends only on the
// standard library and on internal/runner, so every line of it is
// testable without Slack, a network, or an OpenTelemetry build.
package slack

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"time"
)

// Slack signs every request it sends so the receiver can tell Slack from
// anyone who has found the URL. The signature is
//
//	v0=hex(HMAC-SHA256(signing_secret, "v0:" + timestamp + ":" + raw_body))
//
// and the timestamp is checked against the clock so a captured request
// cannot be replayed later.
const (
	HeaderSignature = "X-Slack-Signature"
	HeaderTimestamp = "X-Slack-Request-Timestamp"

	// DefaultTolerance is the replay window Slack documents: reject a
	// request whose timestamp is more than five minutes from now.
	DefaultTolerance = 5 * time.Minute
)

var (
	ErrMissingHeaders = errors.New("slack: missing signature or timestamp header")
	ErrBadTimestamp   = errors.New("slack: malformed timestamp")
	ErrStale          = errors.New("slack: request timestamp outside the replay window")
	ErrBadSignature   = errors.New("slack: signature mismatch")
)

// Verifier checks Slack request signatures.
type Verifier struct {
	secret    []byte
	tolerance time.Duration
	now       func() time.Time
}

// NewVerifier returns a Verifier for the app's signing secret. An empty
// secret is refused: with no secret every signature would be forgeable.
func NewVerifier(secret string) (*Verifier, error) {
	if secret == "" {
		return nil, errors.New("slack: empty signing secret")
	}
	return &Verifier{secret: []byte(secret), tolerance: DefaultTolerance, now: time.Now}, nil
}

// Sign returns the v0 signature for a body at a Unix timestamp. Slack
// computes this on its side; it is exported so tests and local curl
// examples can produce a request the Verifier will accept.
func Sign(secret string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("v0:" + strconv.FormatInt(timestamp, 10) + ":"))
	mac.Write(body)
	return "v0=" + hex.EncodeToString(mac.Sum(nil))
}

// Verify checks the request headers against the raw body. The body must
// be the exact bytes Slack sent, before any form parsing.
func (v *Verifier) Verify(h http.Header, body []byte) error {
	sig, tsRaw := h.Get(HeaderSignature), h.Get(HeaderTimestamp)
	if sig == "" || tsRaw == "" {
		return ErrMissingHeaders
	}
	ts, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		return ErrBadTimestamp
	}
	skew := v.now().Sub(time.Unix(ts, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > v.tolerance {
		return ErrStale
	}
	want := Sign(string(v.secret), ts, body)
	// hmac.Equal compares in constant time, so response timing does not
	// leak how many leading bytes of a guessed signature were right.
	if !hmac.Equal([]byte(sig), []byte(want)) {
		return ErrBadSignature
	}
	return nil
}
