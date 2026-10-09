package slack

import (
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// Reference vectors computed with Python's hmac module, not with Sign, so
// the signing recipe is checked against something independent of it.
func TestSign_KnownVectors(t *testing.T) {
	// The worked example from Slack's "Verifying requests from Slack" page.
	docBody := []byte("token=xyzz0WbapA4vBCDEFasx0q6G&team_id=T1DC2JH3J&team_domain=testteamnow&channel_id=G8PSS9T3V&channel_name=foobar&user_id=U2CERLKJA&user_name=roadrunner&command=%2Fwebhook-collect&text=&response_url=https%3A%2F%2Fhooks.slack.com%2Fcommands%2FT1DC2JH3J%2F397700885554%2F96rGlfmibIGlgcZRskXaIFfN&trigger_id=398738663015.47445629121.803a0bc887a14d10d2c447fce8b6703c")
	if got, want := Sign("8f742231b10e8888abcd99yyyzzz85a5", 1531420618, docBody),
		"v0=a2114d57b48eac39b9ad189dd8316235a7b4a8d21a10bd27519666489c69b503"; got != want {
		t.Fatalf("docs vector: got %s want %s", got, want)
	}
	if got, want := Sign("test-secret", 1700000000, []byte("command=%2Fflowgate&text=status")),
		"v0=b8ffe4b18b77bccedcccf607204c0f3676a6e2e2998c08a1f3d6c0050293c2b2"; got != want {
		t.Fatalf("python vector: got %s want %s", got, want)
	}
}

func verifierAt(t *testing.T, secret string, now time.Time) *Verifier {
	t.Helper()
	v, err := NewVerifier(secret)
	if err != nil {
		t.Fatal(err)
	}
	v.now = func() time.Time { return now }
	return v
}

func signedHeader(secret string, ts int64, body []byte) http.Header {
	h := http.Header{}
	h.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	h.Set(HeaderSignature, Sign(secret, ts, body))
	return h
}

func TestVerify(t *testing.T) {
	now := time.Unix(1700000000, 0)
	body := []byte("command=%2Fflowgate&text=status")
	v := verifierAt(t, "s3cret", now)

	t.Run("valid", func(t *testing.T) {
		if err := v.Verify(signedHeader("s3cret", now.Unix(), body), body); err != nil {
			t.Fatalf("want nil, got %v", err)
		}
	})
	t.Run("wrong secret", func(t *testing.T) {
		if err := v.Verify(signedHeader("other", now.Unix(), body), body); !errors.Is(err, ErrBadSignature) {
			t.Fatalf("want ErrBadSignature, got %v", err)
		}
	})
	t.Run("tampered body", func(t *testing.T) {
		h := signedHeader("s3cret", now.Unix(), body)
		if err := v.Verify(h, []byte("command=%2Fflowgate&text=run+latency+0.5+15")); !errors.Is(err, ErrBadSignature) {
			t.Fatalf("want ErrBadSignature, got %v", err)
		}
	})
	t.Run("replay: too old", func(t *testing.T) {
		old := now.Add(-6 * time.Minute).Unix()
		if err := v.Verify(signedHeader("s3cret", old, body), body); !errors.Is(err, ErrStale) {
			t.Fatalf("want ErrStale, got %v", err)
		}
	})
	t.Run("too far in the future", func(t *testing.T) {
		fut := now.Add(6 * time.Minute).Unix()
		if err := v.Verify(signedHeader("s3cret", fut, body), body); !errors.Is(err, ErrStale) {
			t.Fatalf("want ErrStale, got %v", err)
		}
	})
	t.Run("inside the window", func(t *testing.T) {
		ok := now.Add(-4 * time.Minute).Unix()
		if err := v.Verify(signedHeader("s3cret", ok, body), body); err != nil {
			t.Fatalf("want nil, got %v", err)
		}
	})
	t.Run("missing headers", func(t *testing.T) {
		if err := v.Verify(http.Header{}, body); !errors.Is(err, ErrMissingHeaders) {
			t.Fatalf("want ErrMissingHeaders, got %v", err)
		}
	})
	t.Run("malformed timestamp", func(t *testing.T) {
		h := signedHeader("s3cret", now.Unix(), body)
		h.Set(HeaderTimestamp, "yesterday")
		if err := v.Verify(h, body); !errors.Is(err, ErrBadTimestamp) {
			t.Fatalf("want ErrBadTimestamp, got %v", err)
		}
	})
	t.Run("signature without v0 prefix", func(t *testing.T) {
		h := signedHeader("s3cret", now.Unix(), body)
		h.Set(HeaderSignature, h.Get(HeaderSignature)[3:])
		if err := v.Verify(h, body); !errors.Is(err, ErrBadSignature) {
			t.Fatalf("want ErrBadSignature, got %v", err)
		}
	})
}

func TestNewVerifier_RefusesEmptySecret(t *testing.T) {
	if _, err := NewVerifier(""); err == nil {
		t.Fatal("empty secret must be refused: every signature would be forgeable")
	}
}
