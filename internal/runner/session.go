package runner

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrSessionNotFound is returned when a requested session does not exist.
var ErrSessionNotFound = errors.New("runner: session not found")

// Session maintains the state across multi-turn interactions.
type Session struct {
	ConversationID     string            `json:"conversation_id"`
	Channel            ChannelMeta       `json:"channel"`
	History            []Envelope        `json:"history"`
	ActiveExperimentID string            `json:"active_experiment_id,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	CreatedAt          time.Time         `json:"created_at"`
	UpdatedAt          time.Time         `json:"updated_at"`
}

// SessionStore abstracts session state persistence.
type SessionStore interface {
	Get(ctx context.Context, conversationID string) (*Session, error)
	Save(ctx context.Context, session *Session) error
}

// MemoryStore is an in-memory, concurrency-safe implementation of SessionStore.
type MemoryStore struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewMemoryStore returns an initialized in-memory session store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		sessions: make(map[string]*Session),
	}
}

// Get retrieves a session by conversation ID.
func (m *MemoryStore) Get(ctx context.Context, conversationID string) (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.sessions[conversationID]
	if !ok {
		return nil, ErrSessionNotFound
	}

	// Return a defensive shallow copy with copied history slice to prevent data races
	clone := *s
	clone.History = append([]Envelope(nil), s.History...)
	if s.Metadata != nil {
		clone.Metadata = make(map[string]string, len(s.Metadata))
		for k, v := range s.Metadata {
			clone.Metadata[k] = v
		}
	}
	return &clone, nil
}

// Save stores or updates a session.
func (m *MemoryStore) Save(ctx context.Context, session *Session) error {
	if session == nil || session.ConversationID == "" {
		return errors.New("runner: cannot save session with empty conversation ID")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	clone := *session
	clone.History = append([]Envelope(nil), session.History...)
	if session.Metadata != nil {
		clone.Metadata = make(map[string]string, len(session.Metadata))
		for k, v := range session.Metadata {
			clone.Metadata[k] = v
		}
	}
	m.sessions[session.ConversationID] = &clone
	return nil
}
