package telemetry

import (
	"crypto/rand"
	"fmt"
)

// newUUID returns a random v4 UUID string used for correlation IDs.
func newUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// rand.Read should never fail on a healthy system; fall back to a
		// time-ish value to avoid panicking the request path.
		return fmt.Sprintf("fallback-%d", len(b))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
