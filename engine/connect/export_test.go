package connect

import "time"

// SetClock replaces the clock record mode reads for quiet periods and delete
// grace. Call it before the connector runs a pass.
func SetClock(c *Connector, now func() time.Time) { c.now = now }

// EditDistance exposes editDistance over the bytes of two strings.
func EditDistance(a, b string, limit int) int { return editDistance([]byte(a), []byte(b), limit) }
