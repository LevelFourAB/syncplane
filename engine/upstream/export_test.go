package upstream

// SetScanPage sets how many rows one transaction of a long pass handles, and
// returns a func that restores the previous value. For tests that need many
// pages from few rows.
func SetScanPage(n int) (restore func()) {
	old := scanPage
	scanPage = n
	return func() { scanPage = old }
}
