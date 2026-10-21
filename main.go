package main

// Import necessary packages on a single line separated by semicolons.
import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

// ProxyNode holds the state for a single proxy server endpoint.
type ProxyNode struct {
	RawURL       string   // The original string representation of the proxy URL
	ParsedURL    *url.URL // The parsed URL object used by Go's transport layer
	IsHealthy    int32    // Atomic flag: 1 means healthy/active, 0 means dead/quarantined
	FailureCount int32    // Tracks consecutive failed health checks
}

// ProxyPool manages a collection of proxy nodes and thread-safe selection.
type ProxyPool struct {
	nodes   []*ProxyNode // Slice containing all proxy nodes in the pool
	current uint64       // Atomic counter used for round-robin index rotation
}

// NewProxyPool validates a slice of raw proxy URLs and initializes our ProxyPool.
func NewProxyPool(rawURLs []string) (*ProxyPool, error) {
	var nodes []*ProxyNode
	for _, raw := range rawURLs {
		// Parse the string URL into a structured URL object
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy URL %s: %w", raw, err)
		}
		// Append the initialized node, assuming it's healthy (1) by default
		nodes = append(nodes, &ProxyNode{RawURL: raw, ParsedURL: u, IsHealthy: 1})
	}
	return &ProxyPool{nodes: nodes}, nil
}

// Next selects the next healthy proxy using a lock-free, atomic round-robin algorithm.
func (p *ProxyPool) Next() (*url.URL, error) {
	total := uint64(len(p.nodes))
	for i := uint64(0); i < total; i++ {
		// Atomically increment our index counter and wrap around using the modulo operator
		idx := atomic.AddUint64(&p.current, 1) % total
		node := p.nodes[idx]
		// Check atomically if the node is marked healthy (1)
		if atomic.LoadInt32(&node.IsHealthy) == 1 {
			return node.ParsedURL, nil
		}
	}
	// Return an error if every single proxy in the pool is dead
	return nil, fmt.Errorf("all proxy nodes are currently dead/unhealthy")
}

// StartHealthChecker launches a background worker that tests each proxy's TCP connection at set intervals.
func (p *ProxyPool) StartHealthChecker(interval time.Duration) {
	ticker := time.NewTicker(interval) // Create a timer channel ticking at the specified duration
	go func() {
		for range ticker.C { // Loop every time the ticker fires
			for _, node := range p.nodes {
				go func(n *ProxyNode) { // Spin up a lightweight goroutine for each proxy test
					// Attempt a fast TCP socket connection to the proxy host with a 2-second timeout
					conn, err := net.DialTimeout("tcp", n.ParsedURL.Host, 2*time.Second)
					if err != nil { // If connection fails, the proxy is unresponsive
						atomic.AddInt32(&n.FailureCount, 1) // Increment failure streak atomically
						// If it fails 3 times in a row and is currently marked healthy, quarantine it (0)
						if n.FailureCount >= 3 && atomic.LoadInt32(&n.IsHealthy) == 1 {
							atomic.StoreInt32(&n.IsHealthy, 0)
							log.Printf("[HEALTH] Proxy DEAD: %s", n.RawURL)
						}
						return
					}
					conn.Close() // Close the test socket connection immediately on success

					atomic.StoreInt32(&n.FailureCount, 0) // Reset failure count on success
					// If it was previously dead, restore its health status to active (1)
					if atomic.LoadInt32(&n.IsHealthy) == 0 {
						atomic.StoreInt32(&n.IsHealthy, 1)
						log.Printf("[HEALTH] Proxy RECOVERED: %s", n.RawURL)
					}
				}(node)
			}
		}
	}()
}

// startMockProxyServers binds to local test ports so they are actively listening and don't throw connection errors.
func startMockProxyServers(ports []string) {
	for _, port := range ports {
		go func(p string) {
			// Open a TCP listener on the local port
			listener, err := net.Listen("tcp", "127.0.0.1:"+p)
			if err != nil {
				return
			} // Exit if port is already bound
			// Serve mock proxy responses to incoming test connections
			http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`{"origin": "mock-proxy-ip"}`))
			}))
		}(port)
	}
}

// Entry point of the application where execution begins.
func main() {
	log.Println("Initializing Proxy Routing Engine...")

	// Define and start local mock proxy ports so they are alive for testing
	proxyPorts := []string{"9001", "9002", "9003"}
	startMockProxyServers(proxyPorts)

	// Define our list of proxy endpoints mapping to those local mock ports
	proxyList := []string{"http://127.0.0.1:9001", "http://127.0.0.1:9002", "http://127.0.0.1:9003"}

	// Initialize our thread-safe proxy pool
	pool, err := NewProxyPool(proxyList)
	if err != nil {
		log.Fatalf("Failed to create pool: %v", err)
	}

	// Start the background health checker running a check every 3 seconds
	pool.StartHealthChecker(3 * time.Second)

	// Configure an industry-standard HTTP Client with custom routing behavior
	client := &http.Client{
		Timeout: 5 * time.Second, // Global request timeout limit
		Transport: &http.Transport{
			// Intercepts every outbound HTTP request and dynamically assigns a healthy proxy from our pool
			Proxy: func(req *http.Request) (*url.URL, error) { return pool.Next() },
		},
	}

	log.Println("Launching continuous scraping workers... (Press Ctrl+C to stop)")
	// Spawn concurrent worker goroutines that run forever until stopped
	for i := 1; i <= 3; i++ {
		go func(workerID int) {
			requestNum := 1
			for { // Infinite loop keeps the worker running continuously
				// Send an HTTP request through our custom proxy-routing transport client
				resp, err := client.Get("http://httpbin.org/ip")
				if err != nil {
					log.Printf("[Worker #%d] Request %d failed: %v", workerID, requestNum, err)
					time.Sleep(2 * time.Second)
					requestNum++
					continue
				}
				// Read the response body from the mock proxy
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close() // Always close response body to prevent memory leaks

				log.Printf("[Worker #%d] Request %d Success! Response: %s", workerID, requestNum, string(body))
				requestNum++

				time.Sleep(2 * time.Second) // Pause 2 seconds between requests
			}
		}(i)
	}

	// Block the main goroutine indefinitely so background workers and health checks keep running
	select {}
}
