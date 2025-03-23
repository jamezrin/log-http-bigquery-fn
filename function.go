package function

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

// Configuration constants
const (
	projectID = "your-project-id" // Replace with your GCP project ID
	datasetID = "http_logs"
	tableID   = "requests"
)

// RequestLog represents a log entry for an HTTP request
type RequestLog struct {
	Timestamp   time.Time         `bigquery:"timestamp"`
	Method      string            `bigquery:"method"`
	URL         string            `bigquery:"url"`
	Path        string            `bigquery:"path"`
	QueryParams string            `bigquery:"query_params"`
	Headers     map[string]string `bigquery:"headers"`
	RemoteAddr  string            `bigquery:"remote_addr"`
	UserAgent   string            `bigquery:"user_agent"`
	RequestBody string            `bigquery:"request_body,nullable"`
}

var bqClient *bigquery.Client

func init() {
	ctx := context.Background()
	var err error

	// Create BigQuery client
	bqClient, err = bigquery.NewClient(ctx, projectID)
	if err != nil {
		panic(fmt.Sprintf("Failed to create BigQuery client: %v", err))
	}

	// Register the HTTP function
	functions.HTTP("logQuery", handleRequest)
}

// handleRequest processes all HTTP requests, logs them to BigQuery, and returns appropriate responses
func handleRequest(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Log the request to BigQuery
	if err := logRequestToBigQuery(ctx, r); err != nil {
		fmt.Printf("Error logging to BigQuery: %v\n", err)
	}

	// Respond based on the HTTP method
	if _, err := fmt.Fprintf(w, "%s request received and logged\n", r.Method); err != nil {
		fmt.Printf("Error responding to request: %v\n", err)
	}
}

// logRequestToBigQuery logs the HTTP request details to BigQuery
func logRequestToBigQuery(ctx context.Context, r *http.Request) error {
	var requestBody string
	if r.Body != nil {
		bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024)) // Limit to 1MB
		if err == nil {
			requestBody = string(bodyBytes)
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}
	}

	// Extract headers
	headers := make(map[string]string)
	for name, values := range r.Header {
		if len(values) > 0 {
			headers[name] = values[0]
		}
	}

	// Create log entry
	logEntry := RequestLog{
		Timestamp:   time.Now(),
		Method:      r.Method,
		URL:         r.URL.String(),
		Path:        r.URL.Path,
		QueryParams: r.URL.RawQuery,
		Headers:     headers,
		RemoteAddr:  r.RemoteAddr,
		UserAgent:   r.UserAgent(),
		RequestBody: requestBody,
	}

	// Insert into BigQuery
	inserter := bqClient.Dataset(datasetID).Table(tableID).Inserter()
	return inserter.Put(ctx, logEntry)
}
