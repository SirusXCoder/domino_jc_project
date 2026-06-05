package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	defaultAlterURL   = "http://127.0.0.1:8280/alter"
	defaultSchemaFile = "schema.dgraph"
)

type alterResponse struct {
	Data struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

func main() {
	alterURL := flag.String("url", defaultAlterURL, "Dgraph alter endpoint URL")
	schemaPath := flag.String("schema", defaultSchemaFile, "Path to schema.dgraph file")
	flag.Parse()

	log.Printf("reading schema from %q", *schemaPath)
	schema, err := os.ReadFile(*schemaPath)
	if err != nil {
		log.Fatalf("failed to read schema file: %v", err)
	}
	if len(schema) == 0 {
		log.Fatal("schema file is empty")
	}
	log.Printf("loaded %d bytes of schema", len(schema))

	log.Printf("applying schema to %s", *alterURL)
	if err := applySchema(*alterURL, schema); err != nil {
		log.Fatalf("schema update failed: %v", err)
	}

	log.Println("schema update succeeded")
}

func applySchema(alterURL string, schema []byte) error {
	client := &http.Client{Timeout: 30 * time.Second}

	req, err := http.NewRequest(http.MethodPost, alterURL, bytes.NewReader(schema))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request to Dgraph: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("HTTP %s: %s", resp.Status, string(body))
	}

	var parsed alterResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return fmt.Errorf("parse response JSON: %w (body: %s)", err, string(body))
	}

	if len(parsed.Errors) > 0 {
		return fmt.Errorf("Dgraph returned errors: %s", parsed.Errors[0].Message)
	}

	if parsed.Data.Code != "Success" {
		return fmt.Errorf("unexpected response code %q (message: %q)", parsed.Data.Code, parsed.Data.Message)
	}

	log.Printf("Dgraph response: code=%q message=%q", parsed.Data.Code, parsed.Data.Message)
	return nil
}
