package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// InferWithOllama calls a local Ollama instance to infer a SimpleSchema from
// a JSON payload. The registry handles failures with explicit local inference.
//
// Environment variables:
//
//	OLLAMA_URL   base URL of the Ollama server (default: http://localhost:11434)
//	OLLAMA_MODEL model name to use           (default: llama3.2)
func InferWithOllama(payload []byte) (*SimpleSchema, error) {
	return InferWithOllamaContext(context.Background(), payload)
}

func InferWithOllamaContext(ctx context.Context, payload []byte) (*SimpleSchema, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	base := os.Getenv("OLLAMA_URL")
	if base == "" {
		base = "http://localhost:11434"
	}
	model := os.Getenv("OLLAMA_MODEL")
	if model == "" {
		model = "llama3.2"
	}

	prompt := buildPrompt(payload)

	reqBody, err := json.Marshal(map[string]any{
		"model":  model,
		"prompt": prompt,
		"stream": false,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/generate", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	// Inference is infrequent; an owned transport avoids leaving idle HTTP
	// connections behind after the registry's final worker exits.
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Ollama returned HTTP %d", resp.StatusCode)
	}

	const maxInferenceResponse = 1 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxInferenceResponse+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxInferenceResponse {
		return nil, fmt.Errorf("Ollama response exceeds size limit")
	}

	var result struct {
		Response string `json:"response"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	return parseOllamaResponse(result.Response)
}

// buildPrompt constructs a tightly scoped prompt that asks Ollama to return
// only a JSON object mapping field names to their types.
func buildPrompt(payload []byte) string {
	return fmt.Sprintf(`You are a JSON schema inference engine.
Given this JSON message, output ONLY a JSON object where each key is a top-level field name
and each value is one of: "string", "number", "integer", "boolean", "object", "array", "null".
Do not add any explanation, markdown, or code fences. Output raw JSON only.

Message: %s`, string(payload))
}

// parseOllamaResponse extracts a SimpleSchema from the LLM's text response.
// It looks for the first '{...}' block in the response, tolerating markdown fences.
func parseOllamaResponse(text string) (*SimpleSchema, error) {
	// Strip markdown code fences if present
	text = strings.TrimSpace(text)
	text = strings.TrimPrefix(text, "```json")
	text = strings.TrimPrefix(text, "```")
	text = strings.TrimSuffix(text, "```")
	text = strings.TrimSpace(text)

	// Find first JSON object
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start == -1 || end == -1 || end <= start {
		return nil, fmt.Errorf("no JSON object found in Ollama response")
	}
	text = text[start : end+1]

	var raw map[string]string
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return nil, fmt.Errorf("failed to parse Ollama schema response: %w", err)
	}

	fields := make(map[string]FieldType, len(raw))
	for k, v := range raw {
		fields[k] = FieldType(strings.ToLower(v))
	}
	return &SimpleSchema{Fields: fields}, nil
}
