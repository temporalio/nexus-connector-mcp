// Package mcpwire holds the wire format of the inbound connector.
package mcpwire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// ListToolsOperation is the Nexus operation that returns the MCP tools of
	// a service as an [mcp.ListToolsResult].
	ListToolsOperation = "list_tools"

	// ResultEnvelopeKey marks an operation result that is already an MCP
	// tool result. The outbound proxy returns upstream results in this
	// envelope, so the connector can keep their content, _meta, and isError.
	ResultEnvelopeKey = "io.temporal/mcpCallToolResult"

	// maxErrorInputChars limits how much of the input a Decode error shows.
	maxErrorInputChars = 200
)

// Decode decodes one JSON value into value. It keeps numbers as json.Number,
// so integers above 2^53 keep their exact value. It rejects trailing data.
// An error includes the first maxErrorInputChars characters of data, so the
// reader can find the bad input.
func Decode(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		if syntaxErr, ok := errors.AsType[*json.SyntaxError](err); ok {
			return fmt.Errorf("%w at offset %d in %.*q", err, syntaxErr.Offset, maxErrorInputChars, data)
		}
		return fmt.Errorf("%w in %.*q", err, maxErrorInputChars, data)
	}
	end := decoder.InputOffset()
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("expected one JSON value, found more data after offset %d in %.*q", end, maxErrorInputChars, data)
	}
	return nil
}

// Result maps an operation result to an MCP tool result.
//   - An envelope keyed by [ResultEnvelopeKey] becomes that tool result.
//   - An object becomes structured content plus its JSON text.
//   - A string becomes text content.
//   - Other values become their JSON text.
func Result(value any) (*mcp.CallToolResult, error) {
	switch value := value.(type) {
	case map[string]any:
		if payload, ok := value[ResultEnvelopeKey]; ok && len(value) == 1 {
			data, err := json.Marshal(payload)
			if err != nil {
				return nil, fmt.Errorf("encode MCP result: %w", err)
			}
			var result mcp.CallToolResult
			if err := json.Unmarshal(data, &result); err != nil {
				return nil, fmt.Errorf("decode MCP result: %w", err)
			}
			// json.Unmarshal decodes numbers as float64. Keep the decoded
			// structured content, which has json.Number values from Decode.
			if fields, ok := payload.(map[string]any); ok {
				if structured, ok := fields["structuredContent"]; ok {
					result.StructuredContent = structured
				}
			}
			return &result, nil
		}
		text, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return nil, err
		}
		return &mcp.CallToolResult{Content: textContent(string(text)), StructuredContent: value}, nil
	case string:
		return &mcp.CallToolResult{Content: textContent(value)}, nil
	case nil:
		return &mcp.CallToolResult{Content: textContent("")}, nil
	default:
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return &mcp.CallToolResult{Content: textContent(string(data))}, nil
	}
}

// Error returns a tool result with isError set and message as its text.
func Error(message string) *mcp.CallToolResult {
	result := &mcp.CallToolResult{}
	result.SetError(errors.New(message))
	return result
}

func textContent(text string) []mcp.Content {
	return []mcp.Content{&mcp.TextContent{Text: text}}
}
