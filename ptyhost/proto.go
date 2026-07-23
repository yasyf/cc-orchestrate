package ptyhost

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/yasyf/daemonkit/wire"
)

const (
	ptySchemaV1     = `{"name":"cc-orchestrate.pty","version":1,"operations":[{"op":"pty.capture","request":"empty","response":{"text":"string"}},{"op":"pty.keys","request":{"data":"base64-bytes"},"response":"empty"}]}`
	ptySchemaDigest = "d487d54dbf122e2bf481a266d9840f980a10e2da82931d34d3efc4f44f2f3daf"
	ptyWireBuild    = "cc-orchestrate.pty.v1." + ptySchemaDigest
)

const (
	opCapture wire.Op = "pty.capture"
	opKeys    wire.Op = "pty.keys"
)

type captureResponse struct {
	Text string `json:"text"`
}

type keysRequest struct {
	Data []byte `json:"data"`
}

func encodeMessage(message any) ([]byte, error) {
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("pty-host encode: %w", err)
	}
	return payload, nil
}

func decodeMessage(payload []byte, message any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(message); err != nil {
		return fmt.Errorf("pty-host decode: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("pty-host decode: trailing value")
		}
		return fmt.Errorf("pty-host decode trailing value: %w", err)
	}
	return nil
}
