package orchestrate

import (
	"reflect"
	"testing"
)

func schemaType(t *testing.T, node any) string {
	t.Helper()
	m, ok := node.(map[string]any)
	if !ok {
		t.Fatalf("schema node is not an object: %#v", node)
	}
	s, _ := m["type"].(string)
	return s
}

// TestSchemaFor walks a struct carrying every kind schemaFor supports and asserts the
// property types, the required set (non-omitempty, non-pointer), and that skipped and
// unexported fields never surface.
func TestSchemaFor(t *testing.T) {
	type nested struct {
		A string `json:"a"`
	}
	type sample struct {
		Str        string         `json:"str"`
		Num        int            `json:"num"`
		Big        int64          `json:"big"`
		Flag       bool           `json:"flag"`
		Ratio      float64        `json:"ratio"`
		Tags       []string       `json:"tags"`
		Child      nested         `json:"child"`
		Dict       map[string]int `json:"dict"`
		Optional   *string        `json:"optional,omitempty"`
		Ptr        *nested        `json:"ptr"`
		Omitted    string         `json:"omitted,omitempty"`
		Skipped    string         `json:"-"`
		unexported string
	}
	_ = sample{}.unexported // keep the field referenced

	schema := schemaFor(reflect.TypeFor[sample]())
	if schema["type"] != "object" {
		t.Fatalf("top-level type = %v, want object", schema["type"])
	}
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties missing or wrong type: %#v", schema["properties"])
	}

	for _, tc := range []struct {
		field, want string
	}{
		{"str", "string"},
		{"num", "integer"},
		{"big", "integer"},
		{"flag", "boolean"},
		{"ratio", "number"},
		{"tags", "array"},
		{"child", "object"},
		{"dict", "object"},
		{"optional", "string"},
		{"ptr", "object"},
		{"omitted", "string"},
	} {
		if got := schemaType(t, props[tc.field]); got != tc.want {
			t.Errorf("property %q type = %q, want %q", tc.field, got, tc.want)
		}
	}

	if items := schemaType(t, props["tags"].(map[string]any)["items"]); items != "string" {
		t.Errorf("tags items type = %q, want string", items)
	}
	if ap := schemaType(t, props["dict"].(map[string]any)["additionalProperties"]); ap != "integer" {
		t.Errorf("dict additionalProperties type = %q, want integer", ap)
	}

	for _, gone := range []string{"Skipped", "-", "unexported"} {
		if _, present := props[gone]; present {
			t.Errorf("property %q must not be present", gone)
		}
	}

	required := map[string]bool{}
	for _, r := range schema["required"].([]string) {
		required[r] = true
	}
	for _, want := range []string{"str", "num", "big", "flag", "ratio", "tags", "child", "dict"} {
		if !required[want] {
			t.Errorf("field %q should be required", want)
		}
	}
	for _, notWant := range []string{"optional", "ptr", "omitted"} {
		if required[notWant] {
			t.Errorf("field %q must not be required (pointer or omitempty)", notWant)
		}
	}
}

// TestSchemaForPanicsOnUnsupported proves an unschemable field type fails loudly, the
// same panic the registry constructor relies on at package init.
func TestSchemaForPanicsOnUnsupported(t *testing.T) {
	type bad struct {
		Fn func() `json:"fn"`
	}
	defer func() {
		if recover() == nil {
			t.Error("schemaFor did not panic on a func field")
		}
	}()
	schemaFor(reflect.TypeFor[bad]())
}
