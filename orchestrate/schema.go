package orchestrate

import (
	"fmt"
	"reflect"
	"strings"
)

// schemaFor reflects t into a JSON-Schema node: a struct becomes an object whose
// json fields are its properties and whose non-omitempty, non-pointer fields are
// required; a slice or array becomes an array; a map becomes an object keyed by
// additionalProperties; and a primitive becomes its matching scalar. A pointer is
// unwrapped and treated as its element, which is why a *T field is never required.
// It panics on a kind no request or response type may legally hold (func, chan,
// interface, complex, unsafe pointer), so an unschemable field fails at
// registry-construction time rather than on a live request.
func schemaFor(t reflect.Type) map[string]any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Slice, reflect.Array:
		return map[string]any{"type": "array", "items": schemaFor(t.Elem())}
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			panic(fmt.Sprintf("schema: map key must be a string, got %s", t.Key()))
		}
		return map[string]any{"type": "object", "additionalProperties": schemaFor(t.Elem())}
	case reflect.Struct:
		return structSchema(t)
	default:
		panic(fmt.Sprintf("schema: unsupported kind %s for type %s", t.Kind(), t))
	}
}

// structSchema walks t's exported fields into an object schema, keying each by its
// json property name and collecting the required set (non-omitempty, non-pointer).
func structSchema(t reflect.Type) map[string]any {
	props := map[string]any{}
	var required []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name, omitempty, skip := jsonField(f)
		if skip {
			continue
		}
		props[name] = schemaFor(f.Type)
		if f.Type.Kind() != reflect.Pointer && !omitempty {
			required = append(required, name)
		}
	}
	schema := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

// jsonField resolves a struct field's JSON property name and whether it is omitempty,
// honoring an explicit `json:"name,opts"` tag and falling back to the Go field name.
// A `json:"-"` tag skips the field entirely.
func jsonField(f reflect.StructField) (name string, omitempty, skip bool) {
	tag := f.Tag.Get("json")
	if tag == "-" {
		return "", false, true
	}
	name = f.Name
	if tag != "" {
		parts := strings.Split(tag, ",")
		if parts[0] != "" {
			name = parts[0]
		}
		for _, o := range parts[1:] {
			if o == "omitempty" {
				omitempty = true
			}
		}
	}
	return name, omitempty, false
}
