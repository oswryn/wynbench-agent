// Package core contains the Plugin interface and the global plugin registry.
package core

import "fmt"

// Plugin is the interface that all protocol modules must implement.
//
//	Name()      – returns the unique protocol identifier (e.g. "http", "sql").
//	Configure() – optional hook for validating/preparing connection-level settings (invoked by callers as needed).
//	Execute()   – runs a single Action and returns a Result.
type Plugin interface {
	Name() string
	Configure(cfg map[string]any) error
	Execute(action Action) (Result, error)
}

// registry holds all registered plugins keyed by their Name().
var registry = map[string]Plugin{}

// Register adds a Plugin to the global registry.
// It panics if a plugin with the same name has already been registered, which
// catches accidental double-registration at startup.
func Register(p Plugin) {
	name := p.Name()
	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("plugin %q is already registered", name))
	}
	registry[name] = p
}

// Get returns the Plugin for the given protocol name and a boolean indicating
// whether it was found.
func Get(name string) (Plugin, bool) {
	p, ok := registry[name]
	return p, ok
}

// Registered returns a slice of all registered plugin names, useful for
// introspection and health-check endpoints.
func Registered() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	return names
}
