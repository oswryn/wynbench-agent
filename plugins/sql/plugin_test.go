package sqlplugin_test

import (
	"testing"

	"github.com/wynforge/wynbench-agent/core"
	sqlplugin "github.com/wynforge/wynbench-agent/plugins/sql"
)

func TestSQLPlugin_Name(t *testing.T) {
	p := sqlplugin.New()
	if p.Name() != "sql" {
		t.Fatalf("expected name 'sql', got %q", p.Name())
	}
}

func TestSQLPlugin_Configure(t *testing.T) {
	p := sqlplugin.New()
	if err := p.Configure(map[string]any{"dsn": "postgres://localhost/test"}); err != nil {
		t.Fatalf("unexpected configure error: %v", err)
	}
}

func TestSQLPlugin_Execute_MissingConnectionString(t *testing.T) {
	p := sqlplugin.New()
	result, err := p.Execute(core.Action{
		Plugin: "sql",
		Params: map[string]any{"query": "SELECT 1"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Success {
		t.Fatal("expected failure when connection string is missing")
	}
}

func TestSQLPlugin_Execute_MissingQuery(t *testing.T) {
	p := sqlplugin.New()
	result, _ := p.Execute(core.Action{Plugin: "sql", Params: map[string]any{}})
	if result.Success {
		t.Fatal("expected failure when query param is missing")
	}
}

func TestSQLPlugin_Execute_UnsupportedDBType(t *testing.T) {
	p := sqlplugin.New()
	result, _ := p.Execute(core.Action{Plugin: "sql", Params: map[string]any{
		"query":            "SELECT 1",
		"connectionString": "postgres://localhost/test",
		"db_type":          "sqlite",
	}})
	if result.Success {
		t.Fatal("expected failure when db_type is unsupported")
	}
}
