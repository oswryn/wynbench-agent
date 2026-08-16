// Package sqlplugin implements the "sql" protocol plugin for Wynbench.
//
// It supports PostgreSQL and Microsoft SQL Server as first-class sub-plugins
// under a single "sql" action interface.
package sqlplugin

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/wynforge/wynbench-agent/core"
)

const queryTimeout = 15 * time.Second

// Plugin is the SQL protocol plugin.
type Plugin struct{}

// New returns a ready-to-use SQL Plugin.
func New() *Plugin { return &Plugin{} }

// Name returns the protocol identifier used to look up this plugin.
func (p *Plugin) Name() string { return "sql" }

// Configure accepts optional connection-level settings. It is intentionally
// permissive because runtime validation happens at execution time.
func (p *Plugin) Configure(cfg map[string]any) error {
	return nil
}

// Execute runs a SQL query against PostgreSQL or Microsoft SQL Server.
//
// Expected params:
//
//	"query" (string) – the SQL query to execute
//
// Optional params:
//
//	"db_type"         (string) – postgres or mssql
//	"connectionString" (string) – connection string (overrides connection config)
//	"dsn"              (string) – legacy alias for "connectionString"
func (p *Plugin) Execute(action core.Action) (core.Result, error) {
	query, ok := action.Params["query"].(string)
	if !ok || strings.TrimSpace(query) == "" {
		return core.Result{Success: false, Error: "missing param: query"}, nil
	}

	dsn, ok := connectionString(action.Params)
	if !ok || strings.TrimSpace(dsn) == "" {
		return core.Result{Success: false, Error: "missing param: connectionString or dsn"}, nil
	}

	dbType, _ := stringParam(action.Params, "db_type")
	dbType = strings.ToLower(strings.TrimSpace(dbType))
	if dbType == "" {
		dbType = inferDbType(dsn)
	}
	if dbType == "" {
		return core.Result{Success: false, Error: "missing param: db_type or unsupported connection string"}, nil
	}

	driver, ok := sqlDriver(dbType)
	if !ok {
		return core.Result{Success: false, Error: fmt.Sprintf("unsupported db_type %q", dbType)}, nil
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return core.Result{Success: false, Error: fmt.Sprintf("failed to open database driver: %v", err)}, nil
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	if isReadQuery(query) {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return core.Result{Success: false, Error: fmt.Sprintf("query failed: %v", err)}, nil
		}
		defer rows.Close()

		resultRows, err := rowsToMaps(rows)
		if err != nil {
			return core.Result{Success: false, Error: fmt.Sprintf("failed to read rows: %v", err)}, nil
		}

		return core.Result{Success: true, Data: map[string]any{
			"db_type": dbType,
			"rows":    resultRows,
			"count":   len(resultRows),
		}}, nil
	}

	execResult, err := db.ExecContext(ctx, query)
	if err != nil {
		return core.Result{Success: false, Error: fmt.Sprintf("execution failed: %v", err)}, nil
	}

	affected, _ := execResult.RowsAffected()
	insertID, _ := execResult.LastInsertId()

	return core.Result{Success: true, Data: map[string]any{
		"db_type":        dbType,
		"rows_affected":  affected,
		"last_insert_id": insertID,
	}}, nil
}

func stringParam(params map[string]any, key string) (string, bool) {
	v, ok := params[key].(string)
	if !ok || strings.TrimSpace(v) == "" {
		return "", false
	}
	return v, true
}

func connectionString(cfg map[string]any) (string, bool) {
	if v, ok := cfg["connectionString"].(string); ok && strings.TrimSpace(v) != "" {
		return v, true
	}
	if v, ok := cfg["dsn"].(string); ok && strings.TrimSpace(v) != "" {
		return v, true
	}
	return "", false
}

func inferDbType(dsn string) string {
	trimmed := strings.ToLower(strings.TrimSpace(dsn))
	if strings.HasPrefix(trimmed, "postgres://") || strings.HasPrefix(trimmed, "postgresql://") {
		return "postgres"
	}
	if strings.HasPrefix(trimmed, "sqlserver://") || strings.Contains(trimmed, "database=") {
		return "mssql"
	}
	return ""
}

func sqlDriver(dbType string) (string, bool) {
	switch dbType {
	case "postgres", "postgresql":
		return "postgres", true
	case "mssql", "sqlserver":
		return "sqlserver", true
	default:
		return "", false
	}
}

func rowsToMaps(rows *sql.Rows) ([]map[string]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		scans := make([]any, len(columns))
		for i := range scans {
			scans[i] = &values[i]
		}

		if err := rows.Scan(scans...); err != nil {
			return nil, err
		}

		row := make(map[string]any, len(columns))
		for i, col := range columns {
			row[col] = normalizeSQLValue(values[i])
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func normalizeSQLValue(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return string(v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	default:
		return v
	}
}

func isReadQuery(query string) bool {
	normalized := strings.TrimSpace(strings.ToUpper(query))
	return strings.HasPrefix(normalized, "SELECT") || strings.HasPrefix(normalized, "WITH") || strings.HasPrefix(normalized, "SHOW")
}
