# wynbench-agent

Go-based backend engine for Wynbench. Manages connections, executes protocol actions, and runs workflows via a modular plugin system.

---

## Architecture

```
wynbench-agent/
├── cmd/server/        – main entry point; registers plugins & starts HTTP server
├── core/              – types, plugin interface, registry, stores, engine
│   ├── types.go       – Connection, Action, Result, Workflow, WorkflowRun
│   ├── registry.go    – Plugin interface + global registry (Register / Get)
│   ├── store.go       – thread-safe in-memory ConnectionStore & WorkflowStore
│   └── engine.go      – Engine: executes Actions and Workflows
├── config/            – local JSON config persistence + export/import snapshot type
├── api/               – HTTP handlers
│   ├── server.go      – Server struct + route registration
│   ├── connections.go – POST/GET/DELETE /connections
│   ├── actions.go     – POST /actions/execute
│   ├── workflows.go   – workflow CRUD + POST /workflows/run
│   ├── kafka.go       – read-only Kafka helper endpoints
│   ├── config.go      – config export/import endpoints
│   └── health.go      – GET /health
└── plugins/
    ├── http/          – HTTP GET/POST protocol plugin
    ├── kafka/         – Kafka produce/list/read protocol plugin
    └── sql/           – SQL stub protocol plugin

### Plugin Interface

Every protocol module must implement `core.Plugin`:

```go
type Plugin interface {
    Name()      string                         // unique protocol identifier
    Configure(map[string]any) error            // called with connection-level config
    Execute(Action) (Result, error)            // performs the protocol operation
}
```

Register a plugin at startup with `core.Register(plugin)`. The engine looks
plugins up by name when executing an Action.

### Adding a New Plugin

1. Create a new package under `plugins/<protocol>/`.
2. Implement the three `core.Plugin` methods.
3. Call `core.Register(yourplugin.New())` in `cmd/server/main.go`.

No core files need to change.

---

## HTTP API

### Connections

| Method | Path                     | Description                    |
|--------|--------------------------|--------------------------------|
| POST   | `/connections`           | Create a connection            |
| GET    | `/connections`           | List all connections           |
| DELETE | `/connections/{id}`      | Delete a connection by ID      |

**Create body**
```json
{
  "id":       "my-db",
  "name":     "Production DB",
  "protocol": "sql",
  "config":   { "dsn": "******host/db" }
}
```

### Actions

| Method | Path               | Description            |
|--------|--------------------|------------------------|
| POST   | `/actions/execute` | Run a single action    |

**Body**
```json
{
  "plugin":        "http",
  "connection_id": "optional-stored-connection",
  "params":        { "url": "https://example.com", "method": "GET" }
}
```

### Workflows

| Method | Path             | Description                            |
|--------|------------------|----------------------------------------|
| POST   | `/workflows/run` | Run an inline or stored workflow       |

### Health

| Method | Path      | Description                           |
|--------|-----------|---------------------------------------|
| GET    | `/health` | Basic service readiness and plugins   |

**Inline body**
```json
{
  "name": "my-workflow",
  "steps": [
    { "name": "step1", "action": { "plugin": "sql", "params": { "query": "SELECT 1" } } },
    { "name": "step2", "action": { "plugin": "http", "params": { "url": "https://example.com" } } }
  ]
}
```

**Stored workflow body** (reference a previously stored workflow by ID)
```json
{ "id": "stored-workflow-id" }
```

---

## Running

```bash
go run ./cmd/server            # default :8080
go run ./cmd/server -addr :9090
```

The server sends permissive CORS headers for local browser-based clients.

## Testing

```bash
go test ./...
```

## Built-in Plugins

| Name    | Status      | Notes                                                                  |
|---------|-------------|------------------------------------------------------------------------|
| `http`  | Functional  | Basic HTTP GET/POST; params: `url`, `method`, `body`                   |
| `kafka` | Functional  | Produce/list/read helpers; params include `brokers`, `topic`, `value`  |
| `sql`   | Stub        | Validates params, returns stub rows; no real DB                        |
