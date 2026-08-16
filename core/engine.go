// Package core implements the workflow engine that executes ordered sequences
// of Actions by dispatching each one to its registered plugin.
package core

import "fmt"

// Engine executes Actions and Workflows by looking up the appropriate Plugin
// from the global registry and (optionally) enriching Action.Params with
// configuration from a stored Connection.
type Engine struct {
	connections *ConnectionStore
}

// NewEngine creates an Engine backed by the supplied ConnectionStore.
func NewEngine(cs *ConnectionStore) *Engine {
	return &Engine{connections: cs}
}

// ExecuteAction resolves the plugin for a.Plugin, optionally merges the
// corresponding Connection's Config into a.Params, and delegates to
// Plugin.Execute.
func (e *Engine) ExecuteAction(a Action) (Result, error) {
	pluginName := a.Plugin

	// Merge connection config into params when a ConnectionID is provided.
	if a.ConnectionID != "" {
		conn, found := e.connections.Get(a.ConnectionID)
		if !found {
			return Result{Success: false, Error: fmt.Sprintf("connection %q not found", a.ConnectionID)},
				fmt.Errorf("connection %q not found", a.ConnectionID)
		}

		if pluginName == "" {
			pluginName = conn.Protocol
			a.Plugin = pluginName
		} else if conn.Protocol != "" && conn.Protocol != pluginName {
			return Result{
					Success: false,
					Error:   fmt.Sprintf("connection %q uses protocol %q, but action requested plugin %q", a.ConnectionID, conn.Protocol, pluginName),
				},
				fmt.Errorf("connection %q uses protocol %q, but action requested plugin %q", a.ConnectionID, conn.Protocol, pluginName)
		}

		merged := make(map[string]any, len(conn.Config)+len(a.Params))
		for k, v := range conn.Config {
			merged[k] = v
		}
		// Action-level params take precedence over connection config.
		for k, v := range a.Params {
			merged[k] = v
		}
		a.Params = merged
	}

	plugin, ok := Get(pluginName)
	if !ok {
		return Result{Success: false, Error: fmt.Sprintf("unknown plugin %q", pluginName)},
			fmt.Errorf("unknown plugin %q", pluginName)
	}

	return plugin.Execute(a)
}

// RunWorkflow executes each step in the workflow in order. It stops at the
// first error and marks the overall run as failed.
func (e *Engine) RunWorkflow(wf Workflow) WorkflowRun {
	run := WorkflowRun{
		WorkflowID: wf.ID,
		Results:    make([]Result, 0, len(wf.Steps)),
		Success:    true,
	}

	for _, step := range wf.Steps {
		result, err := e.ExecuteAction(step.Action)
		if err != nil {
			result = Result{Success: false, Error: err.Error()}
		}
		run.Results = append(run.Results, result)
		if !result.Success {
			run.Success = false
			break
		}
	}

	return run
}
