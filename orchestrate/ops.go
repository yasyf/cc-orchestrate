package orchestrate

import (
	"encoding/json"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
)

// agentView is the JSON shape every agent-facing op returns: the persisted agent
// fields a parent inspects, flattened from agentRow.
type agentView struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	ProjectID string `json:"project_id"`
	Backend   string `json:"backend"`
	Status    string `json:"status"`
	State     string `json:"state"`
	Activity  string `json:"activity"`
	Tokens    int    `json:"tokens"`
	UpdatedAt string `json:"updated_at"`
	SessionID string `json:"session_id"`
}

func newAgentView(a agentRow) agentView {
	return agentView{
		ID: a.ID, Name: a.Name, ProjectID: a.ProjectID, Backend: a.Backend,
		Status: a.Status, State: a.State, Activity: a.Activity, Tokens: a.Tokens,
		UpdatedAt: a.UpdatedAt, SessionID: a.SessionID,
	}
}

// statusPayload is the EventStatus event body the transcript tailer appends.
type statusPayload struct {
	State    string `json:"state"`
	Tool     string `json:"tool"`
	Target   string `json:"target"`
	LastText string `json:"last_text"`
	Tokens   int    `json:"tokens"`
}

func jsonStatus(st Status) json.RawMessage {
	b, _ := json.Marshal(statusPayload{
		State: st.State, Tool: st.Tool, Target: st.Target, LastText: st.LastText, Tokens: st.Tokens,
	})
	return b
}

func messagePayload(text string) json.RawMessage {
	b, _ := json.Marshal(map[string]string{"text": text})
	return b
}

// handleStatus answers agent-status with one agent's persisted snapshot.
func handleStatus(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		AgentID string `json:"agent_id"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad agent-status body: " + err.Error()}
	}
	ag, err := getAgent(hc.Ctx, hc.DB, b.AgentID)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	body, err := json.Marshal(newAgentView(ag))
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	return daemon.Reply{OK: true, Body: body}
}

// handleList answers agent-list with every agent, optionally filtered to one
// project. An absent body lists all.
func handleList(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Project string `json:"project"`
	}
	if len(hc.Env.Body) > 0 {
		if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
			return daemon.Reply{OK: false, Error: "bad agent-list body: " + err.Error()}
		}
	}
	agents, err := listAgents(hc.Ctx, hc.DB, b.Project)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	views := make([]agentView, len(agents))
	for i, a := range agents {
		views[i] = newAgentView(a)
	}
	body, err := json.Marshal(views)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	return daemon.Reply{OK: true, Body: body}
}

// handleSendMessage answers agent-send-message by appending an OriginHuman
// EventMessage to the target agent's subject log.
func handleSendMessage(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		AgentID string `json:"agent_id"`
		Text    string `json:"text"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad agent-send-message body: " + err.Error()}
	}
	ag, err := getAgent(hc.Ctx, hc.DB, b.AgentID)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	if ag.SubjectID == "" {
		return daemon.Reply{OK: false, Error: "agent has no subject: " + b.AgentID}
	}
	seq, err := hc.Append(hc.Ctx, &event.Event{
		SubjectID: ag.SubjectID, Origin: event.OriginHuman, Type: EventMessage, Payload: messagePayload(b.Text),
	})
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	body, _ := json.Marshal(map[string]int64{"seq": seq})
	return daemon.Reply{OK: true, Body: body}
}

// handleConfigSet answers config-set by upserting one config key.
func handleConfigSet(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad config-set body: " + err.Error()}
	}
	if err := setConfig(hc.Ctx, hc.DB, b.Key, b.Value); err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	return daemon.Reply{OK: true}
}

// handleConfigGet answers config-get with one config key's value and whether it
// is set.
func handleConfigGet(hc daemon.HandlerCtx) daemon.Reply {
	var b struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(hc.Env.Body, &b); err != nil {
		return daemon.Reply{OK: false, Error: "bad config-get body: " + err.Error()}
	}
	value, found, err := getConfig(hc.Ctx, hc.DB, b.Key)
	if err != nil {
		return daemon.Reply{OK: false, Error: err.Error()}
	}
	body, _ := json.Marshal(map[string]any{"value": value, "found": found})
	return daemon.Reply{OK: true, Body: body}
}
