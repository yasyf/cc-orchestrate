package orchestrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/cc-interact/event"
)

func mustInsertAgent(t *testing.T, db *sql.DB, a agentRow) {
	t.Helper()
	if err := insertAgent(context.Background(), db, a); err != nil {
		t.Fatalf("insertAgent %s: %v", a.ID, err)
	}
}

func opCtx(db *sql.DB, body []byte, appendFn daemon.AppendFunc) daemon.HandlerCtx {
	return daemon.HandlerCtx{Ctx: context.Background(), Env: daemon.Envelope{Body: body}, DB: db, Append: appendFn}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %v: %v", v, err)
	}
	return b
}

func TestHandleSendMessage(t *testing.T) {
	db := newTestDB(t)
	mustInsertAgent(t, db, agentRow{
		ID: "a1", ProjectID: "p1", Backend: "tmux", Scope: "/s",
		SubjectID: "subj-1", Status: "active", State: StateUnknown, CreatedAt: "t0",
	})

	var captured *event.Event
	appendFn := func(_ context.Context, e *event.Event) (int64, error) {
		captured = e
		return 7, nil
	}
	body := mustJSON(t, map[string]string{"agent_id": "a1", "text": "hello"})
	reply := handleSendMessage(opCtx(db, body, appendFn))
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}
	if captured == nil {
		t.Fatal("Append was not called")
	}
	if captured.SubjectID != "subj-1" {
		t.Errorf("SubjectID = %q, want subj-1", captured.SubjectID)
	}
	if captured.Type != EventMessage {
		t.Errorf("Type = %q, want %q", captured.Type, EventMessage)
	}
	if captured.Origin != event.OriginHuman {
		t.Errorf("Origin = %q, want %q", captured.Origin, event.OriginHuman)
	}
	var pl struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(captured.Payload, &pl); err != nil || pl.Text != "hello" {
		t.Errorf("payload = %s, want text=hello (err %v)", captured.Payload, err)
	}
	var rb struct {
		Seq int64 `json:"seq"`
	}
	if err := json.Unmarshal(reply.Body, &rb); err != nil || rb.Seq != 7 {
		t.Errorf("reply body = %s, want seq=7 (err %v)", reply.Body, err)
	}
}

func TestHandleSendMessageErrors(t *testing.T) {
	db := newTestDB(t)
	mustInsertAgent(t, db, agentRow{
		ID: "nosub", ProjectID: "p1", Backend: "tmux", Scope: "/s",
		Status: "active", State: StateUnknown, CreatedAt: "t0",
	})
	appendFn := func(context.Context, *event.Event) (int64, error) {
		t.Fatal("Append must not be called when the op errors")
		return 0, nil
	}
	cases := []struct {
		name string
		body map[string]string
	}{
		{name: "missing agent", body: map[string]string{"agent_id": "ghost", "text": "x"}},
		{name: "agent without subject", body: map[string]string{"agent_id": "nosub", "text": "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reply := handleSendMessage(opCtx(db, mustJSON(t, tc.body), appendFn))
			if reply.OK || reply.Error == "" {
				t.Fatalf("reply = %+v, want ok=false with an error", reply)
			}
		})
	}
}

func TestHandleStatus(t *testing.T) {
	db := newTestDB(t)
	mustInsertAgent(t, db, agentRow{
		ID: "a1", ProjectID: "p1", Backend: "tmux", Scope: "/s", SessionID: "sess-1",
		Name: "worker", SubjectID: "subj-1", Status: "active", State: StateWorking,
		Activity: "Bash: ls", Tokens: 10, UpdatedAt: "2026-06-16T00:00:00Z", CreatedAt: "t0",
	})

	reply := handleStatus(opCtx(db, mustJSON(t, map[string]string{"agent_id": "a1"}), nil))
	if !reply.OK {
		t.Fatalf("reply not ok: %s", reply.Error)
	}
	var got agentView
	if err := json.Unmarshal(reply.Body, &got); err != nil {
		t.Fatal(err)
	}
	want := agentView{
		ID: "a1", Name: "worker", ProjectID: "p1", Backend: "tmux", Status: "active",
		State: StateWorking, Activity: "Bash: ls", Tokens: 10,
		UpdatedAt: "2026-06-16T00:00:00Z", SessionID: "sess-1",
	}
	if got != want {
		t.Fatalf("status view = %+v, want %+v", got, want)
	}
}

func TestHandleStatusMissing(t *testing.T) {
	db := newTestDB(t)
	reply := handleStatus(opCtx(db, mustJSON(t, map[string]string{"agent_id": "ghost"}), nil))
	if reply.OK || reply.Error == "" {
		t.Fatalf("reply = %+v, want ok=false with an error", reply)
	}
}

func TestHandleList(t *testing.T) {
	db := newTestDB(t)
	mustInsertAgent(t, db, agentRow{ID: "a1", ProjectID: "p1", Backend: "tmux", Scope: "/s", Status: "active", State: StateWorking, CreatedAt: "t0"})
	mustInsertAgent(t, db, agentRow{ID: "a2", ProjectID: "p2", Backend: "tmux", Scope: "/s", Status: "active", State: StateIdle, CreatedAt: "t1"})

	t.Run("all with absent body", func(t *testing.T) {
		reply := handleList(opCtx(db, nil, nil))
		if !reply.OK {
			t.Fatalf("reply not ok: %s", reply.Error)
		}
		var got []agentView
		if err := json.Unmarshal(reply.Body, &got); err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
	})
	t.Run("filtered by project", func(t *testing.T) {
		reply := handleList(opCtx(db, mustJSON(t, map[string]string{"project": "p2"}), nil))
		var got []agentView
		if err := json.Unmarshal(reply.Body, &got); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].ID != "a2" {
			t.Fatalf("filtered = %+v, want [a2]", got)
		}
	})
}

func TestHandleConfigGetSet(t *testing.T) {
	db := newTestDB(t)
	getBody := mustJSON(t, map[string]string{"key": "backend"})

	var got struct {
		Value string `json:"value"`
		Found bool   `json:"found"`
	}
	reply := handleConfigGet(opCtx(db, getBody, nil))
	if err := json.Unmarshal(reply.Body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Found {
		t.Fatalf("found before set = true, want false")
	}

	if reply := handleConfigSet(opCtx(db, mustJSON(t, map[string]string{"key": "backend", "value": "superset"}), nil)); !reply.OK {
		t.Fatalf("config-set not ok: %s", reply.Error)
	}

	reply = handleConfigGet(opCtx(db, getBody, nil))
	if err := json.Unmarshal(reply.Body, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Found || got.Value != "superset" {
		t.Fatalf("after set = %+v, want value=superset found=true", got)
	}
}
