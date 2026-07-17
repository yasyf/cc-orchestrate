package orchestrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/cc-orchestrate/backend"
	"github.com/yasyf/reposync/registry"
)

func TestRegistryRepoViews(t *testing.T) {
	root := t.TempDir()
	matched := filepath.Join(root, "matched")
	dbOnly := filepath.Join(root, "db-only")
	canonical := filepath.Join(root, "canonical")
	for _, path := range []string{matched, dbOnly, canonical} {
		if err := os.Mkdir(path, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	matchedCanonical, err := filepath.EvalSymlinks(matched)
	if err != nil {
		t.Fatal(err)
	}
	canonicalPath, err := filepath.EvalSymlinks(canonical)
	if err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(root, "symlink")
	if err := os.Symlink(canonical, symlink); err != nil {
		t.Fatal(err)
	}
	loopA := filepath.Join(root, "loop-a")
	loopB := filepath.Join(root, "loop-b")
	if err := os.Symlink(loopA, loopB); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(loopB, loopA); err != nil {
		t.Fatal(err)
	}
	registryOnly := filepath.Join(root, "registry-only")
	missing := filepath.Join(root, "missing")
	localOnly := filepath.Join(root, "local-only")
	loadErr := errors.New("registry unavailable")

	for _, tc := range []struct {
		name       string
		registered registry.Registry
		repos      []repoRow
		want       []registryRepoView
		wantErr    error
		wantJSON   string
		jsonKind   string
	}{
		{
			name: "activated repo matches",
			registered: registry.Registry{Repos: []registry.Repo{{
				Relpath: "matched", Path: matched, Origin: "git@example.com:matched.git", Trunk: "main",
			}}},
			repos:    []repoRow{{ID: "repo-matched", Cwd: matchedCanonical}},
			want:     []registryRepoView{{Relpath: "matched", Path: matched, Origin: "git@example.com:matched.git", Trunk: "main", RepoID: "repo-matched"}},
			wantJSON: fmt.Sprintf(`{"relpath":"matched","path":%q,"origin":"git@example.com:matched.git","trunk":"main","repo_id":"repo-matched"}`, matched),
			jsonKind: "matched",
		},
		{
			name: "registry-only entry",
			registered: registry.Registry{Repos: []registry.Repo{{
				Relpath: "registry-only", Path: registryOnly, Origin: "git@example.com:registry-only.git", Trunk: "main",
			}}},
			want:     []registryRepoView{{Relpath: "registry-only", Path: registryOnly, Origin: "git@example.com:registry-only.git", Trunk: "main"}},
			wantJSON: fmt.Sprintf(`{"relpath":"registry-only","path":%q,"origin":"git@example.com:registry-only.git","trunk":"main"}`, registryOnly),
			jsonKind: "registry-only",
		},
		{
			name:       "DB-only repo",
			registered: registry.Registry{},
			repos:      []repoRow{{ID: "repo-db-only", Cwd: dbOnly}},
			want:       []registryRepoView{},
		},
		{
			name:       "nonexistent registry path",
			registered: registry.Registry{Repos: []registry.Repo{{Relpath: "missing", Path: missing}}},
			repos:      []repoRow{{ID: "repo-other", Cwd: dbOnly}},
			want:       []registryRepoView{{Relpath: "missing", Path: missing}},
		},
		{
			name:       "nonexistent registry path joins on raw path",
			registered: registry.Registry{Repos: []registry.Repo{{Relpath: "missing", Path: missing}}},
			repos:      []repoRow{{ID: "repo-missing", Cwd: missing}},
			want:       []registryRepoView{{Relpath: "missing", Path: missing, RepoID: "repo-missing"}},
		},
		{
			name:       "symlinked registry path",
			registered: registry.Registry{Repos: []registry.Repo{{Relpath: "linked", Path: symlink}}},
			repos:      []repoRow{{ID: "repo-linked", Cwd: canonicalPath}},
			want:       []registryRepoView{{Relpath: "linked", Path: symlink, RepoID: "repo-linked"}},
		},
		{
			name:       "local-only entry",
			registered: registry.Registry{Repos: []registry.Repo{{Relpath: "local-only", Path: localOnly, LocalOnly: true}}},
			want:       []registryRepoView{{Relpath: "local-only", Path: localOnly, LocalOnly: true}},
			wantJSON:   fmt.Sprintf(`{"relpath":"local-only","path":%q,"local_only":true}`, localOnly),
			jsonKind:   "local-only",
		},
		{
			name:    "load error",
			wantErr: loadErr,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := loadRegistry
			loadRegistry = func() (registry.Registry, error) {
				return tc.registered, tc.wantErr
			}
			t.Cleanup(func() { loadRegistry = previous })

			got, err := registryRepoViews(tc.repos)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("registryRepoViews error = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("registryRepoViews: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("registryRepoViews = %+v, want %+v", got, tc.want)
			}
			if tc.wantJSON != "" {
				encoded, err := json.Marshal(got[0])
				if err != nil {
					t.Fatal(err)
				}
				if string(encoded) != tc.wantJSON {
					t.Fatalf("marshaled view = %s, want %s", encoded, tc.wantJSON)
				}
				t.Logf("%s JSON: %s", tc.jsonKind, encoded)
			}
		})
	}

	t.Run("canonicalization error propagates", func(t *testing.T) {
		previous := loadRegistry
		loadRegistry = func() (registry.Registry, error) {
			return registry.Registry{Repos: []registry.Repo{{Relpath: "loop", Path: loopA}}}, nil
		}
		t.Cleanup(func() { loadRegistry = previous })

		_, err := registryRepoViews(nil)
		if err == nil || !strings.Contains(err.Error(), "canonicalizing registry repo path") {
			t.Fatalf("registryRepoViews error = %v, want canonicalization error", err)
		}
	})
}

func TestHandleFleetStatusRegistry(t *testing.T) {
	t.Run("activated repo is joined", func(t *testing.T) {
		ctx := context.Background()
		db := newTestDB(ctx, t)
		installTestFleet(t)
		backend.Register(opBackend{})
		cwd := canonPath(t, gitRepo(ctx, t, "main"))

		previous := loadRegistry
		loadRegistry = func() (registry.Registry, error) {
			return registry.Registry{Repos: []registry.Repo{{Relpath: "demo", Path: cwd, Origin: "git@example.com:demo.git", Trunk: "main"}}}, nil
		}
		t.Cleanup(func() { loadRegistry = previous })

		create := runTyped(handleRepoCreate, opCtx(db, mustJSON(t, map[string]string{"name": "demo", "backend": "optest", "cwd": cwd}), nil))
		if !create.OK {
			t.Fatalf("repo create failed: %s", create.Error)
		}
		var created repoCreateResult
		if err := json.Unmarshal(create.Body, &created); err != nil {
			t.Fatal(err)
		}

		reply := runTyped(handleFleetStatus, opCtx(db, nil, nil))
		if !reply.OK {
			t.Fatalf("fleet status failed: %s", reply.Error)
		}
		var got fleetStatusResult
		if err := json.Unmarshal(reply.Body, &got); err != nil {
			t.Fatal(err)
		}
		want := []registryRepoView{{Relpath: "demo", Path: cwd, Origin: "git@example.com:demo.git", Trunk: "main", RepoID: created.RepoID}}
		if !reflect.DeepEqual(got.Registry, want) {
			t.Fatalf("fleet registry = %+v, want %+v", got.Registry, want)
		}
	})

	t.Run("empty registry is present", func(t *testing.T) {
		db := newTestDB(context.Background(), t)
		installTestFleet(t)
		previous := loadRegistry
		loadRegistry = func() (registry.Registry, error) { return registry.Registry{}, nil }
		t.Cleanup(func() { loadRegistry = previous })

		reply := runTyped(handleFleetStatus, opCtx(db, nil, nil))
		if !reply.OK {
			t.Fatalf("fleet status failed: %s", reply.Error)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(reply.Body, &raw); err != nil {
			t.Fatal(err)
		}
		registryJSON, ok := raw["registry"]
		if !ok {
			t.Fatal("fleet status JSON has no registry field")
		}
		if string(registryJSON) != "[]" {
			t.Fatalf("fleet status registry JSON = %s, want []", registryJSON)
		}
		var got fleetStatusResult
		if err := json.Unmarshal(reply.Body, &got); err != nil {
			t.Fatal(err)
		}
		if got.Registry == nil || len(got.Registry) != 0 {
			t.Fatalf("fleet registry = %#v, want non-nil empty slice", got.Registry)
		}
	})
}
