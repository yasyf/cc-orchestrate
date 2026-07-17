package orchestrate

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/yasyf/cc-interact/daemon"
	"github.com/yasyf/reposync/registry"
)

var loadRegistry = registry.Load

type registryRepoView struct {
	Relpath   string `json:"relpath"`
	Path      string `json:"path"`
	Origin    string `json:"origin,omitempty"`
	Trunk     string `json:"trunk,omitempty"`
	LocalOnly bool   `json:"local_only,omitempty"`
	RepoID    string `json:"repo_id,omitempty"`
}

func registryRepoViews(repos []repoRow) ([]registryRepoView, error) {
	registered, err := loadRegistry()
	if err != nil {
		return nil, fmt.Errorf("load reposync registry: %w", err)
	}
	views := make([]registryRepoView, len(registered.Repos))
	for i, repo := range registered.Repos {
		path := repo.Path
		canonical, err := filepath.EvalSymlinks(path)
		if err == nil {
			path = canonical
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("canonicalizing registry repo path %q: %w", path, err)
		}
		view := registryRepoView{
			Relpath: repo.Relpath, Path: repo.Path, Origin: repo.Origin,
			Trunk: repo.Trunk, LocalOnly: repo.LocalOnly,
		}
		for _, activated := range repos {
			if activated.Cwd == path {
				view.RepoID = activated.ID
				break
			}
		}
		views[i] = view
	}
	return views, nil
}

type registryListRequest struct{}

func handleRegistryList(hc daemon.HandlerCtx, _ registryListRequest) ([]registryRepoView, error) {
	repos, err := listRepos(hc.Ctx, hc.DB, "")
	if err != nil {
		return nil, err
	}
	return registryRepoViews(repos)
}
