# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Rewritten from Python to a single pure-Go CLI built on the
  [cc-interact](https://github.com/yasyf/cc-interact) framework. Distribution
  moves from PyPI wheels to prebuilt binaries and a Homebrew tap.

## [0.1.0] - 2026-06-12

### Added
- Initial scaffolding.
- `cc-orchestrate backends` command reporting which backends (cmux, superset)
  are installed.

[Unreleased]: https://github.com/yasyf/cc-orchestrate/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/yasyf/cc-orchestrate/releases/tag/v0.1.0
