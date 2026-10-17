# Changelog

## 0.2.1

### Added

- `ModuleClient::call_attested_detailed` retains remote method failure fields
  for product adapters while keeping renderings and failure reports redacted.

## 0.2.0

### Breaking changes

- Module search paths are now opt-in. Hosts that need discovery must configure
  `SearchPathConfig` with their own environment variable and app directory;
  TinyBus no longer assumes OpenHuman's `OPENHUMAN_MODULE_PATH` or data paths.
- `ModuleHost::search_paths` is now an instance method and
  `ResolverInputs` carries optional search-path configuration and a linked
  registry. Update resolver construction to pass a `LinkedRegistry` (usually
  `LinkedRegistry::global()`) and the host's `SearchPathConfig`, if any.

### Added

- `UnloadedAccess` provides registry-backed client access for builds without
  the dynamic module loader.
- Hosts can set a bundled release directory and resolve it using an explicitly
  named environment variable.
