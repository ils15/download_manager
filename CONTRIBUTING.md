# Contributing

This repository is an independent Windows 10 x64 download manager. The rules below
are the durable housekeeping rules for every change, review, and release step.

## Scope of a change

- Work only inside the project root. Do not write outside it, and do not widen
  permissions to work around a denial: report the blocker instead.
- Keep the project self-contained and implement the smallest complete, testable
  slice. Add focused tests for the behavior that changed; do not add
  network-dependent, install-level, or end-to-end suites unless the change
  actually alters that path.
- Do not publish, commit, or push from an unreviewed change. No Release or Store
  publication happens from a normal contribution.

## Clean-room and originality

- The implementation is independent. Do not copy, decompile, transplant, or
  reproduce Internet Download Manager (IDM) or any other download manager's code,
  branding, artwork, assets, icons, text, or proprietary algorithms.
- Do not reuse a closed-source binary, decompiled listing, or extracted resource.
  Write the feature from the documented HTTP(S) behavior and from first
  principles. Do not reopen a prior reverse-engineering effort of a
  third-party binary, and do not build on its output.
- New UI, icons, and copy must be original to this project.

## Safety boundaries

- Do not bypass DRM, access controls, or license checks, and do not extract or
  use cookies, credentials, tokens, or browser authentication state. The client
  handles only URLs the user supplied or explicitly opted in, over ordinary
  HTTP(S).
- The application never elevates, never writes Windows registry state, and never
  invokes PowerShell or shell commands. Per-user installation is the default;
  the optional per-machine path is the only place Windows UAC is requested, and
  it is requested by the installer, not by the app.
- Do not install global tools or toolchains, and do not run administrative
  commands as part of a contribution.

## Secrets and generated artifacts

- Never commit secrets, credentials, private keys, certificates, signing material
  (`*.pfx`, `*.p12`, `*.sig`), or a generated Native Messaging host manifest
  (`native-host.json`). The ignore rules live in `.gitignore`; keep them
  current when a new generated file appears.
- Do not commit build outputs, installer artifacts, or downloaded payloads
  (`/target/`, `/artifacts/`, `/installer-output/`, `*.zip`, `*.7z`, `*.iso`,
  `*.apk`, `*.part`, `*.crdownload`, `*.download`).
- Local agent and editor state (`/.opencode/`, `.vscode/`, `.idea/`) stays
  untracked.

## Verification and review

- Do not claim a check passed unless it was actually executed and observed.
  Report every check that was not run as unverified, with the reason.
- The Windows workflow in `.github/workflows/windows.yml` is the release gate:
  formatting, Clippy, locked tests, and the Windows x64 release build on the
  declared Rust 1.92.0 and 1.99.1 toolchains. A change is not publishable until
  that workflow has run successfully on a clean Windows 10 x64 environment.
- A security and quality review (Themis) must pass before any public push or
  release. It covers the clean-room boundary, the safety boundaries above, the
  pending Windows review gates listed in `SECURITY.md`, dependency review of the
  Cargo-generated `Cargo.lock`, and a secret scan.
- Windows-specific behavior — inherited ACLs, Native Messaging registration,
  installer and uninstall behavior, NTFS hard-link and atomic-replace
  semantics — stays a review gate until it is observed on Windows.

## License

This repository does not yet include a `LICENSE` file. No license is granted
until the project owner explicitly chooses one; until then, do not add a license
header, a `LICENSE` file, or a copyright notice on the owner's behalf.
