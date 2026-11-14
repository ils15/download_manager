# Build e CI

Como compilar, testar e empacotar o `download_manager` no Windows x64.

## Toolchain

- Rust 1.92.0 ou superior. `Cargo.toml` declara `rust-version = "1.92"` (piso de MSRV compatível com Slint) e `edition = "2021"`.
- O CI valida as duas pontas da matriz: **1.92.0** (MSRV declarada) e **1.99.1** (toolchain corrente).
- Slint 1.8-compatível é a exigência declarada; `Cargo.lock` resolve Slint/slint-build para **1.18.1**.
- Componentes de CI: `clippy` e `rustfmt`.

## Dependências de sistema (Windows)

- Alvo `x86_64-pc-windows-msvc` com linker MSVC — Visual Studio Build Tools com a carga de trabalho C++.
- Nenhuma dependência de sistema além do toolchain MSVC: `build.rs` compila `ui/main.slint` via `slint-build` durante o build; a UI não depende de WebView ou de serviços externos.
- Inno Setup **7.1.0** apenas para gerar o instalador (`ISCC.exe`).

Nenhum requisito de sistema Linux é documentado pelo projeto; a única execução Linux registrada foi uma verificação local antiga em WSL2 (ver `docs/VERIFICACAO.md`).

## Comandos

Executar localmente:

```powershell
cargo run --release
```

Build de release para o alvo Windows x64 (mesmo comando do CI):

```powershell
cargo build --locked --release --target x86_64-pc-windows-msvc
```

Verificações da esteira (idênticas às do workflow):

```powershell
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
```

Perfil de release definido em `Cargo.toml`: `lto = true`, `codegen-units = 1`, `panic = "abort"`, `strip = true`.

Artefato do executável: `target/x86_64-pc-windows-msvc/release/independent-download-manager.exe`.

## Instalador (Inno Setup)

```powershell
C:\InnoSetup\7.1.0\ISCC.exe installer.iss
```

- Saída: `setup-independent-download-manager.exe` no diretório do script (`OutputDir=.`, `OutputBaseFilename=setup-independent-download-manager`).
- Instalação por usuário por padrão (`PrivilegesRequired=lowest`); a opção all-users no diálogo de override de privilégios é o único ponto em que o UAC do Windows é solicitado, e quem solicita é o instalador, não o app.
- `ArchitecturesAllowed=x64` e `ArchitecturesInstallIn64BitMode=x64`.
- Instala o executável e copia `extension\*` para `{app}\extension`.
- Registra o host de Native Messaging do Chrome em `Software\Google\Chrome\NativeMessagingHosts\org.independent.download_manager` (raiz `HKA`, que resolve para HKCU na instalação por usuário ou HKLM na instalação all-users) e gera `{app}\native-host.json` com `type: stdio`, `args: ["--native-host"]` e `allowed_origins` a partir do ID de extensão informado.
- O ID de extensão é opcional no wizard e, quando informado, precisa ter exatamente 32 caracteres de `a` a `p`. Em branco, o manifesto recebe o placeholder `REPLACE_WITH_EXTENSION_ID` e a integração com o Chrome **não funciona** até uma instalação com ID explícito.
- `UninstallDelete` remove `{app}\native-host.json`; a chave de registro usa `uninsdeletekey`.

O instalador gerado pelo CI é sempre o **template não funcional**: nenhum ID de extensão real, chave fixa ou extensão assinada está incluído.

## CI (`/.github/workflows/windows.yml`)

- Runner: `windows-2025` (Microsoft Windows Server 2025, imagem `windows-2025-vs2026`).
- Matriz: `rust: ["1.92.0", "1.99.1"]`.
- Checkout com `persist-credentials: false`; actions fixadas por SHA imutável (`actions/checkout` v4.4.0, `dtolnay/rust-toolchain`, `actions/upload-artifact` v4.6.2). Permissões do workflow: `contents: read`.
- Passos: formatação → Clippy (`-D warnings`) → testes travados (`--locked`) → build de release `x86_64-pc-windows-msvc`.
- Apenas a perna Rust 1.99.1 instala o Inno Setup 7.1.0 do release oficial, verifica o SHA-256 (`0362a383ed217d4c4239b5933866dd96d3eb2102737da92f80f6057a4b40df2f`), compila `installer.iss` e publica o instalador.
- Artefatos publicados: instalador template, executável de release e os três arquivos da extensão (`manifest.json`, `popup.html`, `popup.js`).
- Artefatos de Actions não são releases: nenhuma release foi criada, nenhuma tag foi enviada e nada foi publicado na Chrome Web Store.

## Notas de verificação

- Execução registrada do workflow: run `39309557904` (commit `5c6ff09`), `success` nas duas pernas da matriz — formatação, Clippy, testes travados, build Windows x64, instalação do Inno Setup 7.1.0 com verificação de SHA-256, compilação do `installer.iss` e upload dos artefatos.
- Resultado de testes nessa execução: 96 aprovados no binário de testes da biblioteca e 35 no binário de testes específico do alvo — 131 no total, 0 falhas, 0 ignorados.
- Cobertura: **NOT MEASURED**.
- `Cargo.lock` é gerado pelo Cargo; a revisão de dependências permanece pendente.
- Ambiente executado: `windows-2025` mais um Windows 11 local. **Windows 10 x64 não foi validado.** ACLs herdadas, registro de Native Messaging, comportamento de desinstalação, Slint/UI, comportamento de privilégio do Inno Setup, semântica de caminho/arquivo atômico do Windows e resolução de dependências seguem como gates de revisão.
