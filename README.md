# download_manager

Gerenciador de downloads HTTP(S) para Windows x64: interface nativa em Slint, motor de transferência em Rust e extensão Chrome opcional que envia apenas a URL da aba ativa.

![Rust](https://img.shields.io/badge/rust-1.92%2B-dea584?logo=rust&logoColor=white)
![Plataforma](https://img.shields.io/badge/plataforma-Windows%20x64-0078D6?logo=windows&logoColor=white)
![Chrome](https://img.shields.io/badge/chrome-MV3%20opcional-4285F4?logo=googlechrome&logoColor=white)
![Licença](https://img.shields.io/badge/licen%C3%A7a-n%C3%A3o%20declarada-lightgrey)

## O que é

Um protótipo independente de gerenciador de downloads para desktop Windows, escrito em Rust. Nenhum código, marca, arte, asset ou algoritmo proprietário de outro gerenciador de downloads é usado.

- Fila com concorrência configurável por instância (1–8), prioridades Low/Normal/High (FIFO dentro da mesma prioridade), pausa, retomada e cancelamento.
- Paciência de banda compartilhada por instância (bytes/s, `0` = ilimitado) e limite separado por download (KB/s).
- Transferência HTTP(S) assíncrona com retry/backoff para falhas transitórias, limites de bytes e de duração absoluta, e commit de saída sem sobrescrever arquivo existente.
- Download multi-segmento condicional (até 4 conexões) com validação estrita de `206`, `Content-Length`/`Content-Range` e ETag forte; fallback automático para fluxo único.
- Retomada em processo via `Range`/`If-Range` enquanto o handle parcial permanece aberto; retomada persistente em disco é desabilitada de propósito.
- SHA-256 esperado opcional e agendamento diário em janela UTC.
- Histórico de terminal persistido (até 500 entradas) sem credenciais, query strings, fragmentos ou diretórios de destino.
- Extensão Chrome Manifest V3 opcional que só lê a URL da aba após um clique explícito; sem telemetria, sem toasts do sistema, sem integração com cookies ou sessão do navegador.

## Requisitos

- Windows x64 com alvo MSVC (`x86_64-pc-windows-msvc`).
- Rust 1.92 ou superior — é a MSRV declarada em `Cargo.toml` (`rust-version = "1.92"`), compatível com Slint. O CI executa em 1.92.0 e 1.99.1.
- Visual Studio Build Tools com a carga de trabalho C++ para o linker MSVC.
- Nenhuma dependência de sistema além do toolchain MSVC: a UI Slint é compilada no build (`build.rs`), sem WebView.
- Para gerar o instalador: Inno Setup 7.1.0 (`ISCC.exe`).
- Ambiente efetivamente validado: runner `windows-2025` (Windows Server 2025) e Windows 11 local. **Windows 10 x64 não foi validado.**

## Instalação / Build

```powershell
# compilar e executar localmente
cargo run --release

# build de release para o alvo Windows x64 (mesmo comando do CI)
cargo build --locked --release --target x86_64-pc-windows-msvc
```

Verificações usadas pelo CI (`.github/workflows/windows.yml`):

```powershell
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
```

Instalador (requer Inno Setup 7.1.0 instalado):

```powershell
C:\InnoSetup\7.1.0\ISCC.exe installer.iss
```

O CI publica como artefatos o executável de release, os arquivos da extensão e o instalador. O instalador gerado é um **template não funcional** enquanto mantiver o ID de extensão placeholder: informe um ID válido do Chrome durante a instalação para ativar a integração.

## Uso

Abra o aplicativo, clique em **Adicionar** e informe a URL HTTP(S). O destino padrão é a pasta `Downloads` do usuário; o nome final é reduzido a um leaf sanitizado e um arquivo existente nunca é sobrescrito. Os limites padrão são 4096 MiB por download e 360 minutos no total, configuráveis no painel de limites de transferência.

Fluxos detalhados — fila e prioridades, segmentos, pausa/retomada, agendamento UTC, histórico, limites e configuração da extensão Chrome — estão em [docs/USO.md](docs/USO.md).

## Documentação

- [docs/USO.md](docs/USO.md) — fluxos de uso, fila, segmentos, agendamento e configuração do Chrome.
- [docs/ARQUITETURA.md](docs/ARQUITETURA.md) — módulos, motor de transferência, persistência e protocolo de inbox.
- [docs/BUILD.md](docs/BUILD.md) — toolchain, dependências de sistema, comandos, instalador e CI.
- [docs/SEGURANCA.md](docs/SEGURANCA.md) — fronteiras de segurança e itens não implementados.
- [docs/VERIFICACAO.md](docs/VERIFICACAO.md) — matriz de funcionalidades e status de verificação.
- [SECURITY.md](SECURITY.md) — checklist de revisão de segurança.
- [CONTRIBUTING.md](CONTRIBUTING.md) — regras de contribuição.

## Licença

Este repositório ainda não inclui um arquivo `LICENSE`. Nenhuma licença é concedida até que o proprietário do projeto escolha uma explicitamente; não adicione cabeçalho de licença, arquivo `LICENSE` ou aviso de copyright em nome do proprietário.
