# Verificação

Matriz de funcionalidades, gates restantes e histórico de verificação. Nenhum resultado abaixo foi reexecutado para esta mudança de documentação.

## Matriz de funcionalidades

| Área | Status | Gate restante |
|---|---|---|
| HTTP(S) multi-segmento condicional, validadores estritos, limpeza, quota/pacing | Implementado; testes loopback determinísticos adicionados | Executado pelo CI Windows run `39309557904`; comportamento em Windows 10 x64 não verificado |
| Pausa/retomada single-stream em processo | Implementado com handle de staging aberto e ETag forte | Não é seguro a crash; retomada persistente desabilitada de propósito |
| Native Messaging opt-in do Chrome, instalador, UI Slint, fila/agendamento, integridade | Implementado neste protótipo | CI Windows executado (`39309557904`); revisão do instalador template e validação em Windows 10 x64 pendentes |
| Site-grabber, FTP/MMS, antivírus, toasts do SO, agendamento/banda entre instâncias | Não implementado | Revisão separada de produto/segurança seria necessária |

## Execuções registradas

- **CI Windows, run `39309557904`** (commit `5c6ff09`, push): `success` nas duas pernas da matriz — Rust 1.92.0 e 1.99.1. Passos executados: `cargo fmt --all -- --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo test --locked --all-targets`, build de release `x86_64-pc-windows-msvc`, instalação do Inno Setup 7.1.0 com verificação de SHA-256, compilação do `installer.iss` e upload do artefato.
- Contagem de testes nessa execução: 96 aprovados no binário de testes da biblioteca e 35 no binário de testes específico do alvo — **131 no total, 0 falhas, 0 ignorados**.
- Ambiente executado: runner `windows-2025` (Microsoft Windows Server 2025, imagem `windows-2025-vs2026`) e um Windows 11 local. **Windows 10 x64 não foi validado.**
- O instalador publicado é a variante template: não carrega ID de extensão do Chrome, então a integração não conecta até o usuário fornecer um ID válido na instalação.
- Artefatos de Actions não são releases: nenhuma release foi criada, nenhuma tag foi enviada e nada foi publicado na Chrome Web Store.
- Cobertura: **NOT MEASURED**.

## Verificação local anterior (histórico, parcialmente obsoleta)

Antes das mudanças de transferência segmentada, uma execução anterior reportou **PASS** em WSL2 Linux x86_64 com rustc/cargo 1.99.0 para: `cargo fmt --check`; `cargo test --locked --all-targets` (98 aprovados); `cargo clippy --all-targets --locked -- -D warnings`; e `cargo build --locked --release`.

Esses resultados vêm de uma execução anterior, não foram reproduzidos independentemente nesta revisão, não cobrem a implementação segmentada, a correção do fallback single-stream nem seus testes, e o número de 98 testes está desatualizado. Nenhuma contagem de testes Linux é reivindicada como verificada. A árvore atual foi exercitada pelo CI Windows run `39309557904`, que é uma execução Windows/MSVC e não reproduz o resultado WSL2.

## Native Messaging: claim exclusivo por item

A mudança de claim do inbox (marcador exclusivo por item, recuperação de marcador obsoleto, testes de concorrência de claimer, devolução de claim não enfileirado e retry limitado de remoção no Windows) foi compilada, formatada, lintada e executada pelo CI Windows run `39309557904` no runner `windows-2025`: o teste de stress de duplo claim exclusivo de Windows e os testes de concorrência, marcador obsoleto e permissão de marcador passaram (96 + 35 testes, 0 falhas, 0 ignorados).

Os mesmos testes permanecem não verificados em Linux, porque nenhuma execução local deles foi feita, e não verificados em Windows 10 x64, que nenhum ambiente executado cobriu.

## Gates pendentes

- Validação em Windows 10 x64.
- Comportamento de NTFS e ACLs herdadas.
- Registro de Native Messaging e comportamento de desinstalação no Windows.
- Slint/UI e comportamento de privilégio do Inno Setup.
- Auditoria de dependências (`Cargo.lock` é gerado pelo Cargo; revisão pendente).
- Varredura de segredos.
- Cobertura de testes (**NOT MEASURED**).
