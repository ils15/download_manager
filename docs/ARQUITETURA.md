# Arquitetura

Documento técnico do `download_manager`. O README fica deliberadamente curto; os detalhes de implementação vivem aqui.

## Visão geral

Aplicação de desktop Windows x64 com UI nativa Slint e motor de transferência em Rust. O binário `independent-download-manager` (pacote `independent-download-manager`, versão 0.1.0, edition 2021) contém tanto o executável principal quanto a biblioteca usada pelos testes. `build.rs` compila `ui/main.slint` em tempo de build via `slint-build`.

A aplicação nunca eleva privilégios, nunca escreve no registro do Windows e nunca invoca PowerShell ou shell. A única escrita no registro ocorre pelo instalador Inno Setup, que registra o host de Native Messaging do Chrome.

## Módulos

| Caminho | Papel |
|---|---|
| `src/main.rs` | Cola da aplicação: `slint::include_modules!()`, settings, histórico, polling do inbox, refresh da UI, acessibilidade e testes de integração da UI. |
| `src/lib.rs` | Raiz da biblioteca; reexporta `downloader`, `filename`, `integrity`, `native_messaging`, `queue`, `schedule`, `url_policy`. |
| `src/downloader_engine.rs` | Motor HTTP(S): requisições, segmentos, retry/backoff, token bucket, limites, retomada em processo, commit sem sobrescrita, limpeza. |
| `src/downloader_engine_segmented_tests.rs` | Testes do caminho multi-segmento contra servidor loopback local (sem rede externa). |
| `src/queue.rs` | Fila, prioridades, permissões de transferência, agendamento, snapshots de job, `retry_cleanup`. |
| `src/native_messaging.rs` | Host de Native Messaging (`--native-host`), inbox em disco, claim exclusivo por item, `rejected/`, limpeza. |
| `src/url_policy.rs` | Validação de URL HTTP(S), rejeição de userinfo e de chaves de query com aparência de autenticação. |
| `src/filename.rs` | Derivação e sanitização do nome de arquivo a partir da URL. |
| `src/integrity.rs` | Normalização e comparação do SHA-256 esperado. |
| `src/schedule.rs` | Janela diária UTC (`HH:MM`), incluindo janela de dia inteiro e janela que cruza a meia-noite. |
| `ui/main.slint` | Shell da UI: menu, toolbar, árvore de categorias, tabela virtualizada, barra de status, modais. |
| `extension/` | Extensão Chrome MV3: `manifest.json`, `popup.html`, `popup.js`. |
| `installer.iss` | Script Inno Setup do instalador por usuário. |

## UI (Slint)

- Layout clássico de gerenciador de downloads: barra de menu (Tarefas, Arquivo, Downloads, Visualizar, Ajuda, Registro), toolbar com botões grandes de ícone+rótulo (Adicionar, Iniciar, Parar, Parar (todos), Excluir, Excluir (todos), Opções, Agendar, Iniciar fila, Parar fila, Grabber, Indique), árvore de categorias redimensionável à esquerda e tabela virtualizada à direita com cabeçalho fixo (nome, tamanho, status, progresso, ETA, velocidade, prioridade).
- Barra de status com contadores ao vivo, velocidade agregada e detalhes do item selecionado.
- Adicionar download, opções de transferência, histórico e "sobre" são popups modais. URL, destino, prioridade, limite de segmentos, limite de velocidade por download, SHA-256 esperado e janela de agendamento UTC são validados no diálogo antes de a fila aceitar o job.
- Toolbar e menu refletem a seleção e o estado real da fila; todo controle é alcançável por teclado, com anel de foco visível, nome acessível e região viva para status e progresso.
- Ícones são glifos vetoriais originais desenhados em Slint. Nenhum asset ou marca de terceiros.
- Grabber, categoria de arquivo e registro do produto não são implementados: aparecem desabilitados com rótulo explícito de "não suportado", em vez de comportamento simulado.

## Motor de transferência

### Requisição e limites

- Cliente HTTP sem cookie jar, sem integração com navegador, sem extração de credenciais, sem autenticação e sem bypass de DRM/controle de acesso.
- Userinfo na URL e chaves de query com aparência de autenticação (tokens, assinaturas, API keys, sessões) são rejeitadas; parâmetros comuns continuam permitidos. O payload de Native Messaging rejeita campos desconhecidos.
- Redirects não são seguidos, inclusive same-origin. É uma escolha conservadora que pode tornar algumas URLs não suportadas.
- Limites configuráveis: máximo de 4096 MiB por download e 360 minutos no total por padrão, ambos ajustáveis no painel de limites de transferência dentro de faixas finitas validadas. Cada chunk de resposta é checado contra o teto agregado antes da escrita, incluindo corpos chunked sem `Content-Length`.
- Retry/backoff limitado para falhas transitórias.
- O deadline absoluto cobre transferência ativa, pausas/retomadas, retries e limpeza automática daquela sessão. Limpeza ainda pendente no deadline é reportada, não estende a transferência.

### Multi-segmento

- Habilitado apenas quando configurado acima do padrão conservador de 1 segmento (máximo `MAX_SEGMENT_COUNT = 4`).
- Para arquivos de pelo menos 8 MiB, uma sonda limitada `bytes=0-0` precisa estabelecer `206` válido, `Content-Length`/`Content-Range` exatos de um byte, tamanho total dentro do teto de bytes e ETag forte.
- Até quatro requisições independentes, cada uma com range verificado, exigem comprimentos, ranges, total e o mesmo ETag.
- Cada segmento escreve em seu próprio arquivo temporário criado exclusivamente; arquivos verificados são concatenados em ordem no handle de staging normal. O uso temporário de disco pode chegar ao dobro do tamanho final.
- Qualquer resposta de range não suportada ou inconsistente apaga o staging dos segmentos e cai para uma requisição single-stream nova, sem tocar no destino final.
- Corpo de range que termina antes do comprimento anunciado conta como inconsistência: a tentativa segmentada é abandonada, o staging é descartado e o single-stream decide o resultado. Se o corpo single-stream também for incompleto, a transferência falha como erro limpo, não publica destino e mantém apenas o parcial em memória para a limpeza local explícita.
- A quota total de conexões configurada inclui as conexões de segmento; pacing global de bytes, limite por download, teto de bytes e deadline absoluto se aplicam.
- Progresso agrega bytes escritos uma única vez e o ETA usa o tamanho total validado.
- Caminhos temporários de segmento permanecem registrados na sessão em memória para que a limpeza por timeout tente novamente após o cancelamento do worker.
- Segmentos nunca retomam após restart; estado parcial segmentado não é retido como validador de retomada.

### Retomada em processo e commit

- Uma transferência pausada retoma com `Range`/`If-Range` apenas enquanto o handle do arquivo parcial criado exclusivamente permanecer aberto no processo em execução. Escritas concluídas mantêm o offset contabilizado nesse handle; a pausa não faz flush e não promete durabilidade segura a crash.
- O commit final faz sync antes do link. Um deadline absoluto é armazenado quando a sessão inicia; tempo em pausa conta e a retomada não o renova.
- Retomada persistente/em disco é desabilitada de propósito: após sair do aplicativo, a transferência recomeça em vez de reabrir um caminho não confiável. Encerramento forçado do processo pode deixar um arquivo parcial de nome aleatório ignorado.
- A retomada em processo depende de fingerprint SHA-256 da URL exata, de um ETag forte e do offset mantido pela sessão de arquivo aberta; nenhum metadado de retomada ou URL crua é persistido. A resposta de range precisa corresponder ao início pedido, ao fim/total completo, ao validador e ao comprimento exato do corpo. Servidor sem ETag forte ainda permite download, mas não retomada segura.

### Paciência de banda (token bucket)

- Pacing aplicado por instância, compartilhado entre downloads concorrentes; atualiza imediatamente. Instâncias separadas não são coordenadas. Não é um limite instantâneo estrito de taxa de fio.
- Cada transferência ativa pode reter uma reserva de 16 KiB, então C transferências ativas podem reter até C × 16 KiB em aberto; o token bucket compartilhado pode reter mais 16 KiB. Cada transferência também pode reter um chunk de resposta cedido (tamanho controlado pelo transporte); buffer de cliente/SO fica fora desses limites.

## Fila

- Concorrência configurável de 1 a 8 jobs por instância. Mudanças de concorrência passam a valer após reiniciar; o limite por download vale para jobs enfileirados depois.
- Novos jobs entram como Normal; o job selecionado pode virar Low, Normal ou High.
- Quando uma permissão de transferência fica disponível, jobs em espera são escolhidos High → Normal → Low, com FIFO entre iguais. Downloads ativos não são preemptivos. Jobs agendados só entram na ordenação de permissões depois que a janela UTC abre.
- Prioridade e permissões são por instância, não compartilhadas entre processos. Não há aging: chegadas contínuas de prioridade High podem adiar trabalho Low indefinidamente.
- Entradas de fila expõem pausa, retomada, cancelamento, bytes recebidos reais e progresso por job.
- ETA é aproximado e só aparece com total validado e velocidade positiva estável. Respostas chunked ou de tamanho desconhecido mostram bytes sem ETA. Amostras de velocidade cobrem tempo de transferência ativa; pausar congela a estimativa (inclusive contra notificações de progresso em voo atrasadas) e retomar reinicia a base e esconde o ETA até novas amostras estáveis.
- A estimativa interna expira após quatro segundos ativos sem amostra nova; com o polling normal, a remoção aparece no refresh de um segundo seguinte (atrasos do scheduler podem estender isso). Texto acessível permanece disponível por job, com anúncios educados em mudanças de status e a cada passo de 10% de progresso, não a cada refresh.

## Agendamento

- Toggle opcional por download no formulário de adição; padrão desligado.
- Janelas válidas exigem `HH:MM` em 24 horas. Limites iguais significam dia inteiro; janelas que cruzam a meia-noite são suportadas.
- A fila recheca a janela após esperas por recurso e libera lock/permissão de transferência antes de aguardar a próxima ocorrência diária.
- Janelas fechadas são consultadas a cada 30 segundos, então uma abertura pode ser notada até ~30 segundos depois, antes de qualquer espera adicional por recurso.
- Hora local do Windows não é usada, evitando ambiguidade de horário de verão.

## Persistência

- Settings e histórico são gravados em temporários únicos `create_new` no mesmo diretório, com `sync_all`, e substituídos atomicamente (rename em Unix, `ReplaceFileW` no Windows quando o arquivo já existe). Falha de substituição antes do commit deixa o arquivo anterior intacto.
- O diretório pai não recebe fsync em toda plataforma/filesystem, então o save mais recente — incluindo a atualização de geração de limpeza de histórico — pode ser perdido após queda abrupta de energia. Durabilidade segura a crash não é prometida.
- Read-modify-write do histórico, captura da geração no enqueue e limpeza usam lock interprocessual por usuário e recarregam/sanitizam/limitam o arquivo atual antes de atualizar. Histórico legado só é removido depois que uma substituição válida é escrita e verificada; se ambos os arquivos (atual e legado) estiverem malformados, o startup reporta erro e preserva os bytes originais para recuperação.
- Diretórios de histórico em Unix usam modo 0700; dados, lock e temporários usam 0600. No Windows o histórico fica em LocalAppData e a ACL é herdada do diretório do perfil do usuário, sem verificação explícita.
- Saves concorrentes de settings continuam atômicos last-writer-wins, não mesclados.
- Entradas de fila terminadas são persistidas automaticamente em diretório dedicado sob o app-data por usuário (LocalAppData no Windows), limitado a 500 registros. Cada registro contém status final, apenas o basename do destino, bytes, SHA-256 quando disponível, horário UTC de término e origem+caminho da URL; credenciais, query strings, fragmentos e diretórios de destino não são armazenados.
- "Limpar histórico" é ação explícita do usuário: avança uma geração persistente para que downloads aceitos antes da limpeza não restaurem entradas antigas ao terminar, enquanto downloads aceitos depois continuam registráveis.
- Settings ausentes usam os padrões normais; settings ilegíveis, malformados ou inválidos mostram aviso de startup antes de aplicar os padrões.

## Native Messaging e inbox

- Nome do host: `org.independent.download_manager`; manifesto gerado pelo instalador em `native-host.json` com `type: stdio` e `args: ["--native-host"]`.
- A extensão lê a URL da aba ativa apenas após clique deliberado e envia somente `{ action, url }`; nunca lê conteúdo de página, cookies, credenciais ou estado de sessão. Chaves de query com aparência de autenticação são rejeitadas na extensão e no host nativo.
- O host aceita apenas uma requisição de URL HTTP(S) explícita e a coloca no inbox local one-shot do aplicativo. Se o app estiver rodando, ele consulta o inbox; caso contrário, o item é consumido no próximo start.
- Produtores escrevem um `incoming/*.tmp` único, fazem sync e renomeiam atomicamente para `*.json`; o poller ignora temporários. Temporários interrompidos não são consumidos. Recupere um `.tmp` apenas após confirmar que o produtor terminou e que nenhuma instância ainda escreve nele; nunca renomeie um temporário vivo. Depois inspecione e remova se incompleto, ou renomeie uma mensagem completa verificada para `.json` para tentar de novo.
- O polling é single-flight dentro de cada instância, e um marcador de claim exclusivo por item (`incoming/.<item>.claim`, criado com create exclusivo atômico) decide qual consumidor é dono de uma URL do inbox, impedindo que dois consumidores enfileirem a mesma URL.
- O vencedor move o item para uma pasta opaca `.processing-<id>` e libera o marcador; o perdedor ignora o item sem erro e um rename que falha libera o marcador para o item continuar reivindicável.
- Um consumidor que perde a corrida de claim depois de já ter movido uma cópia para a pasta de claim não deixa essa cópia órfã: a cópia volta ao inbox com o nome original e um poll posterior a reivindica. A devolução toma o nome atomicamente com hard link, então um item que outro consumidor já devolveu nunca é sobrescrito; a cópia que não consegue tomar o nome permanece recuperável na pasta de claim com o caminho de recuperação reportado.
- O marcador é exclusão mútua real: `create_new` exclusivo (`O_CREAT|O_EXCL` em Unix, `CREATE_NEW` no Windows), então exatamente um consumidor detém o item mesmo com várias corridas.
- No Windows, remoções de caminhos do inbox são repetidas um número limitado de vezes enquanto o SO ainda detém o caminho (códigos de erro 5, 32, 33 e 145); falha genuína é reportada sem retry.
- Residual conhecido: a recuperação de marcador obsoleto remove o marcador por caminho, então pode tomá-lo de um consumidor vivo; o efeito é reenfileirar, não perder, e a exclusão mútua do caminho normal permanece intacta. Marcador deixado por consumidor travado é tratado como resíduo e recuperado após limiar de dez minutos, com uma tentativa de recuperação por item e ciclo de poll.
- Um claim permanece na pasta até o aceite da fila; falha de enqueue devolve o item a `incoming`, enquanto falha na devolução mantém o claim e exibe o caminho de recuperação. Um destino duplicado só é reconhecido para correspondência exata de URL/destino nos estados Queued, Scheduled, Downloading, Paused ou Completed. Jobs Cancelled e FailedCleanupPending não se qualificam; jobs Failed são elegíveis para nova tentativa normal de enqueue.
- Itens malformados, não suportados, grandes demais ou rejeitados por política são movidos intactos para `rejected/` sob nomes opacos, com diagnósticos genéricos que omitem conteúdo e nome de arquivo de origem. Revise manualmente; só reenvie por ação explícita da extensão ou movendo uma mensagem corrigida para um novo nome em `incoming/<novo-id>.json`. Um crash em torno do aceite da fila é ambíguo e claims não são reproduzidos automaticamente. Depois de parar todas as instâncias, inspecione o estado de fila/saída antes de mover manualmente um `message.json` retido em `.processing` para um novo nome em `incoming/`; a recuperação manual pode causar download duplicado.
- Valores de query/fragmento são omitidos da URL visível na fila.

## Dependências

`Cargo.toml` declara: Slint/slint-build 1.8-compatível, reqwest 0.12 (`http2`, `rustls-tls`, `stream`, sem features padrão), Tokio 1, tokio-util 0.7, futures-util 0.3, serde/serde_json 1, URL 2, SHA-2 0.10, thiserror 2, UUID 1 (v4) e tempfile 3 para testes focados. `rust-version = "1.92"` declara o piso de MSRV compatível com Slint. O `Cargo.lock` gerado resolve Slint/slint-build para 1.18.1 e reqwest para 0.12.28.

Perfil de release: `lto = true`, `codegen-units = 1`, `panic = "abort"`, `strip = true`.

Testes de transferência segmentada usam apenas um servidor loopback local e não exigem rede externa.
