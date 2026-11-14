# Uso

Fluxos detalhados do `download_manager`. Para requisitos e build, veja `README.md` e `docs/BUILD.md`.

## Adicionar um download

1. Abra o aplicativo e clique em **Adicionar**.
2. Informe a URL HTTP(S). URLs com userinfo embutido ou chaves de query com aparência de autenticação são rejeitadas; parâmetros comuns continuam permitidos.
3. Ajuste, se necessário, no diálogo: destino, prioridade, limite de segmentos, limite de velocidade por download (KB/s), SHA-256 esperado e janela de agendamento UTC. Todos os campos são validados antes de a fila aceitar o job.
4. Confirme. O destino padrão é a pasta `Downloads` do usuário; o nome final é reduzido a um leaf sanitizado e nunca sobrescreve um arquivo existente.

Padrões de settings carregados no startup: 3 downloads concorrentes, velocidade padrão por download `0` (ilimitada), pacing global `0` (ilimitado), 4096 MiB por download, 360 minutos no total e 1 segmento. Settings ausentes usam esses padrões; settings ilegíveis, malformados ou inválidos mostram aviso de startup antes de aplicar os padrões.

## Fila, prioridades e concorrência

- Concorrência de jobs por instância: 1 a 8. A mudança passa a valer após reiniciar o aplicativo.
- Novos jobs entram como Normal; o job selecionado pode ser Low, Normal ou High.
- Com uma permissão de transferência livre, a escolha é High → Normal → Low, com FIFO entre iguais. Downloads ativos não são preemptivos; jobs agendados só entram na ordenação depois de a janela UTC abrir.
- Prioridade e permissões são por instância, não compartilhadas entre processos. Não há aging: chegadas contínuas de prioridade High podem adiar trabalho Low indefinidamente.
- Ações disponíveis por entrada: pausa, retomada, cancelamento; a tabela mostra bytes recebidos reais e progresso.
- ETA é aproximado e só aparece com total validado e velocidade positiva estável. Respostas chunked ou de tamanho desconhecido mostram bytes sem ETA. Pausar congela a estimativa; retomar reinicia a base e esconde o ETA até novas amostras estáveis. A estimativa interna expira após quatro segundos ativos sem amostra nova.
- Barra de status: contadores ao vivo, velocidade agregada e detalhes do item selecionado.

## Limites de banda e de transferência

- Pacing global em bytes/s compartilhado por downloads concorrentes da mesma instância; `0` = ilimitado; atualiza imediatamente. Não é um limite instantâneo estrito de taxa de fio, e instâncias separadas não são coordenadas.
- Limite separado por download em KB/s, aplicado aos jobs enfileirados depois da mudança.
- Teto de bytes (padrão 4096 MiB) e duração absoluta (padrão 360 minutos), ambos ajustáveis dentro de faixas finitas validadas no painel de limites de transferência. O deadline absoluto inclui transferência ativa, pausas/retomadas, retries e limpeza automática; limpeza pendente no deadline é reportada, não estende a transferência.

## Segmentos

- Downloads multi-segmento são condicionais e ficam desligados no padrão de 1 segmento. Configurado acima disso (até 4), o motor exige arquivo de pelo menos 8 MiB, sonda `bytes=0-0` com `206` válido, `Content-Length`/`Content-Range` exatos de um byte, total dentro do teto e ETag forte.
- O uso temporário de disco pode chegar ao dobro do tamanho final.
- Qualquer inconsistência descarta o staging dos segmentos e refaz a transferência em fluxo único, sem tocar no destino final.
- Segmentos nunca retomam após pausa, falha ou restart; a tentativa single-stream posterior começa do zero.

## Pausa e retomada

- Retomar usa `Range`/`If-Range` apenas enquanto o handle do arquivo parcial criado exclusivamente permanecer aberto no processo em execução. A pausa não faz flush e não promete durabilidade a crash.
- O deadline absoluto é armazenado no início da sessão: pausa conta e retomada não renova.
- Retomada persistente em disco é desabilitada de propósito: após sair do aplicativo, a transferência recomeça. Encerramento forçado pode deixar um parcial de nome aleatório ignorado.
- Um servidor sem ETag forte permite download, mas não retomada segura.
- Se o destino final for commitado e a remoção do alias temporário falhar, o job ainda é reportado como Completed com diagnóstico explícito de limpeza pendente; falhas de limpeza por cancelamento também permanecem visíveis. A UI habilita **Retry pending cleanup** para esses estados e reporta o resultado; chamadores de fila/API podem usar `DownloadQueue::retry_cleanup`. Esse retry remove apenas o alias temporário e atualiza o estado local da sessão: não faz requisição de rede e pode ser usado após o deadline. O handle/caminho permanece sob posse até a remoção ter sucesso.

## Agendamento

- O toggle de agendamento no formulário de adição é opcional e vem desligado. Janelas exigem `HH:MM` em 24 horas; limites iguais significam dia inteiro, e janelas que cruzam a meia-noite são suportadas.
- A fila recheca a janela após esperas por recurso e libera lock/permissão antes de aguardar a próxima ocorrência diária.
- Janelas fechadas são consultadas a cada 30 segundos: uma abertura pode ser notada até ~30 segundos depois.
- Hora local do Windows não é usada, evitando ambiguidade de horário de verão.

## Histórico

- Entradas de fila terminadas são persistidas por usuário (LocalAppData no Windows), limitado a 500 registros.
- Cada registro guarda status final, apenas o basename do destino, bytes, SHA-256 disponível, horário UTC de término e origem+caminho da URL. Credenciais, query strings, fragmentos e diretórios de destino não são armazenados.
- **Limpar histórico** é ação explícita: avança uma geração persistente para que downloads aceitos antes da limpeza não restaurem entradas antigas ao terminar; downloads aceitos depois continuam registráveis.
- Histórico legado só é removido após uma substituição válida ser escrita e verificada. Se ambos os arquivos (atual e legado) estiverem malformados, o startup reporta erro e preserva os bytes originais para recuperação.

## Notificações e acessibilidade

- Conclusão e falha de transferência aparecem em região viva acessível dentro do app e podem ser dispensadas. Não há toast do SO, serviço de notificação nem telemetria.
- Todo controle é alcançável por teclado, com anel de foco visível e nome acessível. Anúncios de status e progresso são educados e ocorrem em mudanças de status e a cada passo de 10% de progresso, não a cada refresh.
- O campo de URL aceita colagem explícita com Ctrl+V apenas após o gesto do usuário; o app não monitora nem lê a área de transferência automaticamente.

## Configuração do Chrome

O instalador do CI é um template. Para gerar a entrada de origem permitida e registrar o host:

1. Informe um ID válido de extensão do Chrome na página do instalador.
2. Para desenvolvimento com extensão desempacotada, se o ID ainda não for conhecido: instale uma vez com o campo em branco, carregue `{app}\extension` como extensão desempacotada, leia o ID em `chrome://extensions` e rode o instalador novamente com o mesmo modo (usuário/máquina) e caminho de instalação, informando esse ID.
3. A instalação com ID em branco grava o placeholder e não conecta até ser substituída por um ID explícito. Um ID de desenvolvimento desempacotado é suficiente; um ID futuro da Store também serve, mas publicação na Web Store não é necessária.
4. Para extensões desempacotadas, use o ID da pasta `{app}\extension` instalada.

O instalador é o dono do registro de Native Messaging do Chrome; o app e a extensão não modificam o registro. O host aceita apenas uma requisição de URL HTTP(S) explícita e a coloca no inbox local one-shot do app. Com o app rodando, ele consulta o inbox; caso contrário, o item é consumido no próximo start.

A extensão usa apenas as permissões `activeTab` e `nativeMessaging`, lê a URL da aba ativa somente após um clique deliberado e envia somente `{ action, url }`. Ela nunca lê conteúdo de página, cookies, credenciais ou estado de sessão.

## Recuperação do inbox

- Um `*.tmp` interrompido não é consumido. Recupere apenas após confirmar que o produtor terminou e que nenhuma instância ainda escreve nele; nunca renomeie um temporário vivo. Inspecione e remova se incompleto, ou renomeie uma mensagem completa verificada para `.json` para tentar de novo.
- Itens rejeitados vão intactos para `rejected/` sob nomes opacos, com diagnósticos que omitem conteúdo e nome de origem. Revise manualmente; só reenvie por ação explícita da extensão ou movendo uma mensagem corrigida para um novo nome em `incoming/<novo-id>.json`.
- Um crash em torno do aceite da fila é ambíguo e claims não são reproduzidos automaticamente. Após parar todas as instâncias, inspecione o estado de fila/saída antes de mover manualmente um `message.json` retido em `.processing` para um novo nome em `incoming/`; a recuperação manual pode causar download duplicado.
- Um claim permanece na pasta até o aceite da fila; falha de enqueue devolve o item a `incoming`, e falha na devolução mantém o claim exibindo o caminho de recuperação.

## Não implementado

- Site-grabber: não implementado; limites de endereço público, redirect/DNS-rebinding, página/profundidade/bytes estão ausentes.
- Transporte FTP ou MMS: não implementado ou revisado por compatibilidade.
- Integração com antivírus: não implementada; exigiria revisão de integração de fornecedor/SO e empacotamento.
- Notificações de desktop do SO e coordenação global entre instâncias para prioridade ou banda agregada. Prioridade por instância existe conforme descrito acima.
- Grabber, categoria de arquivo e registro do produto aparecem desabilitados na UI com rótulo explícito de "não suportado", em vez de comportamento simulado.
- ETA é uma estimativa aproximada de transferência ativa, não garantia de conclusão: exige amostras positivas repetidas por pelo menos dois segundos e um total validado, com corte interno de quatro segundos ativos sem amostra nova.
