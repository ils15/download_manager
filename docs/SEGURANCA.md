# Segurança e fronteiras de comportamento

Este documento preserva as fronteiras de segurança declaradas pelo projeto. O checklist formal de revisão está em `SECURITY.md`.

## Fronteiras do cliente HTTP

- O cliente HTTP não tem cookie jar, integração com navegador, extração de credenciais, suporte a autenticação nem bypass de DRM/controle de acesso.
- Userinfo na URL e chaves de query com aparência de autenticação (tokens, assinaturas, API keys, sessões) são rejeitadas; parâmetros comuns continuam permitidos. O payload de Native Messaging rejeita campos desconhecidos.
- Redirects não são seguidos, inclusive same-origin. É intencionalmente conservador e pode tornar algumas URLs não suportadas.
- A seleção de URL é explícita; URLs fornecidas manualmente ainda podem apontar para serviços HTTP privados/locais. Envie apenas URLs confiáveis. Nenhum crawler/site-grabber é implementado.

## Integridade e resolução de nomes

- SHA-256 esperado é opcional na API de transferência e na UI. Campo vazio desabilita a checagem; entrada não vazia precisa ter exatamente 64 caracteres hexadecimais sem espaços (hex maiúsculo é aceito).
- O digest reportado é calculado a partir do handle do arquivo de staging; não é garantia contra um escritor local que corra com o commit por hard link baseado em pathname. Consumidores adversariais devem re-hashear o destino e/ou isolar o diretório.
- A validação cobre os bytes lidos do handle aberto. Um processo com permissão de escrita no diretório de destino pode substituir o pathname de staging entre o hash e o commit por hard link, então o destino final não é garantido contra esse atacante TOCTOU local. Integridade adversarial exige re-hash do destino e/ou isolamento externo.

## Retomada

- A retomada em processo está atrelada a um fingerprint SHA-256 da URL exata, a um ETag forte e ao offset de bytes mantido pela sessão de arquivo aberta; nenhum metadado de retomada ou URL crua é persistido.
- A resposta de range precisa corresponder ao início pedido, ao fim/total completo, ao validador e ao comprimento exato do corpo. Servidor sem ETag forte ainda permite download, mas não retomada segura.

## Saída e limites

- Downloads vão para a pasta `Downloads` do usuário. Nomes de saída são reduzidos a um leaf sanitizado; arquivo final existente não é sobrescrito.
- O máximo padrão é 4096 MiB por download e 360 minutos no total; ambos configuráveis no painel de limites de transferência dentro de faixas finitas validadas.
- Cada chunk de resposta é checado contra o teto agregado antes da escrita, incluindo corpos chunked sem `Content-Length`.

## Telemetria, clipboard e persistência

- Nenhuma telemetria é incluída.
- O campo de URL é manual; seu `LineEdit` padrão aceita colagem explícita com Ctrl+V apenas após o gesto do usuário. O app não monitora nem lê a área de transferência automaticamente.
- Settings e histórico são escritos em temporários únicos `create_new` no mesmo diretório, com `sync_all`, e substituídos atomicamente (rename em Unix, `ReplaceFileW` no Windows quando o arquivo já existe). Falha de substituição antes do commit deixa o arquivo anterior intacto, e a substituição atômica protege contra conteúdo parcial em falhas ordinárias.
- O diretório pai não recebe fsync em toda plataforma/filesystem, então o save mais recente — incluindo a atualização de geração de limpeza de histórico — pode ser perdido após queda abrupta de energia. Durabilidade segura a crash não é prometida.
- Read-modify-write do histórico, captura da geração no enqueue e operações de limpeza usam lock interprocessual por usuário, recarregando/sanitizando/limitando o arquivo atual antes de atualizar.
- Histórico legado só é removido após uma substituição válida ser escrita e verificada; se ambos os arquivos (atual e legado) estiverem malformados, o startup reporta erro e preserva os bytes originais para recuperação.
- Diretórios de histórico em Unix usam modo 0700; dados, lock e temporários usam 0600. No Windows o histórico fica em LocalAppData, com ACL herdada do diretório do perfil do usuário e sem verificação explícita.
- Saves concorrentes de settings permanecem atômicos last-writer-wins, não mesclados.

## Inbox de Native Messaging

- Produtores escrevem um `incoming/*.tmp` único, fazem sync e renomeiam atomicamente para `*.json`; o poller ignora temporários. Temporários interrompidos não são consumidos — recupere apenas após confirmar que o produtor terminou e nenhuma instância ainda escreve nele.
- O polling é single-flight por instância e um marcador de claim exclusivo por item (`incoming/.<item>.claim`, create exclusivo atômico) garante que consumidores concorrentes não enfileirem a mesma URL duas vezes. Detalhes completos do protocolo, recuperação de marcador obsoleto e retry de remoção no Windows estão em `docs/ARQUITETURA.md`.
- Itens malformados, não suportados, grandes demais ou rejeitados por política são movidos intactos para `rejected/` sob nomes opacos, com diagnósticos genéricos que omitem conteúdo e nome de arquivo de origem.
- Um crash em torno do aceite da fila é ambíguo e claims não são reproduzidos automaticamente. Recuperação manual pode causar download duplicado.
- Valores de query/fragmento são omitidos da URL visível na fila.

## Instalação e privilégios

- O script Inno Setup usa instalação por usuário por padrão. O instalador pode oferecer o override padrão all-users, que é onde o UAC do Windows é solicitado.
- O aplicativo em si nunca eleva nem acessa o registro. O instalador registra o host de Native Messaging do Chrome e gera `native-host.json`.

## Não implementado / requer revisão

- Site-grabber: não implementado (sem limites de endereço público, redirect/DNS-rebinding, página/profundidade/bytes).
- Transporte FTP ou MMS: não implementado ou revisado por compatibilidade.
- Integração com antivírus: não implementada; exigiria revisão de integração de fornecedor/SO e empacotamento.
- Notificações de desktop do SO e coordenação global entre instâncias para prioridade ou banda agregada: não implementadas. O agendamento por prioridade é por instância.
- Retomada persistente: nenhum metadado é gravado; retomada single-stream é local ao processo e atrelada a um handle já aberto. Transferências segmentadas não são retomadas por segmento.
- ETA é estimativa aproximada de transferência ativa, não garantia de conclusão.
