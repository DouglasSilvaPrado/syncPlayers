# Liga Virtual — CMTracker Sync

Adaptação do repositório `syncPlayers` (anteriormente SoFIFA). O projeto original não é copiado para evitar distribuir `.env`, `.git` e `venv` do ZIP enviado. O novo workflow substitui o antigo.

## Instalação

1. No repositório GitHub `DouglasSilvaPrado/syncPlayers`, faça backup e **desative** `SoFIFA Sync` em Actions para impedir escrita concorrente.
2. Copie **o conteúdo desta pasta** para a raiz do repositório e remova do GitHub o antigo `.github/workflows/sofifa-sync.yml` depois de validar a nova configuração.
3. Em GitHub → Settings → Secrets and variables → Actions → Repository secrets configure:
   - `CMTRACKER_API_KEY`: chave da API aprovada pelo CMTracker.
   - `SUPABASE_URL`: URL do projeto Supabase.
   - `SUPABASE_SERVICE_ROLE_KEY`: chave service_role (nunca exponha publicamente).
4. Em Actions → CMTracker Sync → Run workflow, selecione `inspect` e digite `26100100` como `db_slug` no primeiro teste. Veja nos logs os campos reais retornados.
5. Confira se a API usa `/players`, `db`, `page`, `pageSize`, autenticação `x-api-key` e se a resposta identifica o ID do EA. **Esses parâmetros são hipóteses configuráveis, não um contrato verificado.** Se necessário, altere o bloco `env` do workflow para definir `CMTRACKER_PLAYERS_PATH`, `CMTRACKER_*_PARAM`, `CMTRACKER_AUTH_HEADER`, `CMTRACKER_ID_IS_EA_ID`.
6. Execute `dry-run` e analise os contadores sem alterar o Supabase.
7. Somente após validar os valores e fazer backup, em GitHub → Settings → Secrets and variables → Actions → Variables adicione `CMTRACKER_ALLOW_WRITE=YES`; então `apply` realiza as alterações, e o agendamento dominical também grava.

## Segurança da migração

- **Por padrão somente simulação**; gravação exige `--apply` e `CMTRACKER_ALLOW_WRITE=YES`.
- Atualiza **apenas registros existentes** da tabela `players`, identificados por `sofifa_player_id` (que deve corresponder ao EA ID da API).
- Não modifica IDs internos, vínculos com times, contratos, salários, multas, carteiras, transações nem colunas fora da lista `FIELD_ALIASES`.
- Não cria jogadores novos automaticamente. Inserção de novos jogadores e ampliação dos atributos exigem mapeamento do JSON real e auditoria das regras de negócio.
- Semelhante ao original, processa páginas em lotes; limite inicial 20 páginas x 25 jogadores, configurável.
- `CMTRACKER_DB_SLUG` vazio tenta obter o maior slug numérico de 8 dígitos em `/dbs`.
- Se o formato da API não corresponder ao esperado, interrompe antes de gravar a página; valide os dados em simulação antes de habilitar escrita.
- Após trocar a versão do jogo, revise posições/atributos e os campos que mudaram antes de gravar.

## Executar localmente (PowerShell)

```powershell
python -m pip install -r requirements.txt
$env:CMTRACKER_API_KEY='SUA_CHAVE'
$env:CMTRACKER_DB_SLUG='26100100'
python sync_cmtracker.py --inspect
# Defina SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY no ambiente para dry-run:
python sync_cmtracker.py
# Só após teste e backup:
# $env:CMTRACKER_ALLOW_WRITE='YES'
# python sync_cmtracker.py --apply
```

## Notas importantes

- O endpoint `/dbs` é documentado visualmente como `GET /api/v1/dbs`; a forma exata do JSON e do endpoint de jogadores requerem confirmação com `--inspect` ou o portal privado da API.
- Nunca compartilhe `.env` ou service_role em ZIPs, commits, prints ou logs.
- Se o campo `id` do CMTracker for o EA player ID e não existir `eaPlayerId`, habilite `CMTRACKER_ID_IS_EA_ID=true` **somente depois de confirmar**.
- A API do CMTracker informa limites de 100 req/min e 25.000 req/mês. Ajuste número de páginas e intervalo para respeitá-los.

## Se a API responder HTTP 401

1. Confira que `CMTRACKER_API_KEY` foi criado como **Repository secret**, com a chave da API (não senha do portal). O nome deve ser exato.
2. Execute `inspect`. O log informa `Credencial CMTracker: configurada` sem revelar o segredo.
3. Caso continue 401, consulte a seção **Authentication** da documentação privada do CMTracker. Se exigir `Authorization: Bearer`, crie a **Repository variable** `CMTRACKER_AUTH_HEADER` com valor `authorization`. Se exigir `x-api-key`, mantenha o valor padrão. Não experimente formatos indiscriminadamente.
4. Confirme que a chave está ativa e aprovada para acessar `/api/v1/players`. Nunca cole a chave nos logs ou aqui.


## Autenticação e paginação (documentação CMTracker)

O endpoint oficial de jogadores é `GET https://api.cmtracker.net/api/v1/players?db=26100100&limit=25&page=0` com o cabeçalho HTTP `X-API-Key: <chave>`. O script já usa **X-API-Key** e **limit**, não `pageSize`.

Se aparecer `401 Unauthorized`, confira o valor do Secret `CMTRACKER_API_KEY` e confirme com o CMTracker que a chave tem acesso à versão escolhida. Não exponha a chave nem cole logs com secrets. `CMTRACKER_API_MODE`, `CMTRACKER_SYNC_SECRET` e `CMTRACKER_PROXY_SECRET` são usados pela integração interna da Liga Virtual e não fazem parte deste fluxo de consulta direta à API.

Após resolver o `401`, execute `inspect` para checar os campos retornados, depois `dry-run`. Não habilite `CMTRACKER_ALLOW_WRITE=YES` até validar o mapeamento e o resultado da simulação.
