# Exercicios: Concorrencia Avancada em Go

Estes exercicios estendem o projeto `portfolio-api` com foco nos binarios `event-watcher`
e `snapshot-runner`. Cada exercicio pede que voce modifique ou estenda arquivos reais
do codebase. Trabalhe na ordem — exercicios posteriores assumem que os anteriores
foram concluidos.

Antes de comecar, certifique-se de que o projeto compila e os testes existentes passam:

```bash
go build ./...
go test ./...
```

---

## Sumario

| #  | Titulo | Dificuldade | Conceitos-Chave |
|----|--------|-------------|-----------------|
| 1  | [Adicionar um consumer de alerta ao fan-out](#exercicio-1-adicionar-um-consumer-de-alerta-ao-fan-out) | Iniciante | Fan-out broadcast, channels direcionais, `select` |
| 2  | [Adicionar coluna `total_usd` ao snapshot](#exercicio-2-adicionar-coluna-total_usd-ao-snapshot) | Iniciante | Pipeline de dados, agregacao, migracao SQL |
| 3  | [Contar eventos por token no metrics worker](#exercicio-3-contar-eventos-por-token-no-metrics-worker) | Iniciante | `select` com multiplos canais, `map` como acumulador |
| 4  | [Rate limiter para o worker pool](#exercicio-4-rate-limiter-para-o-worker-pool) | Intermediario | `time.Ticker` como semaforo, bounded concurrency |
| 5  | [Retry com backoff exponencial no LogsFetcher](#exercicio-5-retry-com-backoff-exponencial-no-logsfetcher) | Intermediario | Decorator pattern, `time.After`, `select` com `ctx.Done()` |
| 6  | [Teste de integracao do pipeline do watcher](#exercicio-6-teste-de-integracao-do-pipeline-do-watcher) | Intermediario | Mocks, pipeline end-to-end, channel draining |
| 7  | [Stage de deduplicacao no watcher](#exercicio-7-stage-de-deduplicacao-no-watcher) | Intermediario | `concurrent.Stage`, map como filtro stateful |
| 8  | [Worker pool dinamico baseado em queue depth](#exercicio-8-worker-pool-dinamico-baseado-em-queue-depth) | Avancado | Goroutines dinamicas, `len(ch)`/`cap(ch)`, `sync.WaitGroup` |
| 9  | [Substituir polling por WebSocket](#exercicio-9-substituir-polling-por-websocket) | Avancado | `gorilla/websocket`, `eth_subscribe`, composicao de canais |
| 10 | [Timeout por wallet no snapshot runner](#exercicio-10-timeout-por-wallet-no-snapshot-runner) | Avancado | `context.WithTimeout`, partial failure, `select` |
| 11 | [Benchmark comparando tamanhos de worker pool](#exercicio-11-benchmark-comparando-tamanhos-de-worker-pool) | Avancado | `testing.B`, benchmark parametrizado, analise de throughput |
| 12 | [Modo "diff" no snapshot runner (Desafio)](#exercicio-12-modo-diff-no-snapshot-runner-desafio) | Avancado | Pipeline composto, comparacao de snapshots, fan-in |

---

## Exercicio 1: Adicionar um consumer de alerta ao fan-out

**Dificuldade:** Iniciante

### Conceito

O `FanOut` em `internal/concurrent/fanout.go` transmite cada evento para **todos**
os consumers registrados (broadcast). Atualmente o watcher tem 3 consumers: `persistWorker`,
`logWorker` e `metricsWorker`. Neste exercicio voce adicionara um quarto consumer
que funciona como um sistema de alertas simples.

O ponto-chave e entender que adicionar um consumer ao fan-out e trivial: basta
aumentar o `numConsumers` na chamada a `concurrent.FanOut` e consumir o canal
adicional. Cada consumer recebe **todos** os eventos independentemente dos demais.

### Instrucoes

1. **`internal/watcher/watcher.go`** — No metodo `Run`, altere a chamada a `FanOut`
   de 3 para 4 consumers:

   ```go
   consumers := concurrent.FanOut(ctx, normalized, 4)
   ```

2. **`internal/watcher/watcher.go`** — Adicione o novo consumer:

   ```go
   go w.alertWorker(ctx, consumers[3])
   ```

3. **`internal/watcher/watcher.go`** — Implemente o metodo `alertWorker`:

   ```go
   func (w *Watcher) alertWorker(_ context.Context, events <-chan *domain.WalletEvent) {
       const threshold = 1000.0 // USD threshold — ajuste conforme necessario

       for event := range events {
           // Parse o amount do evento e compare com o threshold.
           // Se exceder, logue um alerta.
       }
       log.Println("watcher: alert worker done")
   }
   ```

   O worker deve parsear `event.Amount` (que e uma string, ex: `"1500.250000"`) usando
   `strconv.ParseFloat`, e logar uma mensagem de alerta quando o valor exceder o threshold.

### Dica

- Observe que o `alertWorker` **nao** fecha o canal — ele e um consumer. O canal e
  criado e fechado pelo `FanOut`. Essa e a regra de ownership: quem cria o canal e
  responsavel por fecha-lo.
- Use `strconv.ParseFloat(event.Amount, 64)` para converter o amount. Trate erros
  de parse silenciosamente com `continue`.
- O alerta pode ser um simples `log.Printf` com prefixo `[ALERT]`.

### Validacao

- `go build ./...` compila sem erros.
- Execute o `event-watcher` com wallets cadastradas no banco. Quando um Transfer
  com valor alto for detectado, a mensagem `[ALERT]` deve aparecer no log.
- Verifique que os outros 3 workers (persist, log, metrics) continuam funcionando
  normalmente — o novo consumer nao deve afetar os demais.
- Escreva um teste unitario que cria um canal, envia um evento com `Amount: "2000.000000"`
  e verifica que o `alertWorker` nao entra em deadlock (o canal fecha apos o envio).

---

## Exercicio 2: Adicionar coluna `total_usd` ao snapshot

**Dificuldade:** Iniciante

### Conceito

O snapshot runner em `internal/snapshot/runner.go` ja calcula o `USDValue` de cada
`WalletSnapshotItem`, mas nao armazena o total agregado no registro `WalletSnapshot`.
Neste exercicio voce adicionara um campo `TotalUSD` ao domain e uma coluna correspondente
na tabela do banco de dados.

Este exercicio reforça como dados fluem pelo pipeline: os resultados do worker pool
sao agregados no stage 4 (coleta), e e nesse ponto que voce calculara o total.

### Instrucoes

1. **`migrations/005_add_total_usd.sql`** — Crie uma nova migracao:

   ```sql
   ALTER TABLE wallet_snapshots ADD COLUMN total_usd DOUBLE PRECISION NOT NULL DEFAULT 0;
   ```

2. **`internal/domain/snapshot.go`** — Adicione o campo ao struct:

   ```go
   type WalletSnapshot struct {
       // ... campos existentes ...
       TotalUSD      float64              `json:"total_usd" db:"total_usd"`
   }
   ```

3. **`internal/snapshot/runner.go`** — No stage 4 (apos o loop `for result := range resultCh`),
   calcule o total somando `USDValue` de todos os items:

   ```go
   var totalUSD float64
   for _, item := range allItems {
       totalUSD += item.USDValue
   }
   snapshot.TotalUSD = totalUSD
   ```

4. **`internal/repository/postgres/snapshot_repository.go`** — Atualize as queries SQL
   para incluir a nova coluna `total_usd` no `INSERT` e no `UPDATE` do status.

### Dica

- A soma deve acontecer **depois** que todos os resultados do worker pool foram coletados,
  ou seja, apos o `for result := range resultCh` terminar. Esse e o ponto onde o fan-in
  ja convergiu todos os resultados em uma unica goroutine.
- Lembre-se de que o canal `resultCh` so fecha quando **todos** os workers terminam
  (via `sync.WaitGroup` dentro de `RunWorkerPool`). Entao quando o `range` termina,
  voce tem a garantia de que todos os dados estao em `allItems`.

### Validacao

- Execute a migracao: `psql $DATABASE_URL -f migrations/005_add_total_usd.sql`.
- Execute `go run ./cmd/snapshot-runner` e verifique no JSON de saida que o campo
  `total_usd` aparece com a soma correta dos `usd_value` de todos os items.
- Consulte o banco: `SELECT id, total_usd, status FROM wallet_snapshots ORDER BY created_at DESC LIMIT 1;`
  e confirme que o valor esta persistido.

---

## Exercicio 3: Contar eventos por token no metrics worker

**Dificuldade:** Iniciante

### Conceito

O `metricsWorker` em `internal/watcher/watcher.go` ja conta eventos por `Direction`
(incoming/outgoing) usando um `select` que multiplexa entre o canal de eventos e um
`time.Ticker`. Neste exercicio voce adicionara contagem por `TokenSymbol`, aprendendo
a usar um `map` como acumulador dentro de um loop com `select`.

### Instrucoes

1. **`internal/watcher/watcher.go`** — No `metricsWorker`, adicione um mapa para
   contar por token:

   ```go
   byToken := make(map[string]int)
   ```

2. Dentro do `case event, ok := <-events:`, apos o switch de direction, incremente
   o contador do token:

   ```go
   byToken[event.TokenSymbol]++
   ```

3. Nos tres pontos onde as metricas sao logadas (canal fechado, ticker, ctx.Done),
   inclua o mapa de tokens na mensagem:

   ```go
   log.Printf("watcher: [METRICS] incoming=%d outgoing=%d total=%d byToken=%v",
       incoming, outgoing, incoming+outgoing, byToken)
   ```

### Dica

- O `map[string]int` e seguro aqui porque apenas **uma unica goroutine** (o metricsWorker)
  le e escreve nele. Se multiplas goroutines precisassem acessar o mapa, voce precisaria
  de um `sync.Mutex` ou `sync.Map`. Esse e um conceito importante: dados confinados a
  uma unica goroutine nao precisam de sincronizacao.
- O `%v` no `log.Printf` imprime o mapa no formato `map[USDC:5 USDT:3]`, que e
  suficiente para observabilidade basica.

### Validacao

- `go build ./...` compila sem erros.
- Execute o event-watcher e aguarde pelo menos um ciclo do ticker (15 segundos).
  A mensagem de metricas deve incluir `byToken=map[...]` com os simbolos dos tokens
  detectados.
- Verifique que o mapa mostra `USDC`, `USDT`, e `UNKNOWN` conforme os tokens dos
  eventos recebidos.

---

## Exercicio 4: Rate limiter para o worker pool

**Dificuldade:** Intermediario

### Conceito

O `RunWorkerPool` em `internal/concurrent/workerpool.go` limita a concorrencia pelo
numero de goroutines (`numWorkers`), mas nao limita a **taxa** de requests por segundo.
Por exemplo, com 5 workers, se cada request leva 100ms, voce faz ~50 requests/segundo.
Mas se cada request leva 10ms, voce faz ~500 requests/segundo — o que pode exceder
o rate limit de um RPC como Infura ou Alchemy.

Neste exercicio voce implementara um rate limiter usando `time.Ticker` como semaforo:
antes de processar cada item, o worker espera um "tick", garantindo um intervalo minimo
entre requests.

### Instrucoes

1. **`internal/concurrent/workerpool.go`** — Crie uma nova funcao `RunWorkerPoolWithRateLimit`:

   ```go
   func RunWorkerPoolWithRateLimit[I any, O any](
       ctx context.Context,
       numWorkers int,
       ratePerSecond int,
       input <-chan I,
       process func(context.Context, I) O,
   ) <-chan O {
       output := make(chan O, numWorkers)
       ticker := time.NewTicker(time.Second / time.Duration(ratePerSecond))

       var wg sync.WaitGroup
       wg.Add(numWorkers)

       for i := range numWorkers {
           go func(workerID int) {
               defer wg.Done()
               for item := range input {
                   // Espera pelo tick antes de processar.
                   select {
                   case <-ticker.C:
                   case <-ctx.Done():
                       return
                   }

                   select {
                   case <-ctx.Done():
                       return
                   default:
                   }

                   result := process(ctx, item)

                   select {
                   case output <- result:
                   case <-ctx.Done():
                       return
                   }
               }
           }(i)
       }

       go func() {
           wg.Wait()
           ticker.Stop()
           close(output)
       }()

       return output
   }
   ```

2. **`internal/snapshot/runner.go`** — Substitua a chamada a `RunWorkerPool` por
   `RunWorkerPoolWithRateLimit` com um rate de 5 requests/segundo (ou um valor
   configuravel via parametro do `Runner`).

### Dica

- O `time.Ticker` funciona como um "token bucket" simplificado: cada tick libera
  um token, e os workers competem pelos ticks. Como todos os workers compartilham
  o mesmo ticker, no maximo `ratePerSecond` requests serao iniciadas por segundo,
  independente do numero de workers.
- Atencao: com `ratePerSecond=5` e `numWorkers=5`, cada worker fara em media
  1 request/segundo. Se `numWorkers > ratePerSecond`, alguns workers ficarao ociosos
  na maioria dos ciclos — isso e esperado e nao e um problema.
- Nao esqueca do `ticker.Stop()` na goroutine de cleanup para evitar leak de recursos.

### Validacao

- Escreva um teste em `internal/concurrent/workerpool_test.go` que:
  - Cria um `RunWorkerPoolWithRateLimit` com `ratePerSecond=10` e 3 workers.
  - Envia 10 items pelo canal de entrada.
  - Mede o tempo total de processamento e verifica que levou **pelo menos** 900ms
    (10 items / 10 por segundo = ~1 segundo).
  - Verifica que todos os 10 resultados foram recebidos no canal de saida.
- Execute `go test ./internal/concurrent/ -run TestRunWorkerPoolWithRateLimit -v`.

---

## Exercicio 5: Retry com backoff exponencial no LogsFetcher

**Dificuldade:** Intermediario

### Conceito

O `EthereumLogsFetcher` em `internal/provider/blockchain/ethereum_logs.go` faz uma
unica tentativa de chamada RPC. Se a chamada falhar por um erro transiente (rede
instavel, RPC temporariamente indisponivel), o watcher simplesmente loga o erro e
espera o proximo ciclo do ticker — potencialmente perdendo eventos.

Neste exercicio voce criara um decorator `RetryLogsFetcher` que envolve qualquer
`contracts.LogsFetcher` e adiciona retry com backoff exponencial. Isso usa o padrao
decorator: o wrapper implementa a mesma interface que o inner, adicionando comportamento.

### Instrucoes

1. **Crie `internal/provider/blockchain/retry_logs.go`** com o struct:

   ```go
   type RetryLogsFetcher struct {
       inner      contracts.LogsFetcher
       maxRetries int
       baseDelay  time.Duration
   }

   func NewRetryLogsFetcher(inner contracts.LogsFetcher, maxRetries int, baseDelay time.Duration) *RetryLogsFetcher {
       if inner == nil {
           panic("blockchain.NewRetryLogsFetcher: inner must not be nil")
       }
       return &RetryLogsFetcher{inner: inner, maxRetries: maxRetries, baseDelay: baseDelay}
   }
   ```

2. Implemente `FetchLogs` com retry:

   ```go
   func (f *RetryLogsFetcher) FetchLogs(ctx context.Context, addresses []string, fromBlock uint64) ([]json.RawMessage, uint64, error) {
       var lastErr error
       for attempt := range f.maxRetries + 1 {
           logs, block, err := f.inner.FetchLogs(ctx, addresses, fromBlock)
           if err == nil {
               return logs, block, nil
           }
           lastErr = err
           if attempt == f.maxRetries {
               break
           }

           delay := f.baseDelay * time.Duration(1<<uint(attempt))
           log.Printf("retry_logs: attempt %d/%d failed: %v — retrying in %s",
               attempt+1, f.maxRetries+1, err, delay)

           select {
           case <-time.After(delay):
           case <-ctx.Done():
               return nil, 0, fmt.Errorf("retry cancelled: %w", ctx.Err())
           }
       }
       return nil, 0, fmt.Errorf("all %d attempts failed: %w", f.maxRetries+1, lastErr)
   }
   ```

3. **`cmd/event-watcher/main.go`** — Envolva o `logsFetcher` com o retry:

   ```go
   logsFetcher := blockchain.NewEthereumLogsFetcher(cfg.EthRPCURL)
   retryFetcher := blockchain.NewRetryLogsFetcher(logsFetcher, 3, 1*time.Second)
   // Use retryFetcher no lugar de logsFetcher ao criar o Watcher.
   ```

### Dica

- O `select` com `time.After` e `ctx.Done()` e essencial: se o contexto for cancelado
  durante o backoff (ex: SIGTERM), o retry para imediatamente em vez de esperar o delay
  completo.
- O backoff exponencial funciona assim: attempt 0 = 1s, attempt 1 = 2s, attempt 2 = 4s.
  A formula e `baseDelay * 2^attempt`, implementada com bit shift: `1 << uint(attempt)`.
- Em producao voce adicionaria jitter (variacao aleatoria) para evitar thundering herd.
  Isso esta fora do escopo deste exercicio, mas adicione um comentario mencionando isso.

### Validacao

- Escreva testes em `internal/provider/blockchain/retry_logs_test.go`:
  - `TestRetryLogsFetcher_SucceedsFirstAttempt`: inner retorna sucesso, verify 1 chamada.
  - `TestRetryLogsFetcher_RetriesOnError`: inner falha 2 vezes e depois retorna sucesso.
    Verifique 3 chamadas no total. Use `baseDelay` de 1ms para o teste ser rapido.
  - `TestRetryLogsFetcher_ExhaustsRetries`: inner sempre falha. Verifique que o erro
    final contem `"all 4 attempts failed"`.
  - `TestRetryLogsFetcher_RespectsContextCancellation`: cancele o contexto antes do
    segundo retry. Verifique que retorna imediatamente com `context.Canceled`.
- Use um mock que conta chamadas via campo `calls int` no struct.

---

## Exercicio 6: Teste de integracao do pipeline do watcher

**Dificuldade:** Intermediario

### Conceito

O watcher tem um pipeline de 3 stages: poller -> normalizer -> fan-out -> consumers.
Testar cada stage isoladamente e importante, mas um teste de integracao que verifica
o fluxo completo garante que os canais estao conectados corretamente e que o close
se propaga pela pipeline inteira.

Neste exercicio voce montara o pipeline com mocks e verificara que um log raw
inserido no inicio chega ate o consumer final como um `WalletEvent` normalizado.

### Instrucoes

1. **Crie `internal/watcher/watcher_integration_test.go`** com o teste
   `TestWatcherPipeline_EndToEnd`.

2. Crie um mock `mockLogsFetcher` que retorna uma lista fixa de logs raw no formato
   JSON. Use o formato do `RawLog`:

   ```go
   rawLog := watcher.RawLog{
       TxHash:      "0xabc123",
       BlockNumber: "0xa",
       Address:     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", // USDC
       Topics: []string{
           "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
           "0x0000000000000000000000001111111111111111111111111111111111111111", // from
           "0x0000000000000000000000002222222222222222222222222222222222222222", // to (tracked)
       },
       Data:    "0x00000000000000000000000000000000000000000000000000000000000f4240", // 1000000 (1 USDC)
       Removed: false,
   }
   ```

3. Monte o pipeline manualmente (sem usar `Watcher.Run`):

   ```go
   ctx, cancel := context.WithCancel(context.Background())
   defer cancel()

   trackedAddresses := map[string]string{
       "0x2222222222222222222222222222222222222222": "wallet-1",
   }

   // Stage 1: gere os raw logs no canal
   rawCh := concurrent.Generate(ctx, rawLogs)

   // Stage 2: normalize
   normalized := concurrent.Stage(ctx, rawCh, func(_ context.Context, raw watcher.RawLog) (*domain.WalletEvent, bool) {
       event, err := watcher.NormalizeTransferLog(raw, trackedAddresses)
       if err != nil || event == nil {
           return nil, false
       }
       return event, true
   })

   // Stage 3: fan-out para 1 consumer (simplificado para o teste)
   consumers := concurrent.FanOut(ctx, normalized, 1)

   // Colete os resultados
   var received []*domain.WalletEvent
   for event := range consumers[0] {
       received = append(received, event)
   }
   ```

4. Verifique que o evento recebido tem os campos corretos: `WalletID == "wallet-1"`,
   `Direction == "incoming"`, `TokenSymbol == "USDC"`, `TxHash == "0xabc123"`.

### Dica

- O ponto-chave deste teste e verificar a **propagacao de close**: quando `Generate`
  fecha o canal de entrada, o `Stage` processa os items restantes e fecha seu canal
  de saida, que por sua vez faz o `FanOut` fechar os canais dos consumers. O `range`
  no consumer termina naturalmente.
- Se o teste travar (deadlock), e provavel que algum canal nao esteja sendo fechado
  corretamente. Adicione um `context.WithTimeout` como safety net:
  ```go
  ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
  ```
- Voce nao precisa de um mock do `EventRepository` porque o teste nao chama `persistWorker`.

### Validacao

- `go test ./internal/watcher/ -run TestWatcherPipeline_EndToEnd -v` passa.
- O teste termina em menos de 1 segundo (nao ha polling envolvido).
- Adicione um segundo raw log com `Removed: true` e verifique que ele e filtrado
  pelo normalizer (nao aparece em `received`).

---

## Exercicio 7: Stage de deduplicacao no watcher

**Dificuldade:** Intermediario

### Conceito

Em blockchains, e possivel receber o mesmo evento mais de uma vez — por exemplo,
quando o poller consulta blocos que se sobrepoe ao intervalo anterior, ou durante
uma reorganizacao de cadeia. Neste exercicio voce adicionara um stage de pipeline
que filtra eventos duplicados baseado no `TxHash`.

O `concurrent.Stage` suporta filtragem nativamente: quando a funcao de transformacao
retorna `(_, false)`, o item e descartado. Voce usara um `map[string]bool` para
rastrear quais `TxHash` ja foram vistos.

### Instrucoes

1. **`internal/watcher/watcher.go`** — No metodo `Run`, adicione um stage de
   deduplicacao entre o normalizer e o fan-out:

   ```go
   // Stage 2: Normalizer
   normalized := concurrent.Stage(ctx, rawLogs, func(_ context.Context, raw RawLog) (*domain.WalletEvent, bool) {
       // ... (codigo existente) ...
   })

   // Stage 2.5: Deduplication (NOVO)
   seen := make(map[string]bool)
   deduplicated := concurrent.Stage(ctx, normalized, func(_ context.Context, event *domain.WalletEvent) (*domain.WalletEvent, bool) {
       if seen[event.TxHash] {
           log.Printf("watcher: duplicate tx=%s — skipping", truncate(event.TxHash, 10))
           return nil, false
       }
       seen[event.TxHash] = true
       return event, true
   })

   // Stage 3: Fan-Out (agora lê de deduplicated)
   consumers := concurrent.FanOut(ctx, deduplicated, 3)
   ```

2. Atualize o diagrama ASCII no comentario do `Run` para incluir o novo stage.

### Dica

- O `map[string]bool` e seguro sem `sync.Mutex` porque o `Stage` executa a funcao
  de transformacao em uma **unica goroutine**. Olhe a implementacao de `Stage` em
  `internal/concurrent/pipeline.go`: ha apenas um `go func()` que processa items
  sequencialmente. Se houvesse multiplas goroutines, voce precisaria de sincronizacao.
- Em producao, o mapa `seen` cresceria indefinidamente. Um exercicio bonus seria
  limitar o tamanho do mapa (ex: manter apenas os ultimos 10.000 hashes) ou usar
  um TTL. Por ora, o mapa simples e suficiente.
- A closure captura `seen` por referencia, entao o mapa persiste entre chamadas
  da funcao de transformacao.

### Validacao

- Escreva um teste em `internal/watcher/watcher_test.go` (ou o integration test
  do Exercicio 6) que envia 3 raw logs pelo pipeline, dois dos quais tem o mesmo
  `TxHash`. Verifique que apenas 2 eventos saem do stage de deduplicacao.
- Verifique no log que a mensagem `"duplicate tx=... — skipping"` aparece para
  o evento duplicado.
- `go test ./internal/watcher/ -v` passa.

---

## Exercicio 8: Worker pool dinamico baseado em queue depth

**Dificuldade:** Avancado

### Conceito

O `RunWorkerPool` usa um numero fixo de workers. Isso e simples, mas nao se adapta
a carga variavel. Neste exercicio voce implementara um worker pool que ajusta o
numero de goroutines dinamicamente:

- Se o canal de entrada esta > 80% cheio, adiciona um worker (escalar para cima).
- Se o canal de entrada esta < 20% cheio, remove um worker (escalar para baixo).
- Respeita limites minimo (1) e maximo (configuravel).

Isso ensina como gerenciar o ciclo de vida de goroutines dinamicamente usando
`context.CancelFunc` individual por worker e um monitor separado.

### Instrucoes

1. **Crie `internal/concurrent/dynamic_pool.go`** com:

   ```go
   func RunDynamicWorkerPool[I any, O any](
       ctx context.Context,
       minWorkers, maxWorkers int,
       input chan I,  // NOTA: chan I (bidirecional) — precisamos de len() e cap()
       process func(context.Context, I) O,
   ) <-chan O
   ```

2. A estrategia interna:
   - Mantenha uma lista de `cancelFunc` por worker ativo.
   - Uma goroutine "monitor" verifica `len(input)` e `cap(input)` a cada 500ms.
   - Se `len(input) > cap(input)*80/100` e workers atuais < maxWorkers: inicie um novo
     worker com seu proprio `context.WithCancel`.
   - Se `len(input) < cap(input)*20/100` e workers atuais > minWorkers: cancele o
     context do ultimo worker adicionado.
   - Use `sync.WaitGroup` para esperar todos os workers antes de fechar o output.

3. **Importante**: cada worker deve respeitar **seu proprio** context de cancelamento
   (para scale-down), mas tambem o context pai (para shutdown geral).

### Dica

- O parametro `input` precisa ser `chan I` (bidirecional) em vez de `<-chan I` porque
  `len()` e `cap()` so funcionam em canais bidirecionais ou com a direcao correta.
  Na pratica, `len()` e `cap()` funcionam em `<-chan` tambem, mas o tipo bidirecional
  deixa a intencao mais clara.
- Para cancelar um worker especifico sem afetar os outros, use um context derivado
  por worker: `workerCtx, workerCancel := context.WithCancel(ctx)`. Quando voce
  chama `workerCancel()`, apenas aquele worker para.
- Cuidado com race conditions ao acessar a lista de workers. Use um `sync.Mutex`
  para proteger o slice de cancel functions.
- Um worker que tem seu context cancelado deve sair do loop `for item := range input`
  graciosamente via `select` com `workerCtx.Done()`.

### Validacao

- Escreva um teste em `internal/concurrent/dynamic_pool_test.go` que:
  - Cria um canal de entrada com buffer 100.
  - Envia 90 items de uma vez (> 80% do buffer).
  - Verifica que workers adicionais sao criados (logue o numero atual de workers).
  - Apos processar todos os items, verifica que workers sao removidos.
  - Verifica que todos os 90 resultados sao recebidos no canal de saida.
- Use `process` functions com `time.Sleep(10 * time.Millisecond)` para simular trabalho.
- `go test ./internal/concurrent/ -run TestRunDynamicWorkerPool -v -race` passa
  (o flag `-race` detecta race conditions).

---

## Exercicio 9: Substituir polling por WebSocket

**Dificuldade:** Avancado

### Conceito

O watcher atual usa polling HTTP (`eth_getLogs` a cada N segundos). Isso tem latencia
inerente: eventos so sao detectados no proximo ciclo de polling. Uma alternativa e usar
WebSocket com `eth_subscribe("logs", ...)`, que envia eventos em tempo real assim que
o nodo Ethereum os processa.

Neste exercicio voce criara uma implementacao alternativa do poller que usa WebSocket,
mas mantem o **mesmo pipeline downstream** (normalizer -> fan-out -> consumers).
Isso demonstra o poder de channels como abstração: o poller produz `RawLog` em um
canal, e o resto do pipeline nao sabe (nem se importa) se os dados vieram via HTTP
ou WebSocket.

### Instrucoes

1. Adicione a dependencia:

   ```bash
   go get github.com/gorilla/websocket
   ```

2. **Crie `internal/provider/blockchain/ethereum_ws.go`** com um struct `EthereumWSLogsFetcher`
   que implementa uma funcao `Subscribe`:

   ```go
   func (f *EthereumWSLogsFetcher) Subscribe(ctx context.Context, addresses []string) (<-chan RawLog, error)
   ```

   A funcao deve:
   - Conectar ao endpoint WebSocket com `websocket.DefaultDialer.DialContext`.
   - Enviar uma mensagem `eth_subscribe` com filtro de logs para os enderecos.
   - Iniciar uma goroutine que le mensagens do WebSocket e envia `RawLog` no canal.
   - Fechar o canal e a conexao quando o context e cancelado.

3. **`internal/watcher/watcher.go`** — Crie um metodo alternativo `startWSPoller` que
   retorna `<-chan RawLog` (mesmo tipo que `startPoller`). O restante do pipeline
   (`Stage`, `FanOut`, consumers) permanece identico.

4. Adicione uma flag ou variavel de ambiente `WATCHER_MODE=ws|poll` para escolher
   entre os dois modos.

### Dica

- A assinatura `eth_subscribe` para logs e:
  ```json
  {
    "jsonrpc": "2.0",
    "method": "eth_subscribe",
    "params": ["logs", {"topics": ["0xddf252ad..."]}],
    "id": 1
  }
  ```
  O nodo responde com um `subscription_id`, e a partir dai envia notificacoes no
  formato `{"method": "eth_subscription", "params": {"subscription": "0x...", "result": {...}}}`.
- O ponto-chave da arquitetura: tanto `startPoller` quanto `startWSPoller` retornam
  `<-chan RawLog`. O pipeline downstream e identico porque consome do canal sem saber
  a origem dos dados. **Channels como interface** — essa e a abstracao.
- Em testes, use um servidor WebSocket mock com `httptest.NewServer` e `websocket.Upgrader`.
- Endpoints como Infura e Alchemy suportam WebSocket em URLs `wss://`.

### Validacao

- `go build ./...` compila.
- Com um endpoint WebSocket real (ex: `wss://mainnet.infura.io/ws/v3/YOUR_KEY`),
  execute `go run ./cmd/event-watcher` com `WATCHER_MODE=ws` e verifique que eventos
  chegam em tempo real (sem o delay do polling).
- Verifique que o modo `poll` continua funcionando normalmente.
- Escreva um teste unitario com um WebSocket mock que envia 3 eventos e verifica
  que todos chegam no canal retornado por `Subscribe`.

---

## Exercicio 10: Timeout por wallet no snapshot runner

**Dificuldade:** Avancado

### Conceito

Atualmente o `processWallet` em `internal/snapshot/runner.go` nao tem timeout
individual — se uma chamada RPC travar, a goroutine do worker fica bloqueada
indefinidamente (ate o timeout global do context pai, se houver).

Neste exercicio voce adicionara um timeout por wallet usando `context.WithTimeout`,
permitindo que o pipeline continue processando outras wallets mesmo quando uma
individual e lenta. Isso demonstra o padrao de **partial failure** com goroutines:
falhas individuais nao devem parar o processamento em lote.

### Instrucoes

1. **`internal/snapshot/runner.go`** — Adicione um campo `walletTimeout` ao struct
   `Runner`:

   ```go
   type Runner struct {
       // ... campos existentes ...
       walletTimeout time.Duration
   }
   ```

2. No construtor `NewRunner`, defina um default:

   ```go
   if walletTimeout <= 0 {
       walletTimeout = 10 * time.Second
   }
   ```

3. No metodo `Run`, altere a funcao passada ao `RunWorkerPool` para usar um context
   com timeout:

   ```go
   resultCh := concurrent.RunWorkerPool(ctx, r.numWorkers, walletCh, func(ctx context.Context, wallet domain.Wallet) walletResult {
       walletCtx, cancel := context.WithTimeout(ctx, r.walletTimeout)
       defer cancel()
       return r.processWallet(walletCtx, wallet, snapshot.ID)
   })
   ```

4. No `processWallet`, verifique o contexto antes de cada chamada RPC:

   ```go
   if ctx.Err() != nil {
       return walletResult{
           WalletID: wallet.ID,
           Items:    items, // retorna items parciais ja coletados
           Error:    fmt.Errorf("wallet timeout: %w", ctx.Err()),
       }
   }
   ```

### Dica

- O `context.WithTimeout` cria um context derivado que e cancelado automaticamente
  apos a duracao especificada. Quando `walletCtx` expira, todas as chamadas HTTP feitas
  com ele retornam imediatamente com `context.DeadlineExceeded`.
- O `defer cancel()` e obrigatorio. Mesmo que o timeout expire sozinho, voce deve
  chamar `cancel()` para liberar os recursos associados ao timer. O linter `go vet`
  avisa se voce esquecer.
- Observe o padrao de partial failure: se `GetBalance` do ETH foi bem-sucedido mas
  `GetTokenBalance` do USDC excedeu o timeout, o resultado contem o item ETH (parcial)
  mais o erro. O stage 4 (agregacao) decide como lidar com isso — no caso atual,
  items parciais sao incluidos no snapshot.
- Nao confunda o timeout por wallet com o timeout global em `cmd/snapshot-runner/main.go`
  (`context.WithTimeout(context.Background(), 5*time.Minute)`). Sao contextos aninhados:
  wallet timeout (10s) < global timeout (5min).

### Validacao

- Escreva um teste em `internal/snapshot/runner_test.go` que:
  - Usa um mock `BalanceProvider` que bloqueia com `time.Sleep(20 * time.Second)`.
  - Configura `walletTimeout = 100 * time.Millisecond`.
  - Verifica que o `Run` completa em menos de 1 segundo (nao espera os 20s).
  - Verifica que o resultado contem um erro com `"deadline exceeded"`.
  - Verifica que o snapshot tem status `"completed"` (ou `"failed"` se nenhum wallet
    retornou dados).
- `go test ./internal/snapshot/ -run TestRunnerWalletTimeout -v -timeout 10s` passa.

---

## Exercicio 11: Benchmark comparando tamanhos de worker pool

**Dificuldade:** Avancado

### Conceito

Quantos workers sao ideais? A resposta depende do tipo de trabalho (CPU-bound vs I/O-bound),
da latencia das chamadas externas, e do rate limit do provider. Neste exercicio voce
criara benchmarks parametrizados para medir o throughput do snapshot runner com
diferentes tamanhos de pool.

Go tem suporte nativo a benchmarks com `testing.B`. Benchmarks sao funcoes que comecam
com `Benchmark` em vez de `Test`, e o framework executa a funcao `b.N` vezes para
obter uma medida estavel.

### Instrucoes

1. **Crie `internal/snapshot/runner_bench_test.go`** com benchmarks parametrizados:

   ```go
   func BenchmarkSnapshotRunner(b *testing.B) {
       workerCounts := []int{1, 2, 4, 8, 16}

       for _, numWorkers := range workerCounts {
           b.Run(fmt.Sprintf("workers-%d", numWorkers), func(b *testing.B) {
               // Setup: crie mocks que simulam latencia de rede
               // com time.Sleep(10 * time.Millisecond).
               // Crie um runner com numWorkers.

               b.ResetTimer()
               for i := 0; i < b.N; i++ {
                   _, err := runner.Run(ctx)
                   if err != nil {
                       b.Fatal(err)
                   }
               }
           })
       }
   }
   ```

2. Os mocks devem:
   - `WalletRepository.FindByBlockchain`: retornar 20 wallets fixas.
   - `BalanceProvider.GetBalance`: `time.Sleep(10ms)` + retornar um valor fixo.
   - `TokenBalanceProvider.GetTokenBalance`: `time.Sleep(10ms)` + retornar um valor fixo.
   - `PriceProvider.GetPriceUSD`: retornar um valor fixo sem delay.
   - `SnapshotRepository`: operacoes no-op (nao persistir).

3. Execute os benchmarks e analise os resultados.

### Dica

- Execute com: `go test ./internal/snapshot/ -bench=BenchmarkSnapshotRunner -benchtime=5s -v`.
- O output tera o formato:
  ```
  BenchmarkSnapshotRunner/workers-1    N    xxxxx ns/op
  BenchmarkSnapshotRunner/workers-2    N    xxxxx ns/op
  BenchmarkSnapshotRunner/workers-4    N    xxxxx ns/op
  ...
  ```
- Com 20 wallets e 10ms por chamada RPC (ETH + 2 tokens = 3 chamadas por wallet):
  - 1 worker: ~20 * 30ms = 600ms
  - 4 workers: ~5 * 30ms = 150ms
  - 20 workers: ~1 * 30ms = 30ms
  Mas na pratica ha overhead de scheduling e channel contention, entao os numeros
  reais serao diferentes.
- Use `b.ResetTimer()` apos o setup para excluir o tempo de inicializacao.
- Use `benchstat` para comparar resultados entre execucoes:
  ```bash
  go install golang.org/x/perf/cmd/benchstat@latest
  go test -bench=. -count=5 > old.txt
  # ... faca mudancas ...
  go test -bench=. -count=5 > new.txt
  benchstat old.txt new.txt
  ```

### Validacao

- Os benchmarks executam sem erros.
- Os resultados mostram que mais workers reduz o tempo (ate certo ponto).
- Identifique o ponto de retorno decrescente: a partir de quantos workers o ganho
  se torna insignificante?
- Documente suas descobertas em um comentario no topo do arquivo de benchmark.

---

## Exercicio 12: Modo "diff" no snapshot runner (Desafio)

**Dificuldade:** Avancado

### Conceito

Atualmente cada execucao do snapshot gera um registro completo e independente. Mas
para relatorios fiscais, o que interessa sao as **mudancas**: quais wallets tiveram
alteracoes de saldo entre dois snapshots? Neste exercicio voce implementara um modo
"diff" que compara o snapshot recem-gerado com o anterior e reporta as diferencas.

Isso combina multiplos conceitos de concorrencia:
- Pipeline para gerar o novo snapshot (existente).
- Query do snapshot anterior (I/O).
- Comparacao em memoria (CPU).
- Output das diferencas via channel para flexibilidade.

### Instrucoes

1. **`internal/contracts/contracts.go`** — Adicione um metodo ao `SnapshotRepository`:

   ```go
   type SnapshotRepository interface {
       // ... metodos existentes ...
       FindLatestCompleted(ctx context.Context) (*domain.WalletSnapshot, error)
       FindSnapshotItems(ctx context.Context, snapshotID string) ([]domain.WalletSnapshotItem, error)
   }
   ```

2. **`internal/domain/snapshot.go`** — Adicione um tipo para representar diferencas:

   ```go
   type SnapshotDiff struct {
       WalletID    string  `json:"wallet_id"`
       AssetSymbol string  `json:"asset_symbol"`
       OldAmount   float64 `json:"old_amount"`
       NewAmount   float64 `json:"new_amount"`
       Change      float64 `json:"change"`      // new - old
       ChangeUSD   float64 `json:"change_usd"`  // change * usd_price
   }
   ```

3. **`internal/snapshot/runner.go`** — Adicione um metodo `RunWithDiff`:

   ```go
   func (r *Runner) RunWithDiff(ctx context.Context) (*domain.WalletSnapshot, []domain.SnapshotDiff, error) {
       // 1. Busque o snapshot anterior (FindLatestCompleted).
       // 2. Execute o pipeline normal (r.Run).
       // 3. Compare os items do novo snapshot com os do anterior.
       // 4. Retorne o novo snapshot e a lista de diffs.
   }
   ```

4. A comparacao deve:
   - Criar um mapa `chave → WalletSnapshotItem` para o snapshot anterior,
     usando `walletID + ":" + assetSymbol` como chave.
   - Iterar sobre os items do novo snapshot e comparar com o mapa.
   - Reportar items novos (existem no novo mas nao no anterior).
   - Reportar items removidos (existem no anterior mas nao no novo).
   - Reportar items alterados (amounts diferentes).

5. **`cmd/snapshot-runner/main.go`** — Adicione uma flag `--diff` que chama
   `RunWithDiff` e imprime os diffs alem do snapshot.

### Dica

- A comparacao nao precisa ser concorrente — ela opera em dados ja coletados em
  memoria. O ponto do exercicio e integrar a logica de diff com o pipeline concorrente
  existente.
- Para detectar items removidos, apos iterar os novos items, verifique quais chaves
  do mapa anterior nao foram visitadas.
- Use `math.Abs(new - old) < 0.000001` para considerar valores iguais (floating point
  comparison). Valores com diferenca menor que esse epsilon sao considerados iguais.
- O `FindLatestCompleted` deve retornar `nil, nil` se nao existir snapshot anterior
  (primeira execucao). Nesse caso, todos os items sao "novos" e nao ha diffs
  significativos — retorne uma lista vazia de diffs.

### Validacao

- Execute `go run ./cmd/snapshot-runner` duas vezes. Na segunda execucao com `--diff`,
  verifique:
  - Se nenhum saldo mudou, a lista de diffs esta vazia.
  - Adicione uma wallet nova no banco entre as duas execucoes e verifique que os items
    da nova wallet aparecem como "novos" no diff.
- Escreva um teste unitario que cria dois conjuntos de `WalletSnapshotItem` em memoria
  e verifica a logica de comparacao (sem banco de dados).
- `go test ./internal/snapshot/ -run TestSnapshotDiff -v` passa.

---

## Resumo

Apos completar todos os exercicios voce tera:

- **Fan-out broadcast** com 4 consumers independentes (Exercicio 1)
- **Agregacao de resultados** do worker pool com persistencia (Exercicio 2)
- **Select multiplexado** com map como acumulador (Exercicio 3)
- **Rate limiting** integrado ao worker pool (Exercicio 4)
- **Retry com backoff exponencial** usando o decorator pattern (Exercicio 5)
- **Teste de integracao** verificando o pipeline end-to-end (Exercicio 6)
- **Stage de filtragem stateful** no pipeline (Exercicio 7)
- **Worker pool dinamico** com scale up/down automatico (Exercicio 8)
- **WebSocket como alternativa ao polling** com a mesma interface de canal (Exercicio 9)
- **Timeout por item** com partial failure (Exercicio 10)
- **Benchmarks parametrizados** para decisoes baseadas em dados (Exercicio 11)
- **Pipeline composto** com comparacao de snapshots (Exercicio 12)

Execute o test suite completo ao final:

```bash
go test ./... -v -count=1 -race
```

O flag `-race` ativa o race detector do Go — ele detecta acessos concorrentes a
memoria compartilhada que nao estao protegidos por sincronizacao. Se algum teste
falhar com `-race`, e um bug real de concorrencia que precisa ser corrigido.
