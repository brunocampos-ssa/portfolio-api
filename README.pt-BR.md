# Portfolio API -- Modulos 2, 3 e 4: Concorrencia + Testes + Auth & gRPC em Go

> [Read in English](README.md)

Material de aula que cobre tres etapas consecutivas do curso avancado de Go:

- **Modulo 2 -- Concorrencia Avancada (5h):** o projeto ganhou dois novos
  binarios (`event-watcher` e `snapshot-runner`) que resolvem problemas
  naturalmente concorrentes: monitoramento de eventos blockchain em tempo
  real e geracao de snapshots de saldo para simulacao de relatorio fiscal.

- **Modulo 3 -- Testes Profissionais (3h):** em cima do codigo do Modulo 2, foi
  adicionada uma camada completa de testes: unitarios, com mocks (`testify/mock`),
  de concorrencia (com `-race`), de integracao (Postgres + Anvil via
  `testcontainers-go`), benchmarks e profiling com `pprof`. Os executaveis do
  Modulo 2 permanecem intactos -- a evolucao foi **aditiva**.

- **Modulo 4 Aula 1 -- JWT + gRPC (3h):** autenticacao com argon2id +
  access tokens (JWT HS256) + refresh tokens com rotacao e deteccao de
  replay. Espelhamento da API REST em gRPC com o **mesmo nucleo de auth**
  servindo aos dois transportes. Aula 2 cobrira observabilidade e
  resiliencia da camada de auth. Detalhes completos abaixo na secao
  "Modulo 4 -- Aula 1".

> **Pre-requisito**: Modulo 1 (Tratamento de Erros, Panic/Recover, Programacao Defensiva).
> Os conceitos de `errors.Is`, `AppError`, `panic`/`recover` e falha parcial
> continuam presentes e nao serao re-explicados aqui.

---

## Objetivos de Aprendizagem

Ao final deste modulo, voce sera capaz de:

| Conceito | Onde aparece |
|----------|-------------|
| Canais direcionais (`<-chan`, `chan<-`) | `concurrent/`, `watcher/` |
| Quem fecha o canal (ownership) | `WorkerPool`, `FanOut`, `Stage` |
| `select` e multiplexacao | `watcher.go`: poller, metricsWorker |
| Worker Pools | `concurrent/workerpool.go`, `snapshot/runner.go` |
| Pipelines | `concurrent/pipeline.go`, `watcher.go` |
| Fan-out (broadcast) | `concurrent/fanout.go`, `watcher.go` |
| Fan-in (merge) | `concurrent/fanout.go` (`Merge`), `snapshot/runner.go` |
| Estrategias de buffer | Ao longo de toda a base de codigo |
| Shutdown gracioso | `cmd/event-watcher`, `ctx.Done()` |

---

## Cenario de Negocio

O `portfolio-api` e uma API REST que gerencia portfolios cripto com PostgreSQL.
Nos modulos anteriores, voce construiu a API principal (consulta de saldo, tratamento
de erros, etc.). Agora o projeto precisa de duas novas capacidades:

1. **Monitoramento de eventos em tempo real** -- um processo que observa a blockchain
   Ethereum procurando eventos de Transfer ERC-20 envolvendo wallets cadastradas no
   banco de dados. Cada evento detectado precisa ser simultaneamente persistido,
   logado e contabilizado para metricas.

2. **Snapshots de saldo para relatorio fiscal** -- um processo que, sob demanda,
   consulta o saldo nativo (ETH) e de tokens ERC-20 (USDC, USDT) de todas as
   wallets Ethereum, converte para USD e persiste o resultado como um snapshot
   pontual.

Ambos os cenarios envolvem operacoes de I/O (chamadas RPC a nos Ethereum, queries
ao banco de dados, chamadas a APIs de preco) que se beneficiam diretamente de
concorrencia.

---

## Por que Concorrencia?

Este projeto e ideal para ensinar concorrencia porque os problemas de negocio
mapeiam diretamente para padroes concorrentes classicos:

| Problema | Padrao concorrente | Por que? |
|----------|-------------------|----------|
| Evento detectado precisa ser persistido, logado E contabilizado | **Fan-out (broadcast)** | Tres consumidores precisam da mesma informacao |
| Muitas wallets precisam ter saldo consultado independentemente | **Worker Pool** | Cada consulta e independente; concorrencia limitada evita rate limiting |
| Dados fluem por etapas sequenciais de transformacao | **Pipeline** | Poller -> normalizer -> broadcast -> consumers |
| Multiplos workers produzem resultados para um unico coletor | **Fan-in (merge)** | O agregador precisa de todos os resultados em um unico stream |
| Processo precisa responder a multiplos sinais (timer, shutdown) | **`select` e multiplexacao** | O poller reage a tick, heartbeat e cancelamento |
| Processo long-running precisa parar de forma limpa | **Shutdown gracioso** | `signal.NotifyContext` + propagacao via `ctx.Done()` |

Sem concorrencia, o watcher processaria eventos sequencialmente (persistir, depois
logar, depois contar) e o snapshot consultaria wallets uma por uma. Com concorrencia,
tudo acontece em paralelo de forma segura e controlada.

---

## Conceitos Teoricos

### 5.1 Canais Direcionais

Go permite restringir a direcao de um canal na assinatura da funcao:

| Sintaxe | Significado | Quem usa |
|---------|-------------|----------|
| `chan T` | Leitura e escrita | O criador do canal |
| `<-chan T` | Somente leitura (receive-only) | O consumidor |
| `chan<- T` | Somente escrita (send-only) | O produtor |

A direcao e verificada em tempo de compilacao. Se voce tentar fechar um `<-chan`,
o compilador rejeita. Se voce tentar enviar para um `<-chan`, o compilador rejeita.
Isso elimina classes inteiras de bugs em tempo de compilacao.

**Exemplos do projeto:**

```go
// startPoller retorna um canal somente-leitura.
// O chamador pode ler, mas NAO pode fechar ou escrever.
func (w *Watcher) startPoller(ctx context.Context, addresses []string) <-chan RawLog {
    rawLogs := make(chan RawLog, 64) // internamente, o poller tem acesso total
    go func() {
        defer close(rawLogs)         // somente o produtor fecha
        // ... envia logs para rawLogs
    }()
    return rawLogs  // conversao implicita: chan RawLog -> <-chan RawLog
}
```

```go
// RunWorkerPool recebe um canal somente-leitura como input
// e retorna um canal somente-leitura como output.
func RunWorkerPool[I any, O any](
    ctx context.Context,
    numWorkers int,
    input <-chan I,                           // pool so le
    process func(context.Context, I) O,
) <-chan O {                                  // chamador so le
    output := make(chan O, numWorkers)         // internamente, pool escreve
    // ...
    return output
}
```

```go
// Stage recebe somente-leitura, retorna somente-leitura.
func Stage[I any, O any](
    ctx context.Context,
    input <-chan I,
    transform func(context.Context, I) (O, bool),
) <-chan O
```

A direcao e uma forma de documentacao executavel: ao olhar a assinatura, voce ja
sabe quem produz e quem consome.

---

### 5.2 Quem Fecha o Canal? (Channel Ownership)

Esta e a regra mais importante de concorrencia com canais em Go:

> **Somente o PRODUTOR (dono/criador) de um canal deve fecha-lo.**

Fechar um canal do lado do consumidor e um dos erros mais comuns e perigosos:

| Acao errada | Consequencia |
|------------|--------------|
| Consumidor fecha o canal | O produtor faz `send` em canal fechado -> **panic** |
| Dois produtores fecham o canal | Segundo `close()` -> **panic** |
| Ninguem fecha o canal | Consumidores bloqueiam para sempre -> **goroutine leak** |

**A cadeia de ownership no projeto:**

```
startPoller()   cria rawLogs      → fecha rawLogs quando ctx cancela
Stage()         cria output       → fecha output quando input fecha
FanOut()        cria N consumers  → fecha todos quando input fecha
RunWorkerPool() cria output       → fecha output quando todos workers terminam
Generate()      cria ch           → fecha ch quando todos items sao enviados
```

Cada funcao cria um canal, faz `defer close(ch)` dentro da goroutine que escreve
nele, e retorna o canal como `<-chan` para que o consumidor nao possa fecha-lo
acidentalmente.

**Exemplo concreto -- WorkerPool:**

```go
func RunWorkerPool[I any, O any](...) <-chan O {
    output := make(chan O, numWorkers)  // pool CRIA o canal

    var wg sync.WaitGroup
    wg.Add(numWorkers)

    for i := range numWorkers {
        go func(workerID int) {
            defer wg.Done()
            for item := range input {
                // ... processa e envia para output
            }
        }(i)
    }

    // Goroutine "closer" -- espera TODOS os workers terminarem,
    // depois fecha o canal de output.
    // O pool CRIOU o canal, entao o pool FECHA.
    go func() {
        wg.Wait()
        close(output)
    }()

    return output  // retorna como <-chan: consumidor nao pode fechar
}
```

**Exemplo concreto -- FanOut:**

```go
func FanOut[T any](ctx context.Context, input <-chan T, numConsumers int) []<-chan T {
    outputs := make([]chan T, numConsumers)
    readOnly := make([]<-chan T, numConsumers)

    for i := range numConsumers {
        ch := make(chan T, 16)    // FanOut CRIA cada canal
        outputs[i] = ch
        readOnly[i] = ch          // conversao chan T -> <-chan T
    }

    go func() {
        defer func() {
            for _, ch := range outputs {
                close(ch)          // FanOut FECHA todos os canais que criou
            }
        }()
        // ... broadcast de items
    }()

    return readOnly  // consumidores recebem somente-leitura
}
```

---

### 5.3 Select e Multiplexacao

O `select` e como um `switch`, mas para canais. Ele permite que uma goroutine
espere em multiplos canais simultaneamente:

```go
select {
case msg := <-ch1:
    // ch1 produziu um valor
case ch2 <- value:
    // valor foi enviado para ch2
case <-ctx.Done():
    // contexto foi cancelado
default:
    // nenhum canal pronto (nao-bloqueante)
}
```

Regras importantes:
- **Somente UM case executa por iteracao**
- Se multiplos cases estao prontos, Go escolhe um **pseudo-aleatoriamente**
- Sem `default`, o `select` bloqueia ate um case estar pronto
- Com `default`, o `select` nunca bloqueia (util para tentativas nao-bloqueantes)

**Exemplo 1 -- Poller com tres sinais** (`watcher.go`):

```go
for {
    select {
    case <-ticker.C:
        // Hora de fazer polling: chama eth_getLogs
        logs, newBlock, err := w.logsFetcher.FetchLogs(ctx, addresses, lastBlock)
        if err != nil {
            log.Printf("watcher: fetch logs error: %v", err)
            continue  // nao crasha em erros transientes -- tenta de novo no proximo tick
        }
        // ... envia logs para o canal

    case <-heartbeat.C:
        // Liveness check: loga status periodicamente
        log.Printf("watcher: heartbeat -- last block: %d, tracking %d addresses",
            lastBlock, len(addresses))

    case <-ctx.Done():
        // Shutdown gracioso: para o polling
        log.Println("watcher: poller stopping")
        return
    }
}
```

Este `select` multiplexa tres fontes de eventos em um unico loop. Sem `select`,
voce precisaria de tres goroutines separadas com sincronizacao manual.

**Exemplo 2 -- metricsWorker com duas fontes** (`watcher.go`):

```go
for {
    select {
    case event, ok := <-events:
        if !ok {
            // Canal fechado: reporta metricas finais e sai
            log.Printf("watcher: [METRICS] final -- incoming=%d outgoing=%d total=%d",
                incoming, outgoing, incoming+outgoing)
            return
        }
        switch event.Direction {
        case "incoming":
            incoming++
        case "outgoing":
            outgoing++
        }

    case <-ticker.C:
        // Reporta metricas parciais periodicamente
        log.Printf("watcher: [METRICS] incoming=%d outgoing=%d total=%d",
            incoming, outgoing, incoming+outgoing)

    case <-ctx.Done():
        // Shutdown
        return
    }
}
```

Note o uso do idioma `event, ok := <-events`. Quando o canal e fechado, `ok` e
`false` e `event` e o zero-value do tipo. Isso permite distinguir "recebi um
valor" de "o canal fechou".

**Select nao-bloqueante para shutdown rapido** (`workerpool.go`):

```go
for item := range input {
    // Verifica cancelamento ANTES de processar
    select {
    case <-ctx.Done():
        return
    default:
        // contexto ok, continua
    }

    result := process(ctx, item)

    // Envia resultado, mas respeita cancelamento
    select {
    case output <- result:
    case <-ctx.Done():
        return
    }
}
```

O primeiro `select` com `default` verifica cancelamento sem bloquear. O segundo
`select` sem `default` bloqueia ate conseguir enviar OU o contexto ser cancelado
-- sem isso, uma goroutine poderia ficar presa em um canal cheio mesmo apos o
shutdown.

---

### 5.4 Worker Pools

Um Worker Pool e um conjunto fixo de N goroutines que leem de um canal compartilhado
de input. Cada item do canal e processado por exatamente UM worker.

```
                    ┌──────────┐
          ┌────────►│ Worker 1 │────────┐
          │         └──────────┘        │
          │                             │
 input ───┤         ┌──────────┐        ├──► output
 (chan)    ├────────►│ Worker 2 │────────┤    (chan)
          │         └──────────┘        │
          │                             │
          └────────►│ Worker N │────────┘
                    └──────────┘
```

**Por que bounded (limitado)?** Porque operacoes de I/O tem limites:
- Endpoints RPC Ethereum tem rate limiting
- Bancos de dados tem limite de conexoes
- APIs externas tem throttling

Com `numWorkers = 3`, no maximo 3 chamadas RPC acontecem simultaneamente. Sem essa
limitacao, 1000 wallets gerariam 1000 chamadas simultaneas e provavelmente seriam
bloqueadas.

**Implementacao** (`concurrent/workerpool.go`):

```go
func RunWorkerPool[I any, O any](
    ctx context.Context,
    numWorkers int,
    input <-chan I,
    process func(context.Context, I) O,
) <-chan O {
    output := make(chan O, numWorkers)    // buffer = numWorkers

    var wg sync.WaitGroup
    wg.Add(numWorkers)

    for i := range numWorkers {
        go func(workerID int) {
            defer wg.Done()
            for item := range input {    // range sai quando input fecha
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
        close(output)                    // pool fecha o canal que criou
    }()

    return output
}
```

Conceitos-chave:
1. **`range input`** -- o loop sai automaticamente quando `input` e fechado. Por
   isso o chamador DEVE fechar o input, senao os workers bloqueiam para sempre.
2. **`sync.WaitGroup`** -- coordena o termino de todos os workers antes de fechar
   o output.
3. **Generics `[I, O]`** -- a funcao funciona com qualquer tipo de input e output.
4. **Dois `select` statements** -- um para verificar cancelamento antes do
   processamento, outro para enviar o resultado respeitando cancelamento.

**Uso no snapshot runner** (`snapshot/runner.go`):

```go
walletCh := concurrent.Generate(ctx, wallets)

resultCh := concurrent.RunWorkerPool(ctx, r.numWorkers, walletCh,
    func(ctx context.Context, wallet domain.Wallet) walletResult {
        return r.processWallet(ctx, wallet, snapshot.ID)
    },
)

for result := range resultCh {
    // agrega resultados
}
```

---

### 5.5 Pipelines

Um pipeline e uma sequencia de estagios onde a saida de um estagio e a entrada do
proximo. Cada estagio roda em sua propria goroutine.

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  source  │ ──► │ stage 1  │ ──► │ stage 2  │ ──► │ consumer │
│          │     │(transform│     │(transform│     │          │
└──────────┘     └──────────┘     └──────────┘     └──────────┘
```

`Stage[I, O]` e o bloco de construcao reutilizavel:

```go
func Stage[I any, O any](
    ctx context.Context,
    input <-chan I,
    transform func(context.Context, I) (O, bool),  // (resultado, manter?)
) <-chan O {
    output := make(chan O, 1)  // buffer = 1

    go func() {
        defer close(output)    // propagacao de fechamento

        for item := range input {
            select {
            case <-ctx.Done():
                return
            default:
            }

            result, keep := transform(ctx, item)
            if !keep {
                continue       // filtragem: descarta o item
            }

            select {
            case output <- result:
            case <-ctx.Done():
                return
            }
        }
    }()

    return output
}
```

**Filtragem**: o segundo retorno `bool` permite que o estagio descarte itens.
Por exemplo, o normalizador no watcher descarta logs que nao sao Transfer events
ou que nao envolvem wallets rastreadas.

**Propagacao de fechamento**: quando o `input` fecha, o `range` sai, e o `defer
close(output)` fecha a saida. Isso cria uma cascata de fechamento pelo pipeline
inteiro:

```
source fecha → stage 1 fecha → stage 2 fecha → consumer sai do range
```

**Composicao**: stages sao componiveis. Voce pode encadear quantos quiser:

```go
input := concurrent.Generate(ctx, items)
step1 := concurrent.Stage(ctx, input, filterFn)
step2 := concurrent.Stage(ctx, step1, transformFn)
// consumir step2
```

`Generate[T]` converte um slice em um canal, servindo como fonte do pipeline:

```go
func Generate[T any](ctx context.Context, items []T) <-chan T {
    ch := make(chan T, len(items))  // buffer = tamanho do slice
    go func() {
        defer close(ch)
        for _, item := range items {
            select {
            case ch <- item:
            case <-ctx.Done():
                return
            }
        }
    }()
    return ch
}
```

---

### 5.6 Fan-Out (Broadcast)

Fan-out e o padrao onde UM produtor envia cada item para TODOS os N consumidores.
Cada consumidor recebe uma copia de todos os itens.

```
                 ┌─────────────┐
                 │    input     │
                 └──────┬──────┘
                        │
                   ┌────▼────┐
                   │ FanOut  │  le cada item uma vez, envia para TODOS
                   └────┬────┘
                   ┌────┼────────┐
                   ▼    ▼        ▼
                 [ch1] [ch2]  [chN]   ← cada consumidor recebe TODOS os items
```

**Diferenca crucial entre Fan-Out (broadcast) e Worker Pool:**

| Aspecto | Fan-Out (Broadcast) | Worker Pool |
|---------|-------------------|-------------|
| Cada item vai para... | TODOS os consumidores | UM worker |
| Caso de uso | Persistir + logar + contar o mesmo evento | Distribuir trabalho |
| N consumidores x M items = | N x M entregas | M entregas (distribuidas) |

**Uso no watcher**: cada evento normalizado precisa ser simultaneamente:
- Persistido no banco (consumer 0)
- Logado para observabilidade (consumer 1)
- Contabilizado para metricas (consumer 2)

```go
consumers := concurrent.FanOut(ctx, normalized, 3)

go w.persistWorker(ctx, consumers[0])
go w.logWorker(ctx, consumers[1])
go w.metricsWorker(ctx, consumers[2])
```

**Comportamento de bloqueio**: se um consumidor e lento, ele bloqueia o fan-out
para TODOS os consumidores (porque o fan-out espera enviar para cada um). Por
isso os canais de saida sao bufferizados (16 itens) -- o buffer absorve diferencas
temporarias de velocidade entre consumidores.

---

### 5.7 Fan-In (Merge)

Fan-in e o padrao inverso do fan-out: N canais de entrada sao combinados em UM
unico canal de saida.

```
┌─────┐  ┌─────┐  ┌─────┐
│ ch1 │  │ ch2 │  │ chN │
└──┬──┘  └──┬──┘  └──┬──┘
   │        │        │
   └────────┼────────┘
            ▼
   ┌──────────────┐
   │    merged     │  ← canal unico com items de TODOS os inputs
   └──────────────┘
```

```go
func Merge[T any](ctx context.Context, channels ...<-chan T) <-chan T {
    merged := make(chan T, len(channels))

    var wg sync.WaitGroup
    wg.Add(len(channels))

    for _, ch := range channels {
        go func(c <-chan T) {
            defer wg.Done()
            for item := range c {
                select {
                case merged <- item:
                case <-ctx.Done():
                    return
                }
            }
        }(ch)
    }

    go func() {
        wg.Wait()
        close(merged)
    }()

    return merged
}
```

Uma goroutine por canal de entrada garante que um canal lento nao bloqueia os
outros. O `WaitGroup` garante que `merged` so e fechado quando TODOS os inputs
estao esgotados.

O `RunWorkerPool` usa fan-in implicitamente: multiplos workers escrevem no
mesmo canal de output. O `Merge` oferece a mesma funcionalidade de forma
explicita quando voce ja tem canais separados.

---

### 5.8 Estrategias de Buffer

A escolha do tamanho do buffer e uma decisao de design que impacta throughput,
uso de memoria e comportamento sob carga.

| Canal | Buffer | Justificativa |
|-------|--------|---------------|
| `rawLogs` (poller) | **64** | Absorve bursts quando um bloco Ethereum contem muitos Transfer events. Sem buffer, o poller bloquearia ate o normalizador ler cada log. |
| FanOut outputs | **16** | Absorve diferencas de velocidade entre consumidores. Se o persist worker demora em um INSERT, o logger e metrics continuam recebendo. |
| WorkerPool output | **numWorkers** | No maximo `numWorkers` resultados podem ser produzidos simultaneamente. Esse e o teto natural de "em voo". |
| Pipeline Stage output | **1** | Permite que o produtor fique um passo a frente do consumidor. Simples e eficiente para transforms que nao sao I/O-bound. |
| Generate | **len(items)** | Todos os items cabem no buffer, entao `Generate` nao bloqueia. Apropriado para inputs de tamanho conhecido e limitado. |
| Merge output | **len(channels)** | Cada goroutine de input pode enviar um item sem bloquear, mesmo que o consumidor esteja momentaneamente ocupado. |

**Regras gerais:**
- **Unbuffered (`make(chan T)`)**: ponto de sincronizacao; o sender bloqueia ate
  o receiver ler. Use quando voce precisa de handoff garantido.
- **Buffered (`make(chan T, N)`)**: desacopla produtor e consumidor. Use quando
  ha diferencas previsíveis de velocidade ou bursts conhecidos.
- **Buffer muito grande**: desperdiça memoria e pode mascarar problemas de
  backpressure.
- **Buffer muito pequeno**: pode causar contenção desnecessaria.

---

## Walkthrough: Event Watcher

O Event Watcher e um processo long-running que monitora a blockchain Ethereum
por eventos ERC-20 Transfer envolvendo wallets cadastradas.

### Diagrama da Arquitetura

```
                          ┌─────────────────┐
                          │   Ethereum RPC   │
                          │   (eth_getLogs)   │
                          └────────┬────────┘
                                   │
                          ┌────────▼────────┐
                          │     Poller      │  goroutine com select:
                          │                 │  - ticker.C (poll)
                          │  startPoller()  │  - heartbeat.C (liveness)
                          │                 │  - ctx.Done() (shutdown)
                          └────────┬────────┘
                                   │
                          rawLogs chan RawLog (buffer: 64)
                                   │
                          ┌────────▼────────┐
                          │   Normalizer    │  concurrent.Stage()
                          │                 │
                          │ RawLog → *Event │  filtra logs irrelevantes
                          │ (keep=true/     │  converte hex → dominio
                          │  keep=false)    │
                          └────────┬────────┘
                                   │
                       normalized chan *WalletEvent (buffer: 1)
                                   │
                          ┌────────▼────────┐
                          │    FanOut       │  concurrent.FanOut()
                          │   (broadcast)   │  1 input → 3 outputs
                          └────┬───┬───┬────┘
                               │   │   │
                    ┌──────────┘   │   └──────────┐
                    │              │               │
           ch[0] (buf:16)  ch[1] (buf:16)  ch[2] (buf:16)
                    │              │               │
            ┌───────▼──────┐ ┌────▼─────┐ ┌───────▼──────┐
            │ persistWorker│ │logWorker │ │metricsWorker │
            │              │ │          │ │              │
            │ Salva no DB  │ │ Log de   │ │ Conta        │
            │ via EventRepo│ │ cada     │ │ incoming vs  │
            │              │ │ evento   │ │ outgoing     │
            └──────────────┘ └──────────┘ └──────────────┘
```

### Passo a passo

**1. Inicializacao** (`cmd/event-watcher/main.go`)

O `main.go` carrega a configuracao, conecta ao banco, cria as dependencias e
instancia o `Watcher`. O shutdown gracioso e configurado com
`signal.NotifyContext`, que cria um contexto cancelado por SIGINT ou SIGTERM:

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

if err := w.Run(ctx); err != nil && err != context.Canceled {
    log.Fatalf("FATAL: watcher error: %v", err)
}
```

**2. Carregar wallets rastreadas** (`watcher.go` -- `Run()`)

O watcher carrega todas as wallets Ethereum do banco e monta um mapa de lookup
`address -> walletID`. Este mapa e usado pelo normalizador para determinar
se um evento Transfer e relevante.

**3. Poller** (`watcher.go` -- `startPoller()`)

O poller roda em uma goroutine propria. Ele cria o canal `rawLogs` (buffer 64),
e usa `select` para multiplexar tres sinais:

- `ticker.C`: hora de chamar `eth_getLogs`
- `heartbeat.C`: hora de logar status (liveness check para ops)
- `ctx.Done()`: hora de parar

Quando o contexto e cancelado, o poller sai do loop e executa
`defer close(rawLogs)`, propagando o sinal de fechamento para o proximo estagio.

**4. Normalizer** (`watcher.go` -- `concurrent.Stage()`)

O normalizador e um `Stage` que transforma `RawLog` em `*domain.WalletEvent`.
A funcao `NormalizeTransferLog` (`normalizer.go`):

- Ignora logs `Removed` (reorganizados pela chain)
- Verifica se e um evento Transfer (topic[0] == keccak256 de Transfer)
- Extrai enderecos `from` e `to` dos topics
- Determina direcao (`incoming` ou `outgoing`) baseado no mapa de wallets rastreadas
- Converte o hex amount para float64 ajustado por decimals
- Retorna `(event, true)` se relevante ou `(nil, false)` para filtrar

**5. Fan-Out** (`watcher.go` -- `concurrent.FanOut()`)

O stream de eventos normalizados e broadcast para 3 canais de consumidor.
Cada consumidor recebe TODOS os eventos.

**6. Consumidores** (`watcher.go`)

- **persistWorker**: salva cada evento no banco via `eventRepo.Create()`.
  E um consumidor -- ele NAO fecha o canal.
- **logWorker**: imprime cada evento para observabilidade.
- **metricsWorker**: usa `select` avancado para ler de dois canais simultaneamente
  (eventos + ticker de metricas). Detecta fechamento do canal com o idioma
  `event, ok := <-events`.

---

## Walkthrough: Snapshot Runner

O Snapshot Runner e um processo de execucao unica que gera um snapshot pontual
de todos os saldos de wallets Ethereum.

### Diagrama da Arquitetura

```
 ┌──────────────────┐
 │  1. Load Wallets │   walletRepo.FindByBlockchain("ethereum")
 │     (DB query)   │   Retorna []domain.Wallet
 └────────┬─────────┘
          │
          ▼
 ┌──────────────────┐
 │ 2. Generate      │   concurrent.Generate(ctx, wallets)
 │    (slice → chan) │   Cria canal bufferizado com len(wallets)
 └────────┬─────────┘
          │
   walletCh <-chan domain.Wallet
          │
 ┌────────▼─────────┐
 │ 3. Worker Pool   │   concurrent.RunWorkerPool(ctx, numWorkers, walletCh, processWallet)
 │                  │
 │  ┌─────┐ ┌─────┐│
 │  │ W1  │ │ W2  ││   Cada worker:
 │  │     │ │     ││   - Busca saldo ETH via RPC
 │  │     │ │     ││   - Busca saldo USDC e USDT (ERC-20)
 │  └──┬──┘ └──┬──┘│   - Busca preco USD
 │     │       │   │   - Monta walletResult
 │     └───┬───┘   │
 └─────────┤───────┘
           │
    resultCh <-chan walletResult
           │
 ┌─────────▼────────┐
 │ 4. Aggregate     │   for result := range resultCh
 │    & Persist     │   - Acumula items
 │                  │   - Persiste no banco
 │                  │   - Atualiza status (completed/failed)
 └──────────────────┘
```

### Passo a passo

**1. Load Wallets** (`snapshot/runner.go` -- `Run()`)

Carrega todas as wallets Ethereum do banco. Se nao houver wallets, retorna erro
(nao ha o que fazer).

**2. Criar Snapshot pendente**

Cria um registro `wallet_snapshots` com status `"pending"` no banco antes de
comecar o processamento. Isso permite rastrear snapshots em andamento.

**3. Generate**

Converte o slice de wallets em um canal usando `concurrent.Generate`. O canal
e bufferizado com `len(wallets)`, entao todos os items sao enviados imediatamente
sem blocking.

**4. Worker Pool**

`RunWorkerPool` processa wallets com concorrencia limitada. Cada worker executa
`processWallet`, que:

- Busca o saldo nativo ETH via `ethProvider.GetBalance()`
- Busca o preco ETH via `priceProvider.GetPriceUSD()`
- Se `tokenProvider` esta disponivel, busca saldos de USDC e USDT
- Aplica **falha parcial**: se um token falha, os outros continuam
- Retorna `walletResult` com todos os items ou um erro

**5. Aggregate**

O loop `for result := range resultCh` coleta todos os resultados. Quando o canal
fecha (todos os workers terminaram), o loop sai. O runner entao:

- Persiste cada `WalletSnapshotItem` no banco
- Atualiza o status para `"completed"` ou `"failed"` (se todos falharam)
- Imprime o resultado como JSON

---

## Decisoes de Design e Trade-offs

### Por que polling em vez de WebSocket?

O watcher usa polling (`eth_getLogs` via HTTP) em vez de WebSocket subscriptions.

| Aspecto | Polling | WebSocket |
|---------|---------|-----------|
| Complexidade | Simples, sem estado | Gerenciamento de conexao, reconexao |
| Compatibilidade | Funciona com qualquer RPC | Nem todos suportam WebSocket |
| Padroes ensinados | `select`, ticker, heartbeat | Callback-based, menos visivel |
| Resiliencia | Naturalmente idempotente | Precisa de logica de reconexao |

Para fins didaticos, polling e superior porque o loop com `select` demonstra
multiplexacao de canais de forma clara e controlada.

### Por que helpers genericos?

`RunWorkerPool[I, O]`, `Stage[I, O]`, `FanOut[T]`, `Merge[T]` e `Generate[T]`
usam generics (Go 1.18+) por tres razoes:

1. **Reutilizabilidade**: funcionam com qualquer tipo sem type assertions
2. **Type safety**: erros de tipo sao detectados em tempo de compilacao
3. **Valor educacional**: mostram generics aplicados a problemas reais

### Por que binarios separados?

O projeto tem tres binarios (`cmd/api`, `cmd/event-watcher`, `cmd/snapshot-runner`)
em vez de um unico binario com flags:

1. **Responsabilidade unica**: cada binario faz uma coisa
2. **Ciclo de vida independente**: o watcher roda continuamente, o snapshot roda
   sob demanda, a API roda como servidor
3. **Deploy independente**: em producao, cada binario pode ter scaling diferente

### Por que canais bufferizados?

Cada buffer foi escolhido com uma razao especifica (veja secao 5.8). A regra geral:
o buffer deve absorver variacoes previsíveis de velocidade sem desperdicar memoria.

---

## Erros Comuns de Concorrencia

| Erro | Consequencia | Como este projeto evita |
|------|-------------|------------------------|
| **Goroutine leak** | Goroutine bloqueada para sempre, consumindo memoria | Context cancellation + channel close. Todo `select` inclui `case <-ctx.Done()`. Todo canal e fechado pelo produtor. |
| **Fechar canal do lado do consumidor** | `panic: send on closed channel` no produtor | Ownership rule: so o produtor fecha. Canais retornados como `<-chan` impedem `close()` acidental. |
| **Send em canal fechado** | `panic: send on closed channel` | So o owner fecha, e so apos o ultimo send (via `defer` apos o loop de envio). |
| **Race condition** | Dados corrompidos, comportamento indefinido | Canais para comunicacao, sem estado mutavel compartilhado. Nenhuma variavel e escrita por multiplas goroutines. |
| **Deadlock** | Programa trava completamente | Canais bufferizados + context timeout + todo `select` tem `ctx.Done()` como escape. |
| **Esquecer de fechar canal** | Consumidor bloqueia para sempre no `range` | Padrao `defer close(ch)` em toda goroutine produtora. |
| **Fechar canal duas vezes** | `panic: close of closed channel` | Um unico owner por canal, com um unico ponto de fechamento. |

---

## Como Executar Localmente

### Pre-requisitos

- Go 1.25+
- Docker e Docker Compose
- Um endpoint Ethereum RPC (o default `https://eth.drpc.org/` funciona para testes)

### Passo a passo

```bash
# 1. Subir o PostgreSQL (migrations rodam automaticamente)
make db-up

# 2. Rodar a API (terminal 1)
make run

# 3. Rodar o event watcher (terminal 2)
make watcher

# 4. Rodar o snapshot runner (terminal 3)
make snapshot

# 5. Rodar os testes
make test
```

### Build dos binarios

```bash
make build
# Produz:
#   bin/portfolio-api
#   bin/event-watcher
#   bin/snapshot-runner
```

### Variaveis de ambiente

| Variavel | Default | Descricao |
|----------|---------|-----------|
| `PORT` | `8080` | Porta do servidor HTTP |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/portfolio?sslmode=disable` | String de conexao PostgreSQL |
| `ETH_RPC_URL` | `https://eth.drpc.org/` | Endpoint JSON-RPC Ethereum |
| `KLEVER_API_BASE_URL` | `https://api.mainnet.klever.org` | API Klever |
| `COINGECKO_DEMO_API_KEY` | (vazio) | Chave CoinGecko; se vazia, usa mock com precos fixos |
| `WATCHER_POLL_INTERVAL` | `15s` | Intervalo de polling do event watcher |
| `WATCHER_START_BLOCK` | `0` | Bloco inicial (0 = mais recente) |
| `SNAPSHOT_WORKERS` | `3` | Numero de workers concorrentes no snapshot |

---

## Esquema do Banco de Dados

### `users`

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | TEXT PK | Identificador unico |
| `name` | TEXT NOT NULL | Nome do usuario |
| `email` | TEXT NOT NULL | Email |
| `created_at` | TIMESTAMPTZ | Data de criacao |

### `wallets`

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | TEXT PK | Identificador unico |
| `user_id` | TEXT FK → users | Dono da wallet |
| `blockchain` | TEXT NOT NULL | Rede (ethereum, klever) |
| `address` | TEXT NOT NULL | Endereco na blockchain |
| `created_at` | TIMESTAMPTZ | Data de criacao |

### `wallet_events` (NOVO)

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | TEXT PK | Identificador unico |
| `wallet_id` | TEXT FK → wallets | Wallet associada |
| `tx_hash` | TEXT NOT NULL | Hash da transacao |
| `block_number` | BIGINT NOT NULL | Numero do bloco |
| `contract_address` | TEXT NOT NULL | Contrato ERC-20 emissor |
| `event_type` | TEXT NOT NULL | Tipo do evento (default: `transfer`) |
| `direction` | TEXT NOT NULL | `incoming` ou `outgoing` |
| `amount` | TEXT NOT NULL | Quantidade (string para precisao) |
| `token_symbol` | TEXT NOT NULL | Simbolo do token (USDC, USDT, etc.) |
| `raw_payload` | TEXT | JSON bruto do log Ethereum |
| `created_at` | TIMESTAMPTZ | Data de deteccao |

### `wallet_snapshots` (NOVO)

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | TEXT PK | Identificador unico |
| `reference_time` | TIMESTAMPTZ NOT NULL | Momento do snapshot |
| `status` | TEXT NOT NULL | `pending`, `completed` ou `failed` |
| `created_at` | TIMESTAMPTZ | Data de criacao |

### `wallet_snapshot_items` (NOVO)

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id` | TEXT PK | Identificador unico |
| `snapshot_id` | TEXT FK → wallet_snapshots | Snapshot pai |
| `wallet_id` | TEXT FK → wallets | Wallet associada |
| `asset_symbol` | TEXT NOT NULL | Simbolo (ETH, USDC, USDT) |
| `asset_type` | TEXT NOT NULL | `native` ou `erc20` |
| `contract_address` | TEXT | Endereco do contrato ERC-20 |
| `amount` | DOUBLE PRECISION | Saldo do ativo |
| `usd_price` | DOUBLE PRECISION | Preco USD no momento |
| `usd_value` | DOUBLE PRECISION | Valor USD (amount * price) |
| `created_at` | TIMESTAMPTZ | Data de criacao |

---

## Testes (panorama rapido)

Esta secao e um resumo para quem ja conhece o projeto. Uma explicacao
didatica completa da camada de testes -- incluindo o Dockerfile customizado
do Anvil, os helpers `testcontainers-go` e o roteiro de profiling -- esta
logo abaixo, em **Modulo 3 -- Testes Profissionais**.

### Como rodar

```bash
# Testes unitarios + integrados (Postgres + Anvil via testcontainers)
make test

# Apenas unitarios (sem Docker)
make test-unit

# Apenas integrados (requer Docker em execucao)
make test-integration

# Com o detector de corrida
make test-race

# Benchmarks
make bench

# Profile de CPU do hot path do watcher
make profile-cpu
```

### O que esta coberto

| Pacote | Tipo de teste | Onde |
|--------|---------------|------|
| `internal/concurrent` | Unidade + race | `workerpool_test.go`, `fanout_test.go`, `pipeline_test.go`, `race_test.go`, `bench_test.go` |
| `internal/watcher` | Unidade, normalizacao + `Run` com mocks | `normalizer_test.go`, `normalizer_extra_test.go`, `watcher_test.go` |
| `internal/snapshot` | Unidade + mocks (testify/mock) | `runner_test.go`, `runner_extra_test.go` |
| `internal/provider/blockchain` | Whitebox + registry | `ethereum_internal_test.go`, `registry_test.go` |
| `internal/repository/postgres` | Integracao (Postgres via testcontainers) | `repo_test.go`, `user_repository_test.go` |
| `internal/middleware` | Unidade | `recovery_test.go`, `requestid_test.go` |
| `internal/httpapi` | Unidade | `handler_test.go` |
| `internal/domain` | Unidade (`errors.Is`/`errors.As`) | `errors_test.go`, `wallet_test.go` |
| `internal/testutil/ethutil` | Unidade (helpers de encoding) | `anvil_test.go` |
| `test/integration` | Integracao ponta a ponta (Postgres + Anvil) | `snapshot_integration_test.go`, `logs_fetcher_integration_test.go` |

---

## Exercicios

Os exercicios praticos para este modulo estao no arquivo [`EXERCISES.md`](EXERCISES.md).

---

## Modulo 3 -- Testes Profissionais

Esta secao documenta a **camada de testes adicionada em cima do codigo do
Modulo 2**. O objetivo pedagogico nao e reescrever o projeto: e ensinar como
cobrir codigo concorrente, dependente de infraestrutura e sensivel a
performance de forma profissional.

### 3.1 Objetivos de Aprendizagem

Ao final deste modulo voce sabera:

- Escrever testes unitarios idiomaticos com `testing` + `testify/require`
  + `testify/assert` + subtests + tabelas.
- Decidir quando usar um mock (boundaries externos) e quando um stub manual
  e mais legivel.
- Testar codigo concorrente com segurança: `sync.WaitGroup`, canais de
  sincronizacao, `atomic.Int64`, `context.Cancel`.
- Validar ausencia de races com `go test -race`.
- Criar uma infra de integracao determinística com `testcontainers-go`
  subindo **Postgres** + **Anvil** (nosso Dockerfile customizado).
- Mutar estado de uma chain Ethereum local (`anvil_setBalance`,
  `anvil_setStorageAt`, `anvil_impersonateAccount`) para preparar cenarios
  reproduziveis.
- Medir e otimizar hot paths com `go test -bench`, `pprof`, CPU e memoria
  profiles.

### 3.2 Estrategia de Testes

O projeto segue uma **piramide de testes** classica:

```
        ┌──────────────────────────────┐
        │   Integracao (Postgres + Anvil)  │   test/integration (tag `integration`)
        └──────────────────────────────┘
       ┌──────────────────────────────────┐
       │  Concorrencia / race tests         │   internal/concurrent/race_test.go
       └──────────────────────────────────┘
     ┌────────────────────────────────────────┐
     │  Mock-based (testify/mock)               │   internal/{watcher,snapshot}
     └────────────────────────────────────────┘
   ┌──────────────────────────────────────────────┐
   │  Unidade (pure functions, helpers, dominio)    │   tudo em internal/**_test.go
   └──────────────────────────────────────────────┘
```

### 3.3 Dockerfile customizado do Anvil

Em `test/infrastructure/anvil/Dockerfile` temos uma imagem fina sobre
`ghcr.io/foundry-rs/foundry:stable`:

- `anvil` escuta em `127.0.0.1:9000` (port interno).
- `socat` expoe `8545` e redireciona para o port interno.
- Flags extras do `anvil` podem ser passadas via `ANVIL_ARGS`.

O motivo de usar socat e manter um port publico estavel (`8545`) mesmo que
a configuracao interna mude. Alem disso, a imagem customizada nos protege
contra mudancas involuntarias na tag `foundry:stable` -- se uma nova versao
quebrar o JSON-RPC, nosso teste fica isolado.

A infraestrutura esta em:

```
test/infrastructure/anvil/
├── Dockerfile        # imagem custom
└── README.md         # documentacao da imagem
```

O helper Go que constroi e sobe esta imagem esta em
`internal/testutil/anvil/container.go`. Ele usa `testcontainers-go` com
`FromDockerfile` (a imagem e construida uma unica vez, depois cache-hit).

### 3.4 Mutacao de Estado da Chain

O arquivo `internal/testutil/ethutil/anvil.go` embrulha as chamadas RPC
non-standard do Anvil. O objetivo e deixar os testes lendo como Go normal:

| Helper | Metodo RPC | Uso tipico |
|--------|------------|------------|
| `Impersonate(ctx, rpc, addr)` | `anvil_impersonateAccount` | Atuar como um whale sem ter a chave privada |
| `SetEthBalance(ctx, rpc, addr, hexWei)` | `anvil_setBalance` | Preparar saldo reproduzivel |
| `SetEOACode(ctx, rpc, addr, hex)` | `anvil_setCode` | Transformar uma EOA em contrato |
| `SetStorageAt(ctx, rpc, addr, slot, val)` | `anvil_setStorageAt` | Controlar estado de contrato |
| `GetStorageAt(ctx, rpc, addr, slot)` | `eth_getStorageAt` | Ler storage direto |
| `Mine(ctx, rpc, n)` | `anvil_mine` | Avancar blocos |

Os helpers de encoding (`HexWei`, `EthToWei`, `Uint256Hex`, `AddressHex`)
fazem o trabalho chato de zero-padding para 32 bytes / conversao de `float`
para wei sem precisar importar `go-ethereum`.

Exemplo minimo em um teste:

```go
rpc := env.ETHClient() // ethutil.Client
wei := ethutil.EthToWei(2.5)
require.NoError(t, ethutil.SetEthBalance(ctx, rpc, walletAddr, ethutil.HexWei(wei)))
```

### 3.5 Testes Unitarios

Exemplos representativos:

- `internal/concurrent/workerpool_test.go`: verifica que
  `RunWorkerPool[I, O]` respeita `numWorkers` via `atomic.Int32` e reage a
  `ctx.Cancel()`.
- `internal/watcher/normalizer_test.go` + `normalizer_extra_test.go`:
  cobrem paths felizes e de borda (reorg, tokens desconhecidos, block
  numbers invalidos, topics em UPPERCASE, etc.).
- `internal/provider/blockchain/ethereum_internal_test.go`: whitebox para
  `weiToETH` e `padAddress`, alem de `httptest.Server` simulando o no
  Ethereum para o fluxo JSON-RPC.
- `internal/config/config_test.go`: valida parsing de env vars com
  `t.Setenv` e confirma que valores invalidos caem no default.

Todos usam `testify/require` (abort on fail) e `testify/assert` (continue on
fail) com criterios:

- `require` para pre-condicoes (sem isso o teste nao faz sentido).
- `assert` para checagens independentes dentro de um mesmo cenario.

### 3.6 Testes com Mocks

Os mocks `testify/mock` estao em `internal/testutil/mocks/mocks.go`,
cobrindo todas as interfaces de `internal/contracts`. Sao uteis quando:

- O teste precisa assertar **chamada** (nome, argumentos, ordem, contagem).
- O teste precisa simular erros especificos de um boundary externo.

Exemplo em `internal/snapshot/runner_extra_test.go`:

```go
walletRepo := &mocks.WalletRepository{}
walletRepo.On("FindByBlockchain", mock.Anything, "ethereum").
    Return([]domain.Wallet{...}, nil)

ethProvider := &mocks.BalanceProvider{}
ethProvider.On("GetBalance", mock.Anything, mock.Anything).
    Return(0.0, "ETH", errors.New("upstream 500"))

runner := snapshot.NewRunner(walletRepo, snapshotRepo, ethProvider, nil, priceProvider, 1)
result, err := runner.Run(ctx)
// ...
walletRepo.AssertExpectations(t)
```

> Diretriz: **nao mocke demais.** Mocks sao valiosos nos limites externos
> (providers, repositorios). Em dominio puro, stubs manuais ou table tests
> sao mais legiveis. O projeto alterna entre os dois deliberadamente --
> `snapshot/runner_test.go` usa stubs; `snapshot/runner_extra_test.go` usa
> mocks.

### 3.7 Testes de Concorrencia

Concentrados em `internal/concurrent/race_test.go`. Exemplos:

- `TestWorkerPool_NoGoroutineLeakOnCancellation` -- cria um produtor
  infinito, cancela o contexto, e prova que **todos** os workers retornam
  (sem isso, o detector de corrida e o canal de output nao fecham).
- `TestFanOut_BroadcastUnderConcurrentConsumers` -- N consumidores
  somando paralelamente; qualquer race corromperia o total.
- `TestStage_ContextCancellation_StopsPropagation` -- garante que a
  propagacao de cancel chega ate o downstream.

Para rodar com o detector:

```bash
make test-race
```

O detector nao pega **todos** os bugs de concorrencia; ele pega os
detectaveis dinamicamente. Por isso combinamos com testes de propriedade
(contagem, soma, ausencia de leak) que falham mesmo quando o detector nao
observa.

### 3.8 Testes de Integracao

Os testes de integracao vivem em `test/integration/` (tag `integration`).
O ciclo de vida inteiro -- Postgres, Anvil forkado, migrations, bootstrap
da chain -- mora em `internal/testutil/testenv` e e disparado por
**uma unica chamada** dentro do `TestMain`:

```go
var env *testenv.Env

func TestMain(m *testing.M) {
    ctx := context.Background()
    var err error
    env, err = testenv.Setup(ctx)
    if err != nil { log.Fatalf("testenv.Setup: %v", err) }

    code := m.Run()

    _ = env.Close(context.Background())
    os.Exit(code)
}
```

O padrao segue a convencao da `kos-chains/testutil`: arquivos de teste
nao mexem em container, RPC ou SQL de setup -- eles so exercitam o
dominio. Toda plumbing fica no testenv.

**O que `testenv.Setup` faz, na ordem:**

1. Sobe Postgres 15-alpine + Anvil forkado de mainnet **em paralelo**
   (`internal/testutil/postgres` + `internal/testutil/anvil`). Cerca de
   5s de boot economizados.
2. Aplica as migrations (inclui a migration `002_seed_data.up.sql`, que
   planta usuarios + wallets).
3. Carrega `internal/testutil/testenv/fixtures.json` (embarcado via
   `//go:embed`). Ele define: saldo nativo padrao (10 ETH), e os tokens
   ERC-20 que cada wallet recebera por bootstrap (USDC, USDT), com seus
   respectivos whales e amounts.
4. **Bootstrapa a chain a partir do banco** -- esse e o salto didatico:
   - `SELECT DISTINCT address FROM wallets WHERE blockchain = 'ethereum'`
     le as wallets de fato tracked pelo projeto;
   - para cada endereco: `anvil_setBalance` com o saldo nativo do
     fixture, depois `anvil_impersonateAccount` + `eth_sendTransaction`
     para transferir cada token do fixture a partir do whale.
   - o banco e a **fonte da verdade**; a chain e semeada para bater.

**Cenarios cobertos:**

- `TestSnapshotRunner_AgainstSeededChain` -- cenario estrela: com o
  banco seedado e a chain ja bootstrapada, roda o
  `snapshot.NewRunner(...).Run(ctx)` de producao e assere que o
  resultado reflete o bootstrap:
  - saldo nativo **exatamente** igual ao fixture (porque
    `anvil_setBalance` sobrescreve);
  - saldo de cada token **>=** ao fixture (porque `transfer` soma
    sobre o que a wallet ja tinha em mainnet; se o endereco seedado for
    uma celebridade como Vitalik, o saldo real ainda esta la).
- `TestWatcherForkedMainnetUSDCTransfer` -- pega o primeiro endereco
  tracked via `env.GetTrackedETHAddresses(ctx)`, dispara um novo
  `USDC.transfer` do whale para ele via `env.TransferERC20(...)`, e
  verifica que o watcher de producao persiste o evento em
  `wallet_events` com `direction=incoming` e `token_symbol=USDC`.
- `TestLogsFetcher_AgainstAnvil_NegotiatesJSONRPC` -- prova que o
  `EthereumLogsFetcher` fala JSON-RPC com o Anvil real. Caso alguem
  quebre o framing, este teste cai primeiro.
- `TestAnvilHelpers_RoundTrip` -- validacao dos helpers de mutacao de
  chain por si so.

**API principal do `testenv.Env`:**

```go
type Env struct {
    DB          *sql.DB
    Anvil       *anvil.Handle
    Fixtures    Fixtures  // saldo/tokens planejados pelo bootstrap
}

func Setup(ctx context.Context, opts ...Options) (*Env, error)
func (*Env) Close(ctx context.Context) error
func (*Env) RPCURL() string
func (*Env) ETHClient() *ethutil.Client
func (*Env) SeedUserAndWallet(ctx, walletID, userID, name, addr string) error
func (*Env) GetTrackedETHAddresses(ctx context.Context) ([]string, error)
func (*Env) TransferERC20(ctx, token, whale, to string, amount *big.Int) (string, error)
func (*Env) BootstrapChainFromDB(ctx context.Context) error
```

Rodar:

```bash
make test-integration
# ou, apontando para um RPC dedicado (recomendado para CI):
TEST_ETH_FORK_URL=https://mainnet.infura.io/v3/<key> make test-integration
```

> Por padrao `TEST_ETH_FORK_URL` nao esta setado e o teste forka atraves
> de `https://eth.drpc.org/`. Esse RPC publico e gratis mas e
> rate-limited e as vezes remove state antigo -- por isso o fork usa o
> bloco "latest" por padrao em vez de um bloco fixo. Para um pipeline
> deterministico, aponte a env var para um provedor com arquivo completo
> (Infura/Alchemy/QuickNode).
>
> A primeira execucao constroi a imagem do Anvil (`docker build`),
> ~30s. Execucoes subsequentes sao cacheadas. O Dockerfile mora em
> `test/infrastructure/anvil/Dockerfile` e e exposto via `//go:embed`
> por `test/infrastructure/anvil/embed.go` -- o contexto Docker vai como
> um tar em memoria para o testcontainers; nao ha dependencia de path
> relativo.

### 3.9 Benchmarks

Os benchmarks estao proximos ao codigo que medem:

- `internal/watcher/normalizer_extra_test.go::BenchmarkNormalizeTransferLog_{Hit,Miss}`
- `internal/snapshot/runner_extra_test.go::BenchmarkRunner_Run_50Wallets`
- `internal/provider/blockchain/ethereum_internal_test.go::BenchmarkWeiToETH` / `BenchmarkPadAddress`
- `internal/concurrent/bench_test.go::BenchmarkWorkerPool_CPU` / `BenchmarkWorkerPool_IO`

Rodar:

```bash
make bench                      # todos
go test ./internal/watcher \
    -bench=BenchmarkNormalizeTransferLog \
    -benchmem -run=^$ -benchtime=2s
```

Dicas de leitura:

- `ns/op` -- tempo por operacao. Quanto menor, melhor.
- `B/op` + `allocs/op` -- bytes alocados por operacao. Reduzir aqui
  diretamente melhora GC pressure.
- Compare **apenas benchmarks rodados na mesma maquina**. Use
  [`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) para
  analises estatisticas entre runs.

### 3.10 Profiling com pprof

Fluxo tipico para descobrir um gargalo:

```bash
# 1. Gerar profile durante um benchmark.
make profile-cpu

# 2. Interativo no navegador.
go tool pprof -http=:6060 cpu.out

# 3. Top 10 funcoes mais quentes (CLI).
go tool pprof -top -nodecount=10 cpu.out
```

Para memoria:

```bash
make profile-mem
go tool pprof -http=:6060 mem.out
```

O que procurar:

- **CPU**: funcoes com `flat%` alto -- tempo consumido dentro da propria
  funcao (nao em chamadas).
- **Memoria**: muitas alocacoes pequenas (`inuse_objects` vs
  `inuse_space`) frequentemente apontam para strings / slices que poderiam
  ser reutilizados.

### 3.11 O que mudou no codigo de producao?

A regra foi: **mudancas minimas**. Nenhum executavel foi removido, nenhum
fluxo de negocio foi reescrito. As unicas mudancas "producao" sao:

- `go.mod` ganhou `testify/mock` (transitivamente `stretchr/objx`) para os
  mocks de teste.
- Nada mais. Os testes exploram as interfaces ja existentes em
  `internal/contracts`.

Todo o resto e aditivo:

```
internal/testutil/
├── anvil/container.go              # testcontainers wrapper do Anvil
├── containers/env.go               # Postgres + Anvil juntos
├── ethutil/rpc.go                  # cliente JSON-RPC minimo
├── ethutil/anvil.go                # helpers de mutacao da chain
├── ethutil/anvil_test.go           # cobertura dos helpers puros
├── mocks/mocks.go                  # mocks testify/mock
└── postgres/...                    # (ja existia)

test/
├── infrastructure/anvil/Dockerfile # imagem Anvil customizada
├── infrastructure/anvil/README.md
└── integration/                    # testes ponta a ponta (tag `integration`)
```

Modulo 4 Aula 1 adicionou:

```
internal/
├── auth/                                 # NOVO (Modulo 4)
│   ├── argon2.go                         # hasher argon2id em formato PHC
│   ├── argon2_test.go
│   ├── jwt.go                            # TokenIssuer/TokenVerifier HS256
│   └── jwt_test.go
├── grpcapi/                              # NOVO (Modulo 4)
│   ├── auth_interceptor.go               # interceptor unario JWT
│   ├── errors.go                         # AppError -> grpc/codes
│   ├── peer.go                           # user-agent + IP do peer
│   └── server.go                         # AuthServer/PortfolioServer/WalletServer
├── middleware/jwt.go                     # NOVO -- middleware HTTP de JWT
├── repository/postgres/refresh_token_repository.go  # NOVO
└── service/auth_service.go               # NOVO -- Register/Login/Refresh/Logout

proto/portfolio/v1/                       # NOVO -- *.proto
gen/portfolio/v1/                         # NOVO -- codigo gerado pelo buf
examples/restclient/main.go               # NOVO -- demo REST end-to-end
examples/grpcclient/main.go               # NOVO -- demo gRPC end-to-end
buf.yaml, buf.gen.yaml                    # NOVO -- toolchain de protobuf
migrations/005_add_user_auth.up.sql       # NOVO -- password_hash + refresh_tokens
test/integration/auth_helper_test.go      # NOVO
test/integration/auth_http_test.go        # NOVO -- happy + replay + cross-account
test/integration/auth_grpc_test.go        # NOVO -- inclui teste cross-transport
```

Novos `_test.go` foram plantados junto aos arquivos que testam, sem mexer
no codigo de producao.

### 3.12 Fluxo Sugerido para a Aula

1. **Unidade (~20 min)** -- abrir `normalizer_extra_test.go`, explicar
   table tests, mostrar `testify/require` vs `assert`.
2. **Mocks (~15 min)** -- comparar `runner_test.go` (stubs) com
   `runner_extra_test.go` (mocks). Discutir **quando** mockar.
3. **Concorrencia (~20 min)** -- rodar `make test-race`. Remover o
   `select { case <-ctx.Done(): return }` do worker pool e mostrar o
   goroutine leak detectado.
4. **Integracao (~25 min)** -- `make test-integration`. Abrir o
   `Dockerfile.anvil`, o `containers.Env`, explicar `anvil_setBalance`.
5. **Benchmarks + pprof (~20 min)** -- `make profile-cpu`, abrir
   `go tool pprof -http=:6060 cpu.out`, mostrar flamegraph.
6. **Exercicios (resto)** -- alunos trabalham em `EXERCISES.md` (ver
   secao "Modulo 3 -- Testes Profissionais" la).

### 3.13 Cuidados e Limitacoes

- Os testes de integracao **exigem Docker**. Em CI sem Docker, use apenas
  `make test-unit` + `make test-race`.
- O Anvil da imagem customizada e **em memoria**: cada `Start()` comeca
  com uma chain vazia. Se um teste precisar de estado herdado, use
  `--state` ou `--dump-state` via `ANVIL_ARGS`.
- Os benchmarks dao numeros comparaveis apenas **na mesma maquina**. Nunca
  confie em `ns/op` entre laptops diferentes.
- O `-race` aumenta consideravelmente o tempo de execucao (~2-10x).
  Em CI, rode o pipeline unitario completo com `-race` e o pipeline
  de integracao **sem** `-race` para velocidade.

---

## Modulo 4 -- Aula 1: Autenticacao JWT + Espelhamento gRPC

Esta secao documenta a **camada de autenticacao** adicionada em cima
do codigo dos Modulos 2 e 3, e a exposicao da mesma API por **dois
transportes**: REST (que ja existia) e gRPC. O objetivo pedagogico nao
e duplicar o servico: e mostrar que, quando o nucleo de autenticacao e
desenhado corretamente, adicionar um segundo transporte nao multiplica
a superficie de seguranca -- o mesmo `auth.TokenVerifier` serve aos
dois lados.

### 6.1 Objetivos de Aprendizagem

Ao final deste modulo voce sabera:

- Hashar senhas com **argon2id** (memoria-dura, recomendado pela OWASP)
  e armazenar o resultado no formato PHC string padrao.
- Emitir e verificar **JWT** com `golang-jwt/jwt/v5`, pinando o
  algoritmo (HS256) e o issuer para evitar as classicas armadilhas de
  `alg: none` e issuer-confusion.
- Diferenciar os dois propositos de token: access (JWT, sem estado,
  curta vida) e refresh (opaco, com estado, longa vida).
- Implementar **rotacao de refresh tokens** com uma cadeia
  `replaced_by` e detectar replay revogando a familia inteira.
- Escrever uma migration que adiciona o esquema sem quebrar dados ja
  existentes (default `''`, `lower(email)` como indice unico,
  `refresh_tokens` com auditoria).
- Compor um middleware HTTP de autenticacao com a stack existente
  (RequestID -> Logging -> Recovery -> JWT -> handlers).
- Escrever **arquivos `.proto`**, configurar `buf` para gerar codigo Go,
  e implementar os servidores gRPC sem reescrever a logica de negocio.
- Aplicar a mesma politica de auth em gRPC via **interceptor unario**,
  usando o mesmo `TokenVerifier`.
- Mapear `domain.AppError` para HTTP status **e** `grpc/codes.Code` em
  uma unica tabela canonica.

### 6.2 Por que dois tokens?

A pergunta surge naturalmente: se temos JWT, por que precisamos de
refresh tokens? Por que nao um JWT longo?

Resposta curta: **revogacao**. Um JWT e stateless por design -- o
servidor so precisa da chave para verificar. Isso e otimo para
performance, mas significa que **nao da para invalidar um JWT antes
da expiracao**. Se um token vazar, o atacante o usa ate o `exp`
estourar.

A solucao classica e dois tokens:

```
+----------------+              +-----------------+
| Access token   |              | Refresh token   |
| (JWT, HS256)   |              | (opaco, 32B)    |
| 15 minutos     |              | 30 dias         |
| sem estado     |              | com estado (DB) |
| revogavel? NAO |              | revogavel? SIM  |
+----------------+              +-----------------+
       |                                |
       |  request                       |  /auth/refresh
       v                                v
   API verifica            DB busca, valida, ROTACIONA
```

- **Access token** vive nos headers de cada request. Curto justamente
  porque nao da para revogar antes da expiracao.
- **Refresh token** vive numa tabela do banco (hashed). Cada
  `/auth/refresh` invalida o anterior e emite um novo, fechando a
  janela de exposicao.

Se um access token vaza, o atacante tem ate 15 minutos. Se um refresh
token vaza, o ataque e detectavel: na proxima vez que o usuario
legitimo refrescar, o token "antigo" do atacante ja sera invalido --
ou o nosso modelo detecta replay e revoga a familia inteira (ver 6.5.3).

### 6.3 Migration 005: novo esquema, sem quebrar o antigo

```sql
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash TEXT NOT NULL DEFAULT '';
CREATE UNIQUE INDEX IF NOT EXISTS users_email_lower_uq ON users (lower(email));
```

**Por que `DEFAULT ''`?** Porque os usuarios seedados em `002_seed_data`
(u1, u2, u3) ja existiam quando rodamos a migration. Sem o default, o
`ALTER TABLE ADD COLUMN NOT NULL` falharia. Com `''`, eles ficam com hash
vazio -- nao conseguem logar (a comparacao falha), mas existem para os
testes que ja exercitam `/users/{id}/...`. O testenv re-popula com um
hash valido para os testes que precisam.

**Por que `lower(email)`?** Usuarios digitam "Vitalik@Example.com" e
"vitalik@example.com" e esperam que **colidam**. Um indice funcional
em `lower(email)` resolve isso sem mutar o input do usuario (que
preserva o caso original). O repositorio faz `lower($1)` no `WHERE`
da consulta para usar o indice.

```sql
CREATE TABLE IF NOT EXISTS refresh_tokens (
    id          TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash  TEXT NOT NULL,
    issued_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked_at  TIMESTAMPTZ,
    replaced_by TEXT REFERENCES refresh_tokens(id),
    user_agent  TEXT,
    ip          INET
);
CREATE UNIQUE INDEX refresh_tokens_token_hash_uq ON refresh_tokens (token_hash);
CREATE INDEX        refresh_tokens_user_id_idx     ON refresh_tokens (user_id);
```

Pontos de discussao em sala:

- **Por que `token_hash` e nao o token plain?** Mesma razao de senhas:
  se o banco vazar, o atacante nao recebe tokens utilizaveis. Diferente
  de senha porque o input ja tem 256 bits de entropia, entao `SHA-256`
  e suficiente -- nao precisamos de argon2 para refrescar tokens.
- **Por que `replaced_by` aponta para a "proxima"?** Porque a deteccao
  de replay precisa caminhar a cadeia. Quando alguem apresenta um
  token cujo `replaced_by IS NOT NULL`, sabemos que ele ja foi
  rotacionado -- e tratamos como compromisso (ver 6.5.3).
- **`ON DELETE CASCADE`** garante que apagar um usuario apaga seus
  refresh tokens. Diferente do que faziamos para wallets, aqui o
  cascade e correto -- nao queremos refresh tokens orfaos.

### 6.4 Pacote `internal/auth`

Mantemos a logica criptografica isolada do servico. Duas estruturas:

#### 6.4.1 `Argon2idHasher` -- hashing de senhas

argon2id e a funcao recomendada pela OWASP em 2024. E **memory-hard**:
quebrar requer tanto CPU quanto RAM, defesa contra GPUs/ASICs que
dominam contra bcrypt.

Tres parametros tunaveis:

```
m (memoria)     -- KiB de RAM alocados durante o hash
t (time)        -- numero de passes sobre a memoria (custo linear)
p (parallelism) -- threads (custo paralelo)
```

Defaults: `m=65536` (64 MiB), `t=3`, `p=2`. Tunados para ~50-150 ms
por verify num laptop -- confortavel para login interativo, doloroso
para um atacante moendo bilhoes de tentativas.

Armazenamos o hash no formato **PHC string** padrao
(github.com/P-H-C/phc-string-format):

```
$argon2id$v=19$m=65536,t=3,p=2$<salt-base64>$<hash-base64>
```

O formato carrega os parametros junto com o hash. Isso significa que:

1. Podemos rotacionar parametros (m=128MiB no futuro) sem quebrar usuarios antigos -- o `Verify` le os parametros do proprio hash.
2. Outros idiomas (Python, Rust) podem ler nossos hashes -- o formato e compartilhado.

```go
hasher := auth.NewArgon2idHasher()

encoded, err := hasher.Hash("correct-horse-battery")
// $argon2id$v=19$m=65536,t=3,p=2$T4...salt...==$1f...key...

ok, err := hasher.Verify(encoded, "correct-horse-battery")
// true
```

Comparacao de constante (`subtle.ConstantTimeCompare`) protege contra
ataques de timing -- a duracao de `Verify` nao depende de **onde** o
hash diverge.

#### 6.4.2 `TokenIssuer` / `TokenVerifier` -- JWT HS256

```go
issuer := auth.NewTokenIssuer(signingKey, 15*time.Minute)
token, expiresAt, err := issuer.Issue("u-42")

verifier := auth.NewTokenVerifier(signingKey)
claims, err := verifier.Verify(token)
// claims.Subject == "u-42"
```

Tres armadilhas classicas que o `Verify` evita explicitamente:

1. **`alg: none`**. Um atacante poderia montar um JWT com header
   `{"alg":"none"}` e nenhuma assinatura. Pinamos `HS256` na keyfunc:
   se o algoritmo nao e HMAC, rejeitamos antes mesmo de verificar.
2. **issuer confusion**. Se voce so verifica a assinatura, um JWT
   emitido por outro servico (mas que compartilha a chave por
   acidente -- acontece) seria aceito. Adicionamos
   `jwt.WithIssuer(Issuer)`.
3. **exp opcional**. Um JWT sem `exp` viveria para sempre.
   `jwt.WithExpirationRequired()` recusa.

Todas as falhas de `Verify` colapsam em **um unico erro**
(`ErrInvalidToken`). Isso nao e descuido -- e proposital. Se a API
respondesse "expired" vs "bad signature" vs "wrong issuer", o
atacante usaria essa diferenca para sondar. **Uma so resposta para
todas as falhas de auth** e a regra geral.

### 6.5 Os dois tokens em acao

#### 6.5.1 Access token (JWT, stateless)

Emitido em `Login` e em cada `Refresh`. Vive em `Authorization: Bearer
<token>` em cada request. **Nao guardamos copia** -- o servidor confia
na assinatura.

#### 6.5.2 Refresh token (opaco, com estado)

Emitido junto com o access. **32 bytes aleatorios** em base64url, sem
padding. Guardamos `SHA-256(token)` em hex no banco. O cliente recebe
o plain text **uma unica vez** -- se perder, faz login de novo.

#### 6.5.3 Rotacao + deteccao de replay

```
estado inicial:
  refresh_tokens
  +------+--------+-----------+------------+-------------+
  | id   | user   | hash      | revoked_at | replaced_by |
  +------+--------+-----------+------------+-------------+
  | rt-1 | alice  | h(A)      |   NULL     |   NULL      |  <-- ativo
  +------+--------+-----------+------------+-------------+

cliente apresenta A em /auth/refresh:
  - busca por h(A): encontra rt-1
  - rt-1.revoked_at = NULL? OK, ativo
  - emite novo par (B + access)
  - INSERT rt-2 com h(B)
  - UPDATE rt-1 SET revoked_at = NOW(), replaced_by = 'rt-2'

  +------+--------+-----------+------------+-------------+
  | rt-1 | alice  | h(A)      | 2026-04-29 |   rt-2      |  <-- rotacionado
  | rt-2 | alice  | h(B)      |   NULL     |   NULL      |  <-- ativo
  +------+--------+-----------+------------+-------------+
```

Caso normal: o cliente legitimo agora tem B. Ele apresenta B na
proxima vez. A foi revogado.

**E se A aparecer de novo?**

Duas possibilidades:

- **(a)** O cliente legitimo replicou A por um bug (clock skew,
  retry sem rotacionar do lado dele).
- **(b)** Um atacante interceptou A antes da rotacao. O cliente
  legitimo seguiu adiante para B. Agora o atacante tenta usar A.

Nao da para distinguir (a) de (b) so olhando para o pedido. O modelo
escolhe a postura mais defensiva: **trata como compromisso e revoga a
familia inteira**.

```
cliente apresenta A novamente:
  - busca por h(A): encontra rt-1
  - rt-1.revoked_at != NULL  -->  REPLAY DETECTADO
  - RevokeFamily(rt-1.id): caminha replaced_by para frente
                           e replaced_by-volta para tras,
                           revoga tudo em uma volta ao banco

  +------+--------+-----------+------------+-------------+
  | rt-1 | alice  | h(A)      | (intacto)  |   rt-2      |  <-- ja revogado
  | rt-2 | alice  | h(B)      | 2026-04-29 |   NULL      |  <-- agora revogado!
  +------+--------+-----------+------------+-------------+
  - retorna 401 / Unauthenticated
```

O usuario legitimo, da proxima vez que tentar refresh com B, recebe
401 e tem que logar de novo. Custo: uma sessao perdida. Beneficio: se
era um ataque real, o atacante perdeu acesso na hora.

A logica completa esta em
`service.AuthService.Refresh`. O CTE recursivo que caminha a cadeia
em ambas direcoes esta em
`postgres.RefreshTokenRepository.RevokeFamily`.

### 6.6 Endpoints REST

```
POST /auth/register   { name, email, password }              -> 201 + user
POST /auth/login      { email, password }                    -> 200 + tokens
POST /auth/refresh    { refresh_token }                      -> 200 + tokens
POST /auth/logout     { refresh_token }                      -> 204
```

Pontos de discussao:

- **Por que nao cookies?** Porque queremos que clientes mobile e o
  mirror gRPC tratem tokens da mesma forma. Cookies sao um caminho
  feliz pra browser, mas adicionam complexidade (CSRF, SameSite, secure
  flag). Optamos por entregar no body e deixar o cliente decidir como
  guardar -- localStorage (vulneravel a XSS) vs IndexedDB vs HttpOnly
  cookie via gateway -- e uma decisao do cliente.
- **Register nao retorna token**. OWASP recomenda: o caminho de auth
  deve ser exercitado em **toda** entrada. Se Register tambem logasse,
  bugs em Login passariam despercebidos.
- **Logout retorna 204** e **e idempotente**. Se voce manda um
  `refresh_token` que nao existe, retornamos 204 igual. Nao queremos
  dar a um atacante o "sim, esse token existe" como side channel.

### 6.7 Middleware JWT

`internal/middleware/jwt.go` segue exatamente o padrao do RequestID:

```go
func JWT(verifier *auth.TokenVerifier) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            token, ok := bearerToken(r.Header.Get("Authorization"))
            if !ok {
                writeJSONUnauthorized(w, "missing or malformed Authorization header")
                return
            }
            claims, err := verifier.Verify(token)
            if err != nil {
                writeJSONUnauthorized(w, "invalid or expired token")
                return
            }
            ctx := auth.WithClaims(r.Context(), claims)
            next.ServeHTTP(w, r.WithContext(ctx))
        })
    }
}
```

Em `cmd/api/main.go` montamos **dois muxes**:

```go
publicMux := http.NewServeMux()      // /health, /auth/*, /debug/*
authHandler.RegisterRoutes(publicMux)
apiHandler.RegisterPublicRoutes(publicMux)

protectedMux := http.NewServeMux()   // /users/{id}/*
apiHandler.RegisterProtectedRoutes(protectedMux)

rootMux := http.NewServeMux()
rootMux.Handle("/users/", middleware.JWT(verifier)(protectedMux))
rootMux.Handle("/", publicMux)
```

A vantagem pedagogica: adicionar uma nova rota protegida e uma chamada
em `protectedMux` -- ela herda autenticacao automaticamente.

### 6.8 "JWT subject == path id"

Authentication e Authorization sao coisas diferentes. O middleware
JWT prova **quem voce e**. Para decidir **se voce pode** ler
`/users/{id}/portfolio`, comparamos:

```go
func (h *Handler) requireSelf(w http.ResponseWriter, r *http.Request) (string, bool) {
    pathID := r.PathValue("id")
    claims, ok := auth.ClaimsFromContext(r.Context())
    if !ok {
        write401(w)         // bug de wiring -- middleware nao foi aplicado
        return "", false
    }
    if claims.Subject != pathID {
        write403(w)         // alice tentando ler /users/bob/...
        return "", false
    }
    return pathID, true
}
```

Esta e a politica de autorizacao mais simples possivel -- "self only".
Modulo 4 Aula 2 abrira espaco para roles (admin, support).

### 6.9 Espelhando a API em gRPC

#### 6.9.1 Proto-first

`proto/portfolio/v1/auth.proto`, `portfolio.proto`, `wallet.proto`.
O `option go_package = "...gen/portfolio/v1;portfoliov1"` indica para
onde buf gera o codigo Go.

#### 6.9.2 buf como toolchain

```yaml
# buf.yaml
version: v2
modules:
  - path: proto
lint:
  use:
    - STANDARD
breaking:
  use:
    - FILE
```

```yaml
# buf.gen.yaml
version: v2
managed:
  enabled: true
plugins:
  - local: protoc-gen-go
    out: gen
    opt: [paths=source_relative]
  - local: protoc-gen-go-grpc
    out: gen
    opt: [paths=source_relative]
```

```bash
make proto-tools   # uma vez: instala buf + plugins em $GOPATH/bin
make proto         # gera gen/portfolio/v1/*.pb.go
```

Comitamos `gen/` no repositorio para que CI / alunos nao precisem
instalar buf so para construir o projeto.

#### 6.9.3 Servidor + interceptor

`internal/grpcapi/server.go` implementa as interfaces `*Server`
geradas. Cada RPC e fino: adapta o request proto para a entrada do
servico, chama o servico existente, traduz o erro.

`internal/grpcapi/auth_interceptor.go` tem o mesmo papel do middleware
HTTP, em forma gRPC:

```go
func UnaryAuthInterceptor(verifier *auth.TokenVerifier) grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
        if _, ok := publicMethods[info.FullMethod]; ok {
            return handler(ctx, req)              // Register, Login, Health bypassam
        }
        token, ok := bearerFromMetadata(ctx)
        if !ok {
            return nil, status.Error(codes.Unauthenticated, "...")
        }
        claims, err := verifier.Verify(token)
        if err != nil {
            return nil, status.Error(codes.Unauthenticated, "...")
        }
        return handler(auth.WithClaims(ctx, claims), req)
    }
}
```

Note: **mesmo `auth.TokenVerifier`** usado pelo middleware HTTP. Esse e
o ponto pedagogico central. O metadado gRPC e `authorization: Bearer
<token>` (lowercase), simetrico ao header HTTP.

#### 6.9.4 Health check

Registramos o servico padrao `grpc.health.v1.Health` para que
load-balancers e Kubernetes possam fazer probe. Tambem habilitamos
`reflection` -- assim `grpcurl` funciona sem precisar dos `.proto`.

#### 6.9.5 Servidor unico, dois transportes

```go
// HTTP
go func() { httpServer.ListenAndServe() }()

// gRPC
go func() { grpcServer.Serve(grpcLis) }()

// SIGTERM dispara desligamento ordenado em ambos
<-stopCtx.Done()
httpServer.Shutdown(ctxBudget)
grpcServer.GracefulStop()
```

Stdlib only, sem `errgroup`. O Modulo 2 ja ensinou
`sync.WaitGroup` e canais -- usamos exatamente esse vocabulario aqui.

### 6.10 Tabela canonica de erros

| Codigo de dominio   | HTTP                  | gRPC                  |
|---------------------|-----------------------|-----------------------|
| `not_found`         | 404 Not Found         | NotFound              |
| `validation_error`  | 400 Bad Request       | InvalidArgument       |
| `invalid_input`     | 400 Bad Request       | InvalidArgument       |
| `unauthenticated`   | 401 Unauthorized      | Unauthenticated       |
| `forbidden`         | 403 Forbidden         | PermissionDenied      |
| `conflict`          | 409 Conflict          | AlreadyExists         |
| `provider_failure`  | 502 Bad Gateway       | Unavailable           |
| `upstream_timeout`  | 504 Gateway Timeout   | DeadlineExceeded      |
| `internal_error`    | 500                   | Internal              |

`internal/httpapi/response.go` (REST) e `internal/grpcapi/errors.go`
(gRPC) compartilham essa tabela linha por linha. Ao adicionar um codigo
novo: adicione em **dois** switches.

### 6.11 Clientes de exemplo

#### REST -- `examples/restclient`

Roda o ciclo completo: register -> login -> get portfolio -> refresh
-> get portfolio (com token rotacionado) -> logout. **So usa stdlib**.

```bash
make run                            # numa aba
go run ./examples/restclient        # noutra
```

Saida tipica:

```
==> Using base URL: http://localhost:8080
    registered or already-existed: id=u_b1... email=alice-restclient@example.com
==> login OK
    access_token expires at  2026-04-29T13:15:42Z
    refresh_token expires at 2026-05-29T13:00:42Z
==> GET /users/{id}/portfolio (with access token)
    response: {"user_id":"u_b1...", ...}
==> refresh OK -- got new pair (rotation chain advanced)
    old refresh token is now revoked
==> GET /users/{id}/portfolio (with rotated access token)
==> logout OK
```

#### gRPC -- `examples/grpcclient`

Mesmo ciclo (sem refresh/logout, que sao REST-only no Aula 1):

```bash
go run ./examples/grpcclient
```

Demonstra `metadata.AppendToOutgoingContext(ctx, "authorization",
"Bearer <token>")` -- o equivalente gRPC do header HTTP. O **mesmo
access_token** funciona nos dois transportes.

#### Por curl

```bash
TOKEN=$(curl -s -X POST localhost:8080/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"alice@example.com","password":"correct-horse-battery"}' \
  | jq -r .access_token)

curl -H "Authorization: Bearer $TOKEN" \
  localhost:8080/users/<user_id>/portfolio
```

#### Por grpcurl

```bash
grpcurl -plaintext -d '{"email":"alice@example.com","password":"correct-horse-battery"}' \
  localhost:50051 portfolio.v1.AuthService/Login

grpcurl -plaintext \
  -H "authorization: Bearer $TOKEN" \
  -d '{"user_id":"<id>"}' \
  localhost:50051 portfolio.v1.PortfolioService/GetPortfolio
```

`grpcurl` usa server reflection (que habilitamos) -- nao precisa dos
`.proto`.

### 6.12 Testes -- a piramide, agora atravessando dois transportes

Mantemos a mesma estrutura do Modulo 3, expandida:

- **Unidade**: `internal/auth/argon2_test.go`, `jwt_test.go`,
  `service/auth_service_test.go` -- argon2 round-trip, JWT
  expirado/tampered/wrong-key, login com senha errada, refresh com
  rotacao + replay, logout idempotente. Mocks via `testify/mock`
  (compartilhado com o Modulo 3).
- **Middleware**: `internal/middleware/jwt_test.go` -- header malformado,
  bearer case-insensitive, token expirado, panic em `JWT(nil)`.
- **Handler HTTP**: `internal/httpapi/handler_test.go` -- requireSelf
  bloqueia cross-account com 403 e 401 quando claims faltam.
- **Integracao** (build tag `integration`):
  - `test/integration/auth_http_test.go` -- happy path completo,
    deteccao de replay revogando a familia, cross-account 403, login
    com senha errada e email inexistente produzindo respostas
    **identicas** byte a byte (sem oracle).
  - `test/integration/auth_grpc_test.go` -- mesmo cenario via
    `grpc.Dial` em porta real (nao bufconn), inclui o teste
    headline: **token emitido por gRPC funciona em REST**.

```bash
make test-unit          # rapido, sem Docker
make test-integration   # com Docker, valida REST e gRPC fim a fim
```

### 6.13 Para a Aula 2

Aula 1 deliberadamente deixou tres temas em aberto, que formarao um
arco coeso para a Aula 2:

1. **Refresh + Logout em gRPC**. Hoje sao REST only. A versao gRPC vai
   exigir que os interceptors saibam diferenciar metodos publicos e
   protegidos -- e eles ja sabem -- e o servico extender o
   `AuthServiceServer` com dois RPCs. Discutiremos por que faz sentido
   ter o mesmo servico em dois transportes.
2. **Observabilidade da auth**. Migrar de `log.Printf` para `slog`
   estruturado. Adicionar um log de auditoria por evento de auth
   (login OK, login fail, refresh, logout, replay detected). No gRPC,
   adicionar um interceptor de tracing (OpenTelemetry).
3. **Resiliencia**. Rate-limit em `/auth/login` para frear ataques de
   forca bruta. Block list de tokens conhecidos comprometidos
   (denylist por `jti`).

### 6.14 Fluxo Sugerido para a Aula

1. **Conceitos (~15 min)** -- abrir 6.2/6.5 no quadro. Discutir por
   que dois tokens, e desenhar o fluxograma da rotacao + replay.
2. **argon2id (~15 min)** -- abrir `internal/auth/argon2.go`,
   passar pelo formato PHC. Discutir por que **nao** bcrypt (memory
   hardness vs GPU-friendliness).
3. **JWT (~15 min)** -- abrir `jwt.go`. Os tres pinning (algoritmo,
   issuer, exp). Mostrar `TestTokenVerifier_Verify_RejectsAlgNone`.
4. **Migration + repos (~10 min)** -- abrir `005_add_user_auth.up.sql`
   e `refresh_token_repository.go`. Destacar `RevokeFamily` com o CTE
   recursivo bidirecional.
5. **Servico (~25 min)** -- abrir `service/auth_service.go`. Caminhar
   `Login` (constant timing!), `Refresh` (rotacao + replay).
6. **Middleware + handler REST (~15 min)** -- mostrar o two-mux split
   em `cmd/api/main.go`. Rodar `examples/restclient` ao vivo.
7. **gRPC (~25 min)** -- abrir os `.proto`, rodar `make proto`,
   passar pelo interceptor e server.go. Rodar `examples/grpcclient`
   ao vivo. Demo final: o **mesmo token funciona nos dois lados**.
8. **Testes (~10 min)** -- abrir `auth_http_test.go` e `auth_grpc_test.go`.
   Mostrar como o teste de cross-transport amarra o final da aula.

---

## Desafios para Casa

Estes desafios vao alem do material da aula. Eles exigem pesquisa e aplicacao
criativa dos padroes aprendidos.

### 1. Adicionar suporte a WebSocket

Substitua o polling do event watcher por uma subscription WebSocket (`eth_subscribe`).
Voce precisara:

- Gerenciar a conexao WebSocket (connect, reconnect, backoff)
- Manter o mesmo pipeline downstream (normalizer -> fan-out -> consumers)
- Comparar: o que muda na arquitetura? O que permanece igual?

### 2. Implementar um dashboard em tempo real

Crie um endpoint WebSocket no `cmd/api` que transmita eventos do watcher para
clientes conectados. Use um padrao de broadcast (similar ao FanOut) para enviar
eventos a todos os clientes conectados.

### 3. Suporte a multiplas blockchains

Estenda o watcher para monitorar eventos em Ethereum, Polygon e BSC simultaneamente.
Cada blockchain tera seu proprio poller, mas todos alimentarao o mesmo pipeline
de normalizacao e persistencia. Considere como usar `Merge` para combinar os
streams.

### 4. Lookup historico de precos

O snapshot runner atualmente usa o preco atual. Implemente um lookup historico
que busque o preco no momento do snapshot (`reference_time`). Isso requer:

- Uma nova interface `HistoricalPriceProvider`
- Cache de precos historicos (para nao repetir chamadas)
- Worker pool para buscar precos em paralelo com saldos

---

## Estrutura do Projeto

```
portfolio-api/
├── cmd/
│   ├── api/main.go                              # REST API server
│   ├── event-watcher/main.go                    # Monitor de eventos Ethereum (NOVO)
│   └── snapshot-runner/main.go                  # Gerador de snapshots (NOVO)
├── internal/
│   ├── concurrent/                              # Helpers de concorrencia reutilizaveis (NOVO)
│   │   ├── workerpool.go                        # RunWorkerPool[I,O]
│   │   ├── workerpool_test.go
│   │   ├── fanout.go                            # FanOut[T] + Merge[T]
│   │   ├── fanout_test.go
│   │   ├── pipeline.go                          # Stage[I,O] + Generate[T]
│   │   └── pipeline_test.go
│   ├── config/config.go                         # Variaveis de ambiente
│   ├── contracts/contracts.go                   # Interfaces (ports/adapters)
│   ├── domain/
│   │   ├── errors.go                            # Sentinel errors, AppError
│   │   ├── event.go                             # WalletEvent (NOVO)
│   │   ├── portfolio.go                         # Portfolio, Holding
│   │   ├── snapshot.go                          # WalletSnapshot, WalletSnapshotItem (NOVO)
│   │   ├── user.go                              # User
│   │   └── wallet.go                            # Wallet
│   ├── httpapi/                                 # Handlers HTTP e mapeamento de erros
│   ├── middleware/                              # RequestID, Logging, Recovery
│   ├── provider/
│   │   ├── blockchain/
│   │   │   ├── ethereum.go                      # Saldo ETH via JSON-RPC
│   │   │   ├── ethereum_logs.go                 # Fetcher de event logs (NOVO)
│   │   │   ├── ethereum_token.go                # Saldo de tokens ERC-20 (NOVO)
│   │   │   ├── klever.go                        # Saldo KLV via REST
│   │   │   └── registry.go                      # Registry de providers
│   │   └── pricing/                             # CoinGecko + mock
│   ├── repository/postgres/
│   │   ├── event_repository.go                  # Persistencia de eventos (NOVO)
│   │   ├── snapshot_repository.go               # Persistencia de snapshots (NOVO)
│   │   ├── user_repository.go
│   │   └── wallet_repository.go
│   ├── snapshot/
│   │   ├── runner.go                            # Pipeline: Generate → WorkerPool → Aggregate (NOVO)
│   │   ├── runner_test.go                       # Stubs manuais
│   │   └── runner_extra_test.go                 # Mocks (testify/mock) + benchmarks (MOD. 3)
│   ├── testutil/                                # Infra compartilhada de testes (MOD. 3)
│   │   ├── anvil/container.go                   # testcontainers + Anvil (MOD. 3)
│   │   ├── ethutil/rpc.go                       # Cliente JSON-RPC minimo (MOD. 3)
│   │   ├── ethutil/anvil.go                     # Helpers de mutacao de chain (MOD. 3)
│   │   ├── ethutil/erc20.go                     # Helpers de ERC-20 / tx (MOD. 3)
│   │   ├── mocks/mocks.go                       # Mocks testify/mock (MOD. 3)
│   │   ├── postgres/                            # Helper Postgres (ja existia)
│   │   └── testenv/                             # Bootstrap integrado (MOD. 3)
│   │       ├── env.go                           # Setup + Env (TestMain)
│   │       ├── bootstrap.go                     # DB → chain seeding
│   │       ├── fixtures.go + fixtures.json      # tokens/whales/amounts
│   │       └── seed.go                          # Helpers de DB
│   └── watcher/
│       ├── normalizer.go                        # RawLog → WalletEvent (NOVO)
│       ├── normalizer_test.go
│       ├── normalizer_extra_test.go             # Edge cases + benchmarks (MOD. 3)
│       └── watcher.go                           # Pipeline: Poller → Stage → FanOut → Consumers (NOVO)
├── migrations/
│   ├── 001_create_tables.sql
│   ├── 002_seed_data.sql
│   ├── 003_create_event_tables.sql              # (NOVO)
│   └── 004_create_snapshot_tables.sql           # (NOVO)
├── test/                                        # (MOD. 3)
│   ├── infrastructure/anvil/                    # Imagem customizada do Anvil
│   │   ├── Dockerfile
│   │   ├── embed.go                             # //go:embed expondo FS
│   │   └── README.md
│   └── integration/                             # Tag `integration`, roda via `make test-integration`
│       ├── main_test.go
│       ├── helpers_test.go
│       ├── tx_helpers_test.go                   # erc20TransferCalldata + sendTx
│       ├── snapshot_integration_test.go
│       ├── logs_fetcher_integration_test.go
│       └── watcher_integration_test.go          # Forked mainnet + whale impersonation
├── docker-compose.yml
├── EXERCISES.md
├── Makefile
└── go.mod                                       # Go 1.25; +testify, +testcontainers-go, +migrate
```
