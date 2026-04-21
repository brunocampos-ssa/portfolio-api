# Portfolio API -- Modulo 3: Concorrencia Avancada em Go

Material de aula para o modulo de **concorrencia avancada** do curso de Go.
Este modulo usa o mesmo projeto `portfolio-api` dos modulos anteriores, mas agora
introduz dois novos binarios que resolvem problemas naturalmente concorrentes:
monitoramento de eventos blockchain em tempo real e geracao de snapshots de saldo
para simulacao de relatorio fiscal.

> **Pre-requisito**: Modulo 2 (Tratamento de Erros, Panic/Recover, Programacao Defensiva).
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

## Testes

### Estrategia de testes

O projeto testa os padroes de concorrencia de forma isolada, sem depender de
infraestrutura externa. Cada teste verifica uma propriedade especifica do padrao.

**`concurrent/` -- Testes dos helpers de concorrencia:**

| Teste | O que verifica |
|-------|----------------|
| `TestRunWorkerPool_ProcessesAllItems` | Todos os items sao processados, resultados coletados |
| `TestRunWorkerPool_BoundedConcurrency` | No maximo `numWorkers` goroutines rodam ao mesmo tempo (usa `atomic.Int32`) |
| `TestRunWorkerPool_ContextCancellation` | Workers param quando o contexto e cancelado |
| `TestRunWorkerPool_EmptyInput` | Canal vazio resulta em output vazio, sem panic |
| `TestFanOut_BroadcastsToAllConsumers` | Cada consumidor recebe TODOS os items |
| `TestFanOut_ContextCancellation` | FanOut para quando contexto cancela |
| `TestMerge_CombinesMultipleChannels` | N canais sao fundidos em um |
| `TestMerge_EmptyInputs` | Canais vazios produzem output vazio |
| `TestFanOut_ThenMerge` | FanOut seguido de Merge produz N copias de cada item |
| `TestStage_TransformsItems` | Stage aplica funcao de transformacao, preserva ordem |
| `TestStage_FiltersItems` | Stage descarta items quando `keep=false` |
| `TestStage_ChainedPipeline` | Multiplos stages compostos funcionam corretamente |
| `TestGenerate_CreatesChannelFromSlice` | Slice vira canal com todos os items |
| `TestGenerate_EmptySlice` | Slice vazio produz canal vazio |

**`watcher/` -- Testes do normalizador:**

| Teste | O que verifica |
|-------|----------------|
| `TestNormalizeTransferLog_IncomingTransfer` | Transfer para endereco rastreado -> evento `incoming` |
| `TestNormalizeTransferLog_OutgoingTransfer` | Transfer de endereco rastreado -> evento `outgoing` |
| `TestNormalizeTransferLog_UntrackedAddress` | Transfer entre enderecos nao rastreados -> `nil` |
| `TestNormalizeTransferLog_RemovedLog` | Log removido por reorg -> `nil` |
| `TestNormalizeTransferLog_NotTransferEvent` | Evento que nao e Transfer -> `nil` |
| `TestNormalizeTransferLog_InsufficientTopics` | Log com < 3 topics -> `nil` |

**`snapshot/` -- Testes do runner:**

| Teste | O que verifica |
|-------|----------------|
| `TestRunner_Run_ProcessesAllWallets` | Pipeline completo: load → generate → pool → aggregate |
| `TestRunner_Run_WithTokens` | Saldos ERC-20 sao incluidos quando tokenProvider existe |
| `TestRunner_Run_NoWallets` | Retorna erro quando nao ha wallets |
| `TestNewRunner_PanicsOnNilDeps` | Construtor faz panic em dependencias nil (programacao defensiva) |

### Como rodar

```bash
# Todos os testes
make test

# Testes de um pacote especifico
go test ./internal/concurrent/ -v
go test ./internal/watcher/ -v
go test ./internal/snapshot/ -v

# Um teste especifico
go test ./internal/concurrent/ -run TestRunWorkerPool_BoundedConcurrency -v
```

---

## Exercicios

Os exercicios praticos para este modulo estao no arquivo [`EXERCISES.md`](EXERCISES.md).

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
│   │   └── runner_test.go
│   └── watcher/
│       ├── normalizer.go                        # RawLog → WalletEvent (NOVO)
│       ├── normalizer_test.go
│       └── watcher.go                           # Pipeline: Poller → Stage → FanOut → Consumers (NOVO)
├── migrations/
│   ├── 001_create_tables.sql
│   ├── 002_seed_data.sql
│   ├── 003_create_event_tables.sql              # (NOVO)
│   └── 004_create_snapshot_tables.sql           # (NOVO)
├── docker-compose.yml
├── EXERCISES.md
├── Makefile
└── go.mod                                       # Go 1.25, unica dep: github.com/lib/pq
```
