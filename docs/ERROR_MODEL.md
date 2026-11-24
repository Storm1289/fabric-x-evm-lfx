# Error model: transient, rejected, terminal, and programming faults

## Introduction

### The problem

Today the codebase has no shared notion of what kind of failure an error represents,
and the classifications it does have are local, mutually incompatible, and erased as
soon as the error is returned. On the endorse path, `classify()`
([`endorser.go:171`](../endorser/core/endorser.go)) maps engine error types onto wire
statuses, and `ExecuteTransaction` ([`endorse.go:78`](../gateway/core/endorse.go))
turns those statuses back into behaviour. The commit pipeline classifies nothing at
all: its dispatch loop hands the first handler error to a sink, and the sink panics on
anything that is not a cancellation. hybridx then keeps a third, private vocabulary on
top of that, in which the only marker separating a programming fault from any other
fault is the wording of one panic message.

Because each classification is flattened into formatted message text as it crosses a
function boundary, no consumer can act on what an error means. A sink cannot
distinguish a storage fault that is worth another attempt from a misconfiguration that
will fail every request until an operator intervenes, and the endorse path delivers a
disagreement between the gateway's and the endorser's clocks and a transaction rejected
at the nonce as the same `StatusTxRejected`, with the cause present only in the message. Retry therefore exists
only where a developer happened to add it, and alerting has nothing to count.

### Solution outline

The proposal is to classify once and make the classification a property of the error
value, so that four kinds — `Transient`, `Rejected`, `Terminal` and `Programming`, with
`Unknown` as the zero value for anything nobody has looked at — answer the two questions
a consumer actually has, whether re-attempting can help and who has to act if it cannot.
A source decides the hard case with one question, would the next request fail the same
way, and the kind then travels on the error itself, so ordinary `%w` propagation carries
it without any intermediate layer knowing the model exists. Only sinks act on it: they
are the sole places that log, count, retry, panic or answer a client, every one of them
handles all four kinds and `Unknown` explicitly, and the document names them so that
"finished" is a thing you can check. What a sink does is fixed by the kind —
`Programming` panics in every build, `Terminal` logs and alerts and is fatal only outside
a release build, `Rejected` answers its submitter where there still is one — except for
retry, which belongs to the operation
rather than the kind, so the sink that owns a call sets its own attempts and backoff from
what that call costs. The endorse path and the commit-notification pipeline adopt it
first, staged so that nothing is classified on the commit that introduces the package and
each sink starts rejecting `Unknown` only once its own sources are converted, which makes
the per-kind counter a measure of how far the migration has got. One question the model
does not settle is what a client should see when its transaction is `Rejected`, because no
standard JSON-RPC shape exists for a permanent rejection; it is left open at the end.

## Scope and non-goals

In scope:

- One classification, applied to two failure paths: transaction execution from the
  gateway through the endorser, and the commit-notification handlers the synchronizer
  runs per block.
- The resume height, because panicking is a designed outcome and the restart point
  decides whether a restart is safe. A restart subscribes from one past the height the
  block store reports, and the block store is the first handler in the chain, so a
  restart never replays a block for the handlers behind it — it skips one. A panic in
  `Gateway.Handle` therefore loses that block's transactions for the queue and the nonce
  gate for good.
- Making a retried commit handler replay-safe, because a per-handler retry is what hands
  the same block to the same handler twice. `Gateway.Handle` is the one that cannot take
  it: it has no record of what it has already handled, and a re-delivered block whose
  transactions conflicted reaches `adopt`, which executes and submits them again. Each
  handler that is retried keeps the height it last completed and returns early at or
  below it.
- Done is defined by the sequencing section: the model is complete for these paths on
  the commit where the last sink rejects `Unknown`.

Out of scope:

- Every other error in the repository. Errors outside these paths keep their current
  form, and one that reaches a sink is `Unknown`, which records that the model has not
  looked at it rather than that it does not matter.
- Startup and configuration paths. They run once, in a process that is already refusing
  to start, where a message for an operator beats a policy.
- A client-visible error for a rejected transaction, and the window in which two
  transactions at one nonce both pass pre-flight. Replacement transactions (#62) would
  remove most of the latter; both stay open at the end.

What does not change:

- `peer.Response.Status` keeps its meanings. The fault record rides in `Response.Payload`,
  which is nil on all three of the endorser's failure paths and needs no plumbing: it is
  `payload` on `ExecuteResponse`, already filled by the server and already read back by
  the client, so a remote endorser's kind travels without a proto change. It does not
  collide with the read-write set, which the queue reads from the top-level `Payload`.
- Revert and execution failure are deliberately not kinds. Both are application outcomes
  that may be committed; the kinds explain why an operation did not complete, not what
  the EVM decided.
- The JSON-RPC codes and the classifier that produces them are unchanged in shape. Three
  admission errors move between them: a failed nonce lookup, a closed queue and a full
  admitted channel are all `-32003` today and all become `-32603`, since each is about
  this node rather than a rule the transaction broke.
  [`JSON_RPC_ERRORS.md`](JSON_RPC_ERRORS.md) specifies the first as a pending fix and the
  other two as `-32003`, so it changes alongside this.
- On the commit side the sinks stay fabric-x only, since `common.ProtocolFabric` routes
  to the SDK's `nfab.NewSynchronizer`
  ([`synchronizer.go:60`](../synchronizer/synchronizer.go)), where hybridx is not in the
  picture. The retry in `nsFilter` is shared code, but only hybridx's sinks consume the
  kind that motivates it.

## The four kinds

A classified error carries exactly one of four kinds, and the kind answers the two
questions a sink has: whether re-attempting the operation can help, and who has to act
if it cannot. `Unknown` is not a fifth kind but the absence of classification; it is the
zero value, so an error that no source has examined reports itself as unexamined
instead of being sorted into one of the four.

- `Transient` — the identical operation may succeed if it is attempted again, as with a
  gRPC blip or a busy database. The kind says only that re-attempting is not pointless.
  Whether this sink is the right place to retry, and under what policy, is a separate
  decision it makes for itself (see [Retrying a transient
  fault](#retrying-a-transient-fault)).
- `Rejected` — the request will never be accepted as submitted, while the deployment is
  healthy, as with a transaction whose nonce no longer matches the ledger. The submitter
  has to change or re-derive the request, so the sink answers them where it can and logs
  the case distinctly where it cannot.
- `Terminal` — the request is fine and the deployment is not, as with a misconfigured
  namespace, clocks that disagree, or an unrecognised protocol status. No request
  succeeds until an operator acts, so the sink logs an error, raises an alertable
  counter, and debug-panics so that a test or a development run cannot pass over it
  quietly.
- `Programming` — an invariant that this code guarantees has been violated. A retry is
  meaningless and so is carrying on, so the sink logs, counts, and panics, in every
  build.

## Panicking, and the two kinds of panic

Three of the five values can stop the process, and they do not all stop it under the
same conditions, so the difference is part of the model rather than an implementation
detail.

A plain `panic` is unconditional: it takes the process down in every build, release
included. `common.DebugPanic` panics only when the binary was not built with
`-tags release` ([`debug.go`](../common/debug.go)), and `DebugBuild` is a compile-time
constant, so in a release build the call is a no-op and the sink carries on with the
log and the counter it already produced.

The kinds map onto them like this:

| Kind | Development, CI, tests | Release build |
|---|---|---|
| `Programming` | log, count, `panic` | log, count, `panic` |
| `Terminal` | log, count, `DebugPanic` | log, count, carry on |
| `Unknown` | log, count, `DebugPanic` | log, count, carry on |
| `Transient`, `Rejected` | never panic | never panic |

`Programming` panics everywhere because the condition is a statement about this code,
not about the deployment or the request. If an invariant this code guarantees has been
violated, nothing else the process is holding is trustworthy either, and continuing
means running on state the code says is impossible. It is also the only response that
reliably reaches a person: a release build that swallowed the fault would carry on
serving other transactions, leaving a log line and a counter as the only trace that one
of its guarantees had gone.

This is a change in behaviour, and it is worth being explicit about what it buys and
what it costs. Every `Programming` site in the table debug-panics today, so a release
build survives all of them. Several of those faults are about one transaction — an
endorsement with no readable read-write set, a status the in-process endorser should
never have produced — and after this change any one of them takes the gateway down in
production. That is the intended trade: these are conditions that cannot arise unless
the code is wrong, so the right response is a crash and a stack trace, not a degraded
node that keeps answering.

That trade is only sound where the condition genuinely cannot be provoked from outside
the process, so the kind carries two limits. The first is a test a source has to pass
before it may claim `Programming`: no input can produce this condition, however hostile
or merely out of date the sender is. A fault a client can trigger by deploying an
ordinary contract, or a remote endorser can trigger by running last month's build, is
not an invariant this code guarantees and broke — it is a condition this code does not
handle, and a release build that crashes on it has handed the deployment's availability
to whoever sends the next transaction. Such a fault is `Terminal`: still loud in CI and
in a development run, still counted, and no longer a way to stop the node.

The second limit is who decided the kind. A kind produced inside this process is a
statement this binary makes about its own invariants, and it is as trustworthy as the
binary. A kind read back out of `Response.Payload` is a statement another organisation's
endorser makes, across a version boundary this deployment does not control, and it
cannot carry the same authority. A kind that arrives from outside this process is
therefore never `Programming`: `proposalResponse`
([`client.go:165`](../endorser/client/client.go)) is the one function that lifts a remote
endorser's payload into a response the gateway reads, so it is the one place the
demotion to `Terminal` belongs. That is the same argument the unknown-status rows below
make, applied once at the boundary rather than row by row.

`Terminal` is the opposite case. A misconfigured namespace or a version mismatch between
the gateway and a remote endorser fails every transaction identically, and crashing
neither fixes it nor helps anyone find it — a gateway that crash-loops through a rolling
upgrade is worse than one that logs and raises a counter. But the same condition in a
test or a development run almost always means a wiring mistake, and a log is easy to
miss, so it debug-panics there. `Unknown` follows `Terminal` for the same reason, with
the migration in mind: loud while the model is being adopted, conservative in production
because the set of errors a dependency can produce is not closed.

## Carrying the kind on the error

The kind is a property of the error value rather than a second return value, which is
what lets it survive the layers between a source and a sink. A type declares its own kind
with a single `FaultKind() Kind` method, so classification costs no allocation and the
type identity that existing `errors.AsType` checks depend on is unchanged; a site with no
error type of its own tags a wrapped error through `fault.Wrap`; and `fault.KindOf` walks
the chain from the outside in, returning `Unknown` when it finds no kind at all. Because
an ordinary `fmt.Errorf("...: %w", err)` carries no kind of its own, that walk passes
through to the inner error, so propagation preserves the classification without the
propagator knowing about it, while a layer that genuinely knows better overrides it
deliberately.

## When several errors arrive at once

A chain has one innermost error, so walking it answers the question. A joined error
forms a tree, and a fan-out across several endorsers produces one error per endorser,
and in both cases something has to decide which kind the group reports. The rule is that
the kind least likely to be fixed by retrying wins, in this order:

    Programming > Terminal > Rejected > Transient

So a group containing a `Rejected` branch is `Rejected` even if a sibling was a transport
blip. That is the answer that matters on the endorse path: if one endorser says the nonce
is below its state, the transaction will never be accepted as submitted, and retrying
because another endorser happened to time out only wastes the budget. "Most severe" is
deliberately not the phrasing, because it invites the opposite reading.

`ExecuteTransaction` has to join its endorsers' errors for this to apply. Today it returns
the first error in slice order ([`endorse.go:172`](../gateway/core/endorse.go)), which
picks by goroutine layout rather than by meaning, so a blip from the first endorser hides
a rejection from the second.

One disagreement is not resolved by the ordering at all, because the disagreement is
itself the diagnosis: if one endorser reports the nonce as too low and another reports it
as too high, they do not share a view of the ledger, and no kind carried by either branch
describes that. The join reports `Terminal`.

## Sources, propagators, and sinks

The three roles divide the work. A source is where a failure first becomes an error, and
it is the only place with enough information to classify it; it classifies and does not
log. A propagator adds context and returns, using `fmt.Errorf("...: %w", err)` and
nothing more, and it never classifies, logs, or decides. A sink is where an error stops,
so it is the only place that logs, counts, retries, panics, or answers a client, and it
decides from `KindOf` rather than from message text. Every sink handles all four kinds
and `Unknown` explicitly. The one exception is a source that can see a cause which
would be lost by the time the error reached a sink; it may log and act on the kind while
the cause is still visible, as `ExecuteTransaction` does for request-building faults
([`endorse.go:85`](../gateway/core/endorse.go)).

## Retrying a transient fault

`Transient` does not say where to retry, and the sink that owns a call is the only place
that knows what retrying it costs. So each retry policy — how many attempts, and with
what backoff — is derived from the operation itself: how long its remote side plausibly
takes to recover, and how much of the pipeline an attempt occupies. A policy is never
derived from another component's timers, and it does not have to keep a transaction's
successors from being evicted while it runs: their nonce has not moved, so the client
resubmits them. (One existing budget is justified by exactly that kind of cross-component
argument, in [`depgraph_queue.go:53`](../gateway/core/depgraph_queue.go), and its comment
should be rewritten to say what the budget is really for.)

What a policy does have to respect is the pipeline it occupies while it waits. An
endorser call retried on an endorse worker holds one of the eight workers for the whole
policy, and a general endorser outage therefore holds all of them, fills the admitted
channel behind them, and turns a transport fault into a refusal at admission. That is
why the full channel is `Transient` backpressure in the table below rather than a
violated invariant, and why a long policy belongs off the draining worker rather than on
it. The same applies one stage later: a retry in `worker` holds the transaction's
dependency-graph node, so its dependents stay held for as long as the retry runs.

## The endorse path

This section applies the model to the endorse path, from admission to the commit that
closes the loop, and gives every failure on it one of the four kinds — except for the two
the model cannot reach, which are called out as such in the table rather than given a kind
they do not have. A failure that happens before the transaction is admitted is
answered to the client synchronously as a JSON-RPC error and is classified where it
occurs, without passing through a sink. A failure after admission has no caller left to
answer, so it ends at a sink, and it is usually about the deployment rather than the
transaction: request building, clocks that disagree, transport, a server fault, or the one
client error that pre-flight cannot catch, two different transactions at one nonce. Because the gateway's local endorser is called
in process, the kind travels back in `peer.Response.Payload`, since a new protobuf field
on the response would not reach it. The sinks then retry transport faults under their own
policies, report a rejection to its submitter as far as the open question allows, and
raise an alert for terminal faults.

### The sinks on this path

The definition of done is that every sink rejects `Unknown`, so the list has to be
complete rather than indicative. These are the places where an error on this path stops:

| Sink | Where | What it owns |
|---|---|---|
| The RPC boundary | [`eth.go:270`](../gateway/api/eth.go) | the client's synchronous answer, for everything up to and including admission |
| The endorser's `Execute` | [`endorser.go:83`](../endorser/core/endorser.go) | turning an error into a status and a kind on the wire; logging belongs here, not in the engine below it |
| `submitToManager` | [`depgraph_queue.go:409`](../gateway/core/depgraph_queue.go) | the first endorsement, and the retry policy for that call |
| `worker` | [`api.go:165`](../gateway/core/api.go) | the final endorsement, made when the stored one cannot be reused, and the retry policy for that call |
| `markReady` | [`depgraph_queue.go:527`](../gateway/core/depgraph_queue.go) | what the dependency manager hands back |
| `BatchSubmitter.worker` | [`batch_submitter.go:138`](../gateway/core/batch_submitter.go) | the orderer's answer |
| `nonceGate.Observe` | [`nonce_parking.go:272`](../gateway/core/nonce_parking.go) | a parked transaction released by a commit, which has no caller left |
| `untrackOrRetry` via `Handle` | [`depgraph_queue.go:731`](../gateway/core/depgraph_queue.go) | the MVCC retry budget and the conflicts it cannot resolve |

Two places stop being sinks. `ExecuteTransaction` logs and debug-panics over a server
fault or an unrecognised status today ([`endorse.go:145`](../gateway/core/endorse.go)),
which is what flattens the endorser's four distinct faults into one branch; once the
kind rides on the wire it classifies and returns, and the sink that called it decides.
`batchLoop` drops a whole pending batch when the context is done
([`depgraph_queue.go:486`](../gateway/core/depgraph_queue.go)) without untracking those
transactions, which leaves entries behind that nothing will ever finish; a cancellation
there should untrack rather than warn.

The commit handlers themselves — converting a block and storing it — are the next
section's subject, not this one's.

### Where a transaction can fail

The table lists every failure the transaction can meet on this path, the kind it gets,
what happens today, and what the change is. Whether a row panics follows from its kind
and is not repeated per row: `Programming` panics in every build, `Terminal` debug-panics
outside a release build, and nothing else panics at all.

| Stage | Failure | Kind | Today | Change |
|---|---|---|---|---|
| Admission | The raw transaction does not decode | `Rejected` | `-32602`, before the gateway sees it | unchanged, classified where it happens |
| Admission | Stateless validation fails: unprotected transaction, unsupported type, oversize, fee or gas rule, bad signature | `Rejected` | `-32003` through the fallback branch | `-32003`, now chosen from the kind |
| Admission | The same transaction is already queued or parked | `Rejected` | `-32003` | unchanged |
| Admission | Nonce below the gate's next expected nonce | `Rejected` | `-32003` | unchanged |
| Admission | Too many future-nonce transactions parked for one sender | `Rejected` | `-32003` | unchanged |
| Admission | The nonce gate cannot recover the sender, although validation already did | `Programming` | `-32003` | panic, since both checks use the same signer |
| Admission | The sender's nonce cannot be read from the store | `Transient` | `-32003`, because the failure is not wrapped as a lookup fault | wrap it, answer `-32603`, count the kind |
| Admission | The queue is closed for shutdown | `Transient` | `-32003` | answer `-32603`, so the client retries elsewhere |
| Admission | The queue's admitted channel is full | `Transient` | debug-panic, `-32003` in a release build | backpressure: answer `-32603`, count the kind |
| Endorsement | The transaction cannot be marshalled for the invocation | `Programming` | debug-panic, dropped | dropped, and now fatal in a release build too |
| Endorsement | The remote client cannot marshal the transaction for its own request | `Programming` | a bare error the gateway cannot tell from a transport fault | classified at the client, so no retry treats it as transport |
| Endorsement | The invocation cannot be built: the SDK builder refuses it, or `nsVersion` is unparseable on the classic-Fabric path | `Terminal` | debug-panic, dropped | error log and alert, dropped, no longer fatal in a release build |
| Endorsement | The endorser call fails in transport | `Transient` | dropped on the first failure | retried under that call's own policy, then dropped |
| Endorsement | The endorser answers `Unimplemented`, so it does not serve this call at all | `Terminal` | dropped | error log and alert, dropped, no retry |
| Endorsement | The endorser answers `InvalidArgument`, so it cannot decode a transaction this gateway encoded | `Terminal` | dropped, indistinguishable from a transport fault | error log and alert, dropped, no retry |
| Endorsement | Rejection because the gateway's and the endorser's clocks disagree by more than the skew window | `Terminal` | dropped, indistinguishable from the next rows | terminal kind on the wire; error log and alert, dropped |
| Endorsement | Rejection before execution: the signature does not recover, the account cannot pay, the intrinsic gas is too high, or the nonce is below the endorser's state | `Rejected` | dropped, indistinguishable from its neighbours | rejected kind on the wire; answer the submitter, else log and count it apart |
| Endorsement | The nonce is above the endorser's state, so the endorser is behind | `Transient` | dropped as a rejection | transient kind on the wire; retried while the endorser catches up |
| Endorsement | Two endorsers contradict each other on the nonce, one too low and one too high | `Terminal` | whichever came first in slice order, as a rejection | the disagreement is the diagnosis: error log and alert, dropped |
| Endorsement | The endorser cannot open a state snapshot | `Transient` | returned as `StatusServerError`, which the gateway debug-panics over and drops | carried as transient on the wire and retried |
| Endorsement | A state read fails during execution | `Transient` | the state database panics, and nothing recovers it, so the process dies | recovered at the endorser's `Execute` boundary and returned as a transient fault |
| Endorsement | The executor cannot be built: no chain config, or no block time | `Programming` | returned as `StatusServerError`, which the gateway debug-panics over and drops | dropped, and now fatal in a release build too |
| Endorsement | The endorser cannot marshal the result metadata or logs, or cannot sign the endorsement | `Programming` | debug-panic, dropped | dropped, and now fatal in a release build too |
| Endorsement | The status is one the gateway does not know, from a remote endorser, which means a version mismatch | `Terminal` | debug-panic, dropped | error log and alert, dropped, no longer fatal in a release build |
| Endorsement | The status is one the gateway does not know, from the in-process endorser | `Programming` | debug-panic, dropped | dropped, and now fatal in a release build too |
| Endorsement | The local endorser's response carries no read-write set the queue can read | `Programming` | debug-panic, dropped | dropped, and now fatal in a release build too |
| Endorsement | A remote endorser's response carries no readable read-write set, although its status was committable | `Terminal` | not noticed here: the payload is never examined, and the mismatch surfaces at submission as a bytewise payload difference, dropped as if the orderer had failed | checked where the responses are collected, so the fault names the endorser; error log and alert, dropped, no retry |
| Scheduling | The dependency manager returns a reference this queue never issued | `Programming` | error log, debug-panic, node handed back | node handed back, per-kind counter, and now fatal in a release build too |
| Scheduling | A released node belongs to a transaction the queue no longer tracks | not a fault | warning, node handed back | unchanged |
| Submission | The orderer call fails in transport or times out | `Transient` | dropped and completed | retried once a guard exists against resubmitting a transaction that landed; until then dropped and counted |
| Submission | Two endorsers both endorsed, but their read-write sets differ, so the envelope cannot be packaged — typically one endorser is a block behind ([#421](https://github.com/hyperledger/fabric-x-evm/issues/421)) | `Transient` | the packaging error comes back from `Submit` and is dropped and completed as if the broadcast had failed | re-endorsed rather than resubmitted, since the same envelope fails identically; waiting for the lagging endorser needs its block number ([#463](https://github.com/hyperledger/fabric-x-evm/issues/463)) |
| Submission | The orderer refuses the envelope: the channel policy does not accept the gateway's signing identity, the envelope is malformed, or the transaction id is a duplicate | `Transient` | invisible to this gateway: the orderer answers asynchronously, the SDK's reply reader logs a non-SUCCESS reply at WARN and returns nothing, and the replies carry no transaction id and no ordering, so none of them can be attributed to an envelope | no sink can act on it until a refusal becomes observable at all ([#467](https://github.com/hyperledger/fabric-x-evm/issues/467)), so there is nothing to classify yet; until then it is one of the ways a transaction is lost after submission ([#423](https://github.com/hyperledger/fabric-x-evm/issues/423)) |
| After submission | The orderer accepted the transaction and it never appears in a block | no error exists | nothing happens: the node is never handed back, so its dependents wait for good ([#423](https://github.com/hyperledger/fabric-x-evm/issues/423)) | out of reach of this model; it needs a deadline, not a kind |
| After a commit | A conflicted transaction exhausts the MVCC retry budget | `Terminal` | error log naming both possible causes, counter, debug-panic, node released | unchanged, with the kind counted: one cause is an external writer and the other a contract that varies the keys it touches, which a client can deploy |
| After a commit | A conflicted transaction is untracked and its bytes do not decode | `Terminal` | debug-panic, node released | error log and alert, dropped, no longer fatal in a release build |
| After a commit | A conflicted transaction is re-admitted and the admitted channel is full | `Transient` | debug-panic | log, count and drop, since there is no caller left to answer |
| After a commit | A parked transaction released by a commit is refused by the queue | `Transient` or `Programming` | a duplicate is ignored, a closed queue warns, anything else logs and debug-panics | decide from the kind rather than the sentinel: drop a transient refusal, keep the panic for a programming one |

The retry locations follow from where the error is still recoverable. An endorser call is
retried by the sink that made it, `submitToManager` or `worker`, under the policy that
call sets for itself; that is also the retry that lets a lagging endorser catch up.
Admission has no retry by design: the client still holds the request, so re-attempting
from there costs this node nothing. Orderer submission is the one place where a retry is
not yet safe, which is why it stays a drop until #423 is settled.

Issue #423 also covers the one failure on this path that the model cannot describe at
all. A
transaction the orderer accepted and never committed produces no error anywhere: nothing
times it out, so its node is never handed back and its dependents are held for the life
of the process. There is no error value to classify and no sink to classify it at, which
is why the table records it with no kind. Fixing it means giving the submission a
deadline, and the deadline needs a guard against resubmitting a transaction that did in
fact land.

The orderer-refusal row has no kind a sink can act on yet, because a refusal never reaches
one: `Broadcast` returns when the envelope is written to the stream, the orderer answers
asynchronously with no transaction id and no ordering, and the SDK's reply reader logs the
non-SUCCESS status and returns nothing. Making a refusal observable at all is
[#467](https://github.com/hyperledger/fabric-x-evm/issues/467), which also has to decide
whether a reply can be attributed to an envelope; until then a refused transaction is
counted among the ones lost after submission.

The panic columns move in both directions, which is the whole point of separating the two
kinds. `Terminal` rows keep their `DebugPanic`, so they still fail CI and a development
run, but they stop being fatal in production: a misconfigured namespace, clocks that
disagree, or a version mismatch with a remote endorser fails every transaction
identically, and crash-looping the gateway through a rolling upgrade neither fixes that
nor helps the operator find it. `Programming` rows go the other way and become fatal in
every build, release included, because the condition says this code is wrong rather than
that the deployment is. See [Panicking, and the two kinds of
panic](#panicking-and-the-two-kinds-of-panic) for why each side is the way round it is,
and what it costs.

Every sink also counts its kinds and rejects `Unknown` once its sources are converted.
That counter is the only operator-facing signal the model adds, so it carries the whole
value of the `Terminal` rows: nothing else about them is new, since they were already
being dropped. It therefore has to be a named metric, labelled by sink and kind, defined
in the metrics package alongside the existing counters rather than left as an instruction
in this document.

Admission rows never reach a sink, so their change column is about the answer rather than
a retry. Shutdown is `Transient` because nothing is broken and the identical request
succeeds against a running node. The full admitted channel is `Transient` for the same
sort of reason: it is backpressure, and the error cannot tell a brief endorser blip from
an endorser that is gone. Classifying it `Terminal` would alert an operator over a blip,
and it would alert from the wrong component, because the sink that made the failing
endorser call sees the real fault and raises the operator signal there. The one cause
that would be a violated invariant — nothing draining the channel at all — cannot happen
once `Bind` has run, and `Bind` is required before the first `Enqueue`.

Most of the endorser's faults are invisible to the gateway today: a snapshot fault, an
executor that cannot be built, and a marshalling or signing fault all arrive as
`StatusServerError`, while an unrecognised status arrives as a status the gateway has
never seen. They land in the same branch, which is why the kind has to ride on the wire.

The failing state read is the one row whose change is more than a classification. The
read panics because geth's `vm.StateDB` interface has no error returns, so the
implementation has nowhere to put the failure
([`statedb.go:362`](../endorser/execution/statedb.go) and six more like it), and the only
`recover` in the repository is in the filters API, nowhere near this path. Returning a
transient fault therefore means recovering at the endorser's `Execute` boundary and
classifying there, not changing the read. That recover earns its keep twice over: it is
also what stops an in-process endorser from taking the gateway down with it.

The sender-recovery row relies on an invariant worth writing down where it is relied on.
It is `Programming` because validation and the nonce gate use the same signer, which
holds because `New` sets `g.Signer` once and `NewNonceGate` passes that same value
through. `core.New` does accept a caller-supplied `NonceSequencer`, and one built with a
different signer would break the invariant rather than reveal a bug in this code.

The unknown status is split by who reported it, because the invariant is not the same.
The in-process endorser is built from the same tree as the gateway, so a status the
gateway cannot name means the gateway's own switch is out of date, which is a fault in
this code. A remote endorser can run a different version, so the same status is a
deployment mismatch rather than a violated invariant, and panicking over it would
crash-loop the gateway through a rolling upgrade. The gateway already knows which
endorser answered, since the local one is the first entry in its list.

Several outcomes look like failures and are not. An EVM revert and an execution failure
are committed with a receipt at `status = 0`, so neither is a fault. An MVCC conflict is
retried and leaves no receipt, because the transaction will run again. A rejection the
ledger records for any other reason is committed with `status = 0` and is likewise an
outcome. A node the dependency manager releases for a transaction the queue no longer
tracks is not a fault: it is what a completed or already-committed transaction looks like
from the manager's side, so the node is handed straight back. A cancellation is not a
fault either, including one that cuts a rate-limited submission short, and every sink
checks for one before it consults the kind; that is what covers a transaction dropped
because the process is shutting down.

Two drops carry no error at all, and neither reaches a sink. A parked transaction whose
gap never fills is removed after three minutes, and resubmitting it before the gap fills
only parks it again. A parked transaction is also dropped when another arrives at the
same nonce and replaces it, which is the same thing Ethereum does, except that the
fee-bump rule deciding whether the replacement is allowed is still missing
([#62](https://github.com/hyperledger/fabric-x-evm/issues/62)); the displaced transaction
is forgotten with an informational log, and its sender is never told. Both are intended,
and both are invisible to a client beyond `eth_getTransactionByHash` turning from pending
to null.

## The commit-notification pipeline

TBD