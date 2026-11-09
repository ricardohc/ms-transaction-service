# Skill: ms-transaction-service — orquestación monetaria

> Contrato: `ms-transaction-service-openapi.yaml` (fuente de verdad).
> Reglas transversales: ver `common-rules.md`.
> **Reactividad**: **RxJava 3** (exigido por el PDF para el orquestador), con `reactor-adapter` como puente documentado hacia WebFlux cuando se invoque a otros servicios.
> **Nota**: `ms-transaction-service` **es el único** que no escribe directo a la base de otros servicios. Los CRUD de Account, Credit, Card y Wallet sí escriben directo a su propia MongoDB. Transaction coordina operaciones monetarias y escribe solo en su propia base (`banking_transaction`).

## Objetivo
Implementar `ms-transaction-service` como **orquestador monetario**. **No es el microservicio que ejecuta todos los CRUD**: coordina operaciones que afectan dinero y sus estados.

## Stack y restricciones
- Java 17, Spring Boot 3.3.x, Maven, Spring WebFlux (infraestructura), **RxJava 3** para los pipelines de orquestación, Spring Data Reactive MongoDB, Kafka, Lombok, Logback.
- `reactor-adapter` (`RxJava3Adapter`) para interoperar `Single`/`Maybe`/`Flowable` con `Mono`/`Flux` cuando se llame a otros servicios WebFlux.
- Resilience4j con **timeout de 2 s** para llamadas entre servicios (Parte II).
- Distinguir: timeout de llamada/CB (2 s) vs plazo total de la operación (30 s) del OpenAPI.
- JWT en Parte III.
- **Nunca** leer/escribir MongoDB de Account, Credit, Card ni Wallet. Cada participante aplica cambios en su propia base.
- **Prohibido** `blockingGet()`/`block()` en el camino reactivo.

## API exacta
- `POST /api/v1/transactions`: crea operación idempotente por `X-Transaction-ID`, devuelve `202` con referencia de seguimiento.
- `GET /api/v1/transactions/{transactionId}`: consulta estado, `200`.
- Headers: `X-Correlation-ID`, `X-Channel-ID`, y en POST `X-Transaction-ID` (UUID). Seguridad Bearer JWT.
- Tipos: `ACCOUNT_TRANSFER`, `CREDIT_PAYMENT`, `CARD_CONSUMPTION`, `WALLET_TRANSFER`.
- Estados: `PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`, `COMPENSATED`.
- Para `WALLET_TRANSFER`, el origen **nunca** se acepta desde el body: se deriva de JWT, se verifica teléfono y titularidad.

## Flujo de referencia (RxJava)
1. Validar JWT, headers, esquema y reglas.
2. Buscar `X-Transaction-ID`: si ya existe, devolver transacción original (202) sin duplicar efecto.
3. Registrar transacción `PENDING` + mensaje en outbox (atómicamente, en `banking_transaction`).
4. Responder 202; publicar asíncronamente.
5. Orquestar con comandos/respuestas a participantes (contratos abajo) usando `Single`/`Flowable` y adaptadores a `Mono`/`Flux` cuando se invoque a servicios WebFlux.
6. Actualizar `PROCESSING` → `COMPLETED` o `FAILED`.
7. Si hubo efectos parciales, ejecutar compensaciones idempotentes → `COMPENSATED`.
8. Prevenir doble débito/crédito con idempotencia por operación y por paso.

## Eventos de estado (catálogo)
- Tópico: `banking.transaction.events.v1`.
- Productor: `ms-transaction-service`.
- Key: `transactionId`.
- Eventos: `TransactionAccepted`, `TransactionProcessing`, `TransactionCompleted`, `TransactionFailed`.
- `COMPENSATED` es un **estado** de la API. `TransactionCompensated` **no** está en el catálogo: no publicarlo sin cambiar formalmente el contrato.

## Envelope obligatorio
Requeridos: `eventId` (UUID), `eventType`, `eventVersion` (=1), `aggregateId`, `aggregateType`, `occurredAt`, `correlationId`, `transactionId` (nullable), `producer`, `payload`.
Opcionales: `causationId`, `traceparent`, `channelId`.
- `aggregateType=Transaction`, `producer=ms-transaction-service`.
- `eventId` = publicación; `transactionId` = operación idempotente.
- **Nunca** incluir PAN, JWT, OTP, contraseñas o documentos completos.

## Fiabilidad
- `outbox=true`, `at-least-once`, deduplicación por `eventId`, orden por `aggregateId`.
- Outbox + estado de transacción persistidos atómicamente en MongoDB (requiere replica set).
- Publicación asíncrona con reintentos y marcado de publicación.
- Consumidores idempotentes; correlación y trazas propagadas.
- No prometer `exactly-once` end-to-end.

## Contratos de comandos y respuestas (ESTADO: BORRADOR — NO IMPLEMENTAR HASTA APROBACIÓN)
> Estos contratos **no han sido aprobados formalmente**. Antes de implementar:
> 1. Confirmar con el responsable del proyecto que la propuesta es aceptada.
> 2. Verificar que los tópicos, keys y esquemas coinciden con el catálogo Kafka oficial.
> 3. Si el catálogo no incluye estos tópicos, **no crearlos**: usar la vía que el catálogo permita o escalar la decisión.
> 4. Documentar la aprobación (fecha, responsable) en el repositorio antes de codificar.

### Tópicos (PROPUESTA — sujeto a validación contra catálogo)
| Tópico | Dirección | Key | Productor | Consumidor |
|---|---|---|---|---|
| `banking.account.commands.v1` | comando | `accountId` | `ms-transaction-service` | `ms-account-service` |
| `banking.account.replies.v1` | respuesta | `transactionId` | `ms-account-service` | `ms-transaction-service` |
| `banking.credit.commands.v1` | comando | `creditId` | `ms-transaction-service` | `ms-credit-service` |
| `banking.credit.replies.v1` | respuesta | `transactionId` | `ms-credit-service` | `ms-transaction-service` |
| `banking.card.commands.v1` | comando | `cardId` | `ms-transaction-service` | `ms-card-service` |
| `banking.card.replies.v1` | respuesta | `transactionId` | `ms-card-service` | `ms-transaction-service` |
| `banking.wallet.commands.v1` | comando | `walletId` | `ms-transaction-service` | `ms-wallet-service` |
| `banking.wallet.replies.v1` | respuesta | `transactionId` | `ms-wallet-service` | `ms-transaction-service` |

### Comandos (PROPUESTA)
- **Account**: `ReserveFunds`, `CommitDebit`, `ReleaseFunds` (compensación), `CreditFunds`.
- **Credit**: `ApplyCreditPayment`, `RevertCreditPayment` (compensación).
- **Card**: `ReserveCreditLine`, `CommitConsumption`, `ReleaseCreditLine` (compensación), `DebitLinkedAccount`.
- **Wallet**: `DebitWallet`, `CreditWallet`, `RevertWalletOperation` (compensación).

Cada comando lleva: `commandId`, `transactionId`, y los campos específicos del dominio.

### Respuestas (PROPUESTA)
- `CommandAccepted` (commandId, transactionId, status=ACCEPTED)
- `CommandCompleted` (commandId, transactionId, status=COMPLETED, resultPayload)
- `CommandFailed` (commandId, transactionId, status=FAILED, failureCode, failureReason)
- `CompensationCompleted` (commandId, transactionId, status=COMPENSATED)

### Envelope de comandos/respuestas
- Reutiliza el envelope del catálogo.
- `aggregateType` según dominio (`Account`, `Credit`, `Card`, `Wallet`).
- `producer` = `ms-transaction-service` para comandos; el servicio dueño para respuestas.
- `transactionId` obligatorio.
- `payload` con el cuerpo específico del comando/respuesta.

### Reglas (PROPUESTA)
- Cada comando lleva `commandId` (UUID) para deduplicación.
- Las respuestas llevan `transactionId` y `commandId` para correlación.
- Los participantes son idempotentes por `commandId`.
- Reintentos acotados con DLQ.
- Los comandos son la **única** vía por la que Transaction modifica datos en otros servicios. Nunca escritura directa a sus bases.

## Criterios de aceptación
1. Respuestas y validaciones idénticas al YAML.
2. `X-Transaction-ID` repetido no genera segundo cargo.
3. Orquestación no accede a MongoDB de otros servicios.
4. Eventos con envelope válido y sin datos sensibles.
5. Casos probados: éxito, fondos insuficientes, timeout, duplicado, error parcial y compensación.
6. **Los contratos de comandos y respuestas solo se implementan si hay aprobación formal registrada y coinciden con el catálogo Kafka oficial.**
7. Pipelines de orquestación implementados con RxJava 3 (`Single`/`Flowable`), sin `blockingGet()`, con adaptadores a `Mono`/`Flux` cuando se invoque a otros servicios.