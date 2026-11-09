# Common Rules — Microservicios Bancarios

> Reglas transversales aplicables a **todos** los microservicios.
> Cada skill específico referencia este archivo y solo agrega lo propio de su dominio.

## Stack obligatorio
- Java 17, Spring Boot 3.3.x, Maven, Spring Data Reactive MongoDB.
- **Reactividad por servicio**:
  - **Servicios CRUD** (`ms-customer-service`, `ms-account-service`, `ms-credit-service`, `ms-card-service`, `ms-wallet-service`, `ms-report-service`): **Spring WebFlux con Reactor** (`Mono`/`Flux`). Controladores y repositorios reactivos.
  - **Orquestador monetario** (`ms-transaction-service`): **RxJava 3** como exige el PDF para el flujo de orquestación, con `reactor-adapter` como puente documentado hacia WebFlux/Reactor cuando sea necesario.
- Lombok, Logback, inyección por constructor, nombres de código/URLs en inglés.
- Contratos OpenAPI (`docs/openapi.yaml`) como **fuente de verdad** de endpoints, DTOs, validaciones, headers y códigos HTTP.

## Infraestructura por fase
- **Parte I**: Config Server obligatorio. Configuración externalizada; sin secretos en código. Cada servicio declara `spring.config.import`.
- **Parte II**: Eureka Server con panel habilitado; todos los servicios se registran. Spring Cloud Gateway como punto de entrada único. Resilience4j con **timeout de 2 segundos** para llamadas entre servicios (distinto del timeout total de operación de 30 s en Transaction).
- **Parte III**: Kafka para eventos de dominio; JWT para autenticación/autorización; **Redis** para caché de catálogos y datos maestros (no para datos transaccionales).

## Persistencia
- **Database per Service**: cada microservicio tiene su propia base/colecciones.
- Prohibido leer o escribir colecciones de otro servicio. Las operaciones cruzadas se coordinan por API o por eventos.
- Documentos MongoDB separados de DTOs de contrato.
- `BigDecimal` para dinero, moneda explícita, nunca `double`.
- Fechas en UTC en persistencia, `LocalDate`/`LocalDateTime` según `dateLibrary=java8` del generador.
- Índices únicos (documento, teléfono, email según corresponda) y control de concurrencia.
- Spring Data MongoDB con métodos derivados; evitar `@Query` y SQL dinámico.

## Reactividad
- **CRUD (WebFlux + Reactor)**: controladores que devuelven `Mono`/`Flux`; nunca `block()` en el camino reactivo; errores con `onErrorResume`, `switchIfEmpty` y excepciones mapeadas a `ErrorResponse`.
- **Transaction (RxJava 3)**: pipelines con `Single`/`Maybe`/`Flowable`/`Completable`; usar `RxJava3Adapter` para interoperar con `Mono`/`Flux` cuando se llame a WebFlux; nunca `blockingGet()` en el camino reactivo.
- No mezclar ambas bibliotecas sin un adaptador explícito y documentado.

## Idempotencia
- Header `X-Transaction-ID` (UUID) en operaciones que el YAML lo exige.
- Repetir el mismo `X-Transaction-ID` no debe producir doble efecto financiero ni doble registro.
- El header de idempotencia **no implica** invocar `ms-transaction-service`.

## Headers obligatorios
- `X-Correlation-ID`: propagar en logs y en llamadas salientes.
- `X-Channel-ID`: identificar canal.
- `X-Transaction-ID`: solo donde el YAML lo exige.
- `Authorization: Bearer <JWT>` según contrato.

## Seguridad
- Spring Security + OAuth2 Resource Server (JWT) en Parte III.
- Mientras se desarrolla sin proveedor de identidad, `SecurityWebFilterChain` temporal con `permitAll()` **solo en perfil `dev`**.
- Nunca loggear DNI, teléfono, email completo, JWT, OTP, PAN ni credenciales.

## Observabilidad
- Actuator con `health,info,metrics,prometheus`.
- Micrometer + Prometheus.
- Logback con `correlationId` en MDC.
- SpringDoc OpenAPI para Swagger UI.
- Diagramas UML en draw.io: arquitectura general y secuencia por microservicio.

## Manejo de errores
- Estructura común `ErrorResponse` (`code`, `message`, `timestamp`, `correlationId`, `details`).
- Excepciones de dominio propias (`CustomerNotFoundException`, `InsufficientFundsException`, etc.) mapeadas a códigos HTTP del contrato.
- `@RestControllerAdvice` global por servicio.

## Pruebas
- JUnit 5 + Mockito.
- **CRUD**: `reactor-test` (`StepVerifier`) para `Mono`/`Flux`.
- **Transaction**: `TestObserver`/`TestSubscriber` de RxJava 3.
- Pruebas unitarias de reglas de negocio.
- Pruebas de integración con MongoDB embebido (Flapdoodle).
- JaCoCo obligatorio desde Parte II. No inventar porcentaje mínimo: usar el que decida el equipo.
- Casos negativos: duplicados, saldo insuficiente, timeout, error de servicio externo, reintentos.

## Calidad
- Checkstyle con `google_checks.xml` (Parte II).
- JaCoCo con reporte en `target/site/jacoco/`.
- Prohibido código generado editado a mano (`target/generated-sources/`).

## Convenciones
- Paquetes: `com.banking.<domain>.{config,controller,document,dto,event,exception,mapper,repository,service,service.impl}`.
- Nombres de clase: `XxxServiceApplication`, `XxxApiImpl`, `XxxDocument`, `XxxService`, `XxxServiceImpl`, `XxxMapper`, `XxxRepository`.
- DTOs generados: `com.banking.<domain>.dto`.
- API generada: `com.banking.<domain>.api`.