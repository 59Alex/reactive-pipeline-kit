# reactive-pipeline-kit

[![Build](https://github.com/59Alex/reactive-pipeline-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/59Alex/reactive-pipeline-kit/actions/workflows/ci.yml) [![Java 17+](https://img.shields.io/badge/Java-17%2B-437291)](docs/JAVA-SUPPORT.md) [![MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

<details name="readme-language">
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-en-dark.svg"><img src="assets/lang-en-light.svg" width="42" height="26" alt="EN"></picture></summary>

**Version:** [0.3.0 guide](docs/0.3.0/en.md) · [All versions](docs/index.md)<br>
**Java:** [17](docs/0.3.0/java.en.md#java-17) · [21](docs/0.3.0/java.en.md#java-21) · [25](docs/0.3.0/java.en.md#java-25) · [27](docs/0.3.0/java.en.md#java-27)

Typed Reactor pipelines for record enrichment and transformation. Each step has
its own timeout and retry policy: retrying enrichment does not repeat earlier
validation. No Spring container, scripts, or runtime configuration interpreter.

## Capabilities

| Capability | Example and boundaries |
| --- | --- |
| Typed steps and conditions | [Compose a pipeline](docs/0.3.0/en.md#2-compose-typed-and-conditional-steps) |
| Per-attempt timeout and filtered retries | [Retry only the failing step](docs/0.3.0/en.md#3-retry-only-the-failing-step) |
| Bounded record concurrency, input/completion ordering | [Process a stream](docs/0.3.0/en.md#4-limit-concurrency-and-choose-output-order) |
| HTTP cancellation and explicit blocking executors | [HTTP](docs/0.3.0/en.md#5-connect-http-and-propagate-cancellation), [blocking I/O](docs/0.3.0/en.md#6-isolate-blocking-io-use-virtual-threads-when-available) |
| Optional Jolt transformations, ScopedValue and JFR adapters | [Jolt](docs/0.3.0/en.md#8-transform-json-trees-with-jolt), [scoped context](docs/0.3.0/en.md#7-optional-java-25-scoped-context), [diagnostics](docs/0.3.0/en.md#9-observe-executions-and-record-jfr-on-java-27) |

## Try it

From this checkout with JDK 17+:

```sh
./gradlew build publishToMavenLocal
./gradlew -p docs/0.3.0/examples run
```

The independent example consumer resolves locally built artifacts and runs eight
asserted scenarios, including a loopback HTTP exchange. No external service is
required. [Example sources, optional modules and expected output](docs/0.3.0/examples/README.md#english).
These commands publish only to Maven Local; no Maven Central release is claimed.

```java
// Inside a main method; import io.github.alex59.pipeline.* and reactor.core.publisher.Flux.
var pipeline = Pipeline.<String>start()
    .map("trim", String::trim)
    .map("length", String::length);
var lengths = pipeline.process(Flux.just(" java ", " ts "), 2, Ordering.INPUT)
    .collectList().block();
System.out.println(lengths); // [4, 2]
```

Definitions are immutable and executions are cold. A step must emit exactly one
non-null value. Keep transformations nonblocking; use the explicit executor
adapter for blocking calls. The final block above is a synchronous demo boundary.
[Installation and complete examples](docs/0.3.0/en.md#1-install-the-modules-you-need).

## Compatibility and limits

Core and Jolt target Java 17 and support runtimes 17/21/25/27. Java 21+ virtual
threads work through an application-owned executor. Optional ScopedValue requires
25+, and the JFR profile requires 27; neither is pulled into the core. No preview
flags. [Runtime details](docs/0.3.0/java.en.md).

Concurrency bounds subscribed records, not total memory or I/O that ignores
cancellation. Retry may repeat external effects; use application idempotency.
There is no durable execution log, restart recovery, or distributed transaction.
[Architecture and failure behavior](docs/ARCHITECTURE.md).

[MIT](LICENSE) · [Dependency notices](THIRD-PARTY-NOTICES.md)

</details>

<details name="readme-language" open>
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-ru-dark.svg"><img src="assets/lang-ru-light.svg" width="42" height="26" alt="RU"></picture></summary>

**Версия:** [Руководство 0.3.0](docs/0.3.0/ru.md) · [Все версии](docs/index.md)<br>
**Java:** [17](docs/0.3.0/java.ru.md#java-17) · [21](docs/0.3.0/java.ru.md#java-21) · [25](docs/0.3.0/java.ru.md#java-25) · [27](docs/0.3.0/java.ru.md#java-27)

Типизированные пайплайны Reactor для обогащения и преобразования записей. У каждого
шага свои таймаут и политика повторов: повтор обогащения не запускает уже
пройденную валидацию. Без Spring-контейнера, скриптов и интерпретатора конфигурации.

## Возможности

| Возможность | Пример и границы |
| --- | --- |
| Типизированные шаги и условия | [Сборка пайплайна](docs/0.3.0/ru.md#2-соберите-типизированные-и-условные-шаги) |
| Таймаут попытки и повторы выбранных ошибок | [Повтор только упавшего шага](docs/0.3.0/ru.md#3-повторяйте-только-упавший-шаг) |
| Ограничение параллельности, порядок входа или завершения | [Обработка потока](docs/0.3.0/ru.md#4-ограничьте-параллельность-и-выберите-порядок) |
| Отмена HTTP и явный executor для блокирующих вызовов | [HTTP](docs/0.3.0/ru.md#5-подключите-http-и-передавайте-отмену), [блокирующий I/O](docs/0.3.0/ru.md#6-изолируйте-блокирующий-ввод-вывод-и-используйте-виртуальные-потоки) |
| Необязательные адаптеры Jolt, ScopedValue и JFR | [Jolt](docs/0.3.0/ru.md#8-преобразуйте-json-деревья-через-jolt), [контекст](docs/0.3.0/ru.md#7-необязательный-контекст-scopedvalue-на-java-25), [диагностика](docs/0.3.0/ru.md#9-наблюдайте-выполнение-и-записывайте-jfr-на-java-27) |

## Попробовать

В корне этого репозитория с JDK 17+:

```sh
./gradlew build publishToMavenLocal
./gradlew -p docs/0.3.0/examples run
```

Отдельный consumer примеров использует локально собранные артефакты и проверяет
восемь сценариев, включая запрос к локальному HTTP-серверу. Внешние сервисы не
нужны. [Исходники, необязательные модули и ожидаемый результат](docs/0.3.0/examples/README.md#русский).
Команды публикуют артефакты только в Maven Local; выпуск в Maven Central не заявлен.

```java
// Внутри main; импортируйте io.github.alex59.pipeline.* и reactor.core.publisher.Flux.
var pipeline = Pipeline.<String>start()
    .map("trim", String::trim)
    .map("length", String::length);
var lengths = pipeline.process(Flux.just(" java ", " ts "), 2, Ordering.INPUT)
    .collectList().block();
System.out.println(lengths); // [4, 2]
```

Определения неизменяемы, выполнение начинается при подписке. Шаг должен вернуть
ровно одно ненулевое значение. Для блокирующих вызовов используйте адаптер с явным
executor. Вызов block выше находится на синхронной границе демонстрационной программы.
[Подключение и полные примеры](docs/0.3.0/ru.md#1-подключите-нужные-модули).

## Совместимость и ограничения

Ядро и Jolt собраны для Java 17, поддерживают JVM 17/21/25/27. Виртуальные потоки
Java 21+ доступны через executor приложения. Необязательному ScopedValue нужна
Java 25+, профилю JFR — 27; ядро от них не зависит. Preview-флаги не нужны.
[Требования к JVM](docs/0.3.0/java.ru.md).

Лимит параллельности ограничивает подписанные записи, а не весь объём памяти или
I/O, игнорирующий отмену. Повторы могут повторить внешние эффекты: идемпотентность
обеспечивает приложение. Нет постоянного журнала выполнения, восстановления после
перезапуска и распределённых транзакций. [Архитектура и отказы](docs/ARCHITECTURE.md).

[MIT](LICENSE) · [Лицензии зависимостей](THIRD-PARTY-NOTICES.md)

</details>
