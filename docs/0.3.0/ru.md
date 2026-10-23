# reactive-pipeline-kit 0.3.0 — руководство

[← Back to documentation and versions / Назад к документации и версиям](../index.md)

[English](en.md) · [Русский](ru.md)

[Версии](../index.md) · [README](../../README.md) · [Возможности Java](java.ru.md)

Типизированные пайплайны Reactor с повторами в пределах шага, ограниченной параллельностью записей и необязательными адаптерами JSON и диагностики JVM.

## Сводка возможностей

| Возможность | Применение | Пример |
| --- | --- | --- |
| 1. Подключите нужные модули | Соберите локальную версию 0.3.0 из исходников. Core и Jolt работают на Java 17; возможности новых Java вынесены в необязательные модули. | [Код и результат](#1-подключите-нужные-модули) |
| 2. Соберите типизированные и условные шаги | Pipeline<I,O> фиксирует тип входа и выхода в Java. map может изменить тип результата; thenIf запускает шаг того же типа только при выполнении условия. | [Код и результат](#2-соберите-типизированные-и-условные-шаги) |
| 3. Повторяйте только упавший шаг | Политика применяется к одному шагу: повтор обогащения не запускает заново предыдущую валидацию или подготовку. Число повторов не включает первую попытку. | [Код и результат](#3-повторяйте-только-упавший-шаг) |
| 4. Ограничьте параллельность и выберите порядок | process одновременно подписывается не более чем на заданное число записей. Порядок входа или порядок завершения выбирается явно. | [Код и результат](#4-ограничьте-параллельность-и-выберите-порядок) |
| 5. Подключите HTTP и передавайте отмену | HttpSteps.jdk адаптирует HttpClient, которым владеет приложение. Пример запускает локальный сервер и не требует внешнего сервиса. | [Код и результат](#5-подключите-http-и-передавайте-отмену) |
| 6. Изолируйте блокирующий ввод-вывод и используйте виртуальные потоки | BlockingSteps.call создаёт отдельную задачу на каждую попытку и явно передаёт ExecutionContext. Executor принадлежит приложению. | [Код и результат](#6-изолируйте-блокирующий-ввод-вывод-и-используйте-виртуальные-потоки) |
| 7. Необязательный контекст ScopedValue на Java 25 | Используйте java25, когда вложенным синхронным вызовам нужен контекст без передачи параметра через каждый метод. Запуск задачи и отмена остаются в core. | [Код и результат](#7-необязательный-контекст-scopedvalue-на-java-25) |
| 8. Преобразуйте JSON-деревья через Jolt | Необязательный адаптер Jolt копирует спецификацию и входное дерево, затем создаёт отдельный экземпляр преобразования на каждую попытку. | [Код и результат](#8-преобразуйте-json-деревья-через-jolt) |
| 9. Наблюдайте выполнение и записывайте JFR на Java 27 | Observer получает события жизненного цикла с общим execution ID для шагов и повторов одного выполнения. Reactor context переносит метаданные приложения. | [Код и результат](#9-наблюдайте-выполнение-и-записывайте-jfr-на-java-27) |
| 10. Разбирайте ошибки и исправляйте конфигурацию | StepFailure называет упавший шаг, показывает число попыток и сохраняет исходную причину. Пустой результат — ошибка, а не незаметная потеря записи. | [Код и результат](#10-разбирайте-ошибки-и-исправляйте-конфигурацию) |

## 1. Подключите нужные модули

Соберите локальную версию 0.3.0 из исходников. Core и Jolt работают на Java 17; возможности новых Java вынесены в необязательные модули.

```bash
./gradlew build publishToMavenLocal
```

Выполните в корне reactive-pipeline-kit с JDK 17+. Артефакты попадут в локальный Maven-репозиторий.

Готовый отдельный consumer можно запустить сразу из корня библиотеки:

```bash
./gradlew -p docs/0.3.0/examples run
```

[Запуск, исходники, необязательные модули и результаты](examples/README.md#русский).
Consumer получает локальные JAR, не компилируя исходники соседних модулей.
Для создания собственного consumer используйте конфигурацию ниже.

```kotlin
plugins { application }
repositories {
    mavenLocal()
    mavenCentral()
}
dependencies {
    implementation("io.github.59alex:reactive-pipeline-kit:0.3.0")
}
application { mainClass.set("Example") }
```

build.gradle.kts отдельного приложения Gradle с wrapper. Поместите только Example.java в src/main/java. Координаты относятся к локальной публикации и не означают наличие релиза в Maven Central.

```bash
./gradlew run
```

Выполните в проекте приложения, а не в исходниках библиотеки. Example.java проверит восемь сценариев и выведет результаты. Для другого скачанного примера измените mainClass и добавьте его модуль; копируйте только выбранный исходник и используйте требуемый JDK.

- core: Java 17+, Reactor 3.8.8; пайплайны, HTTP и явный адаптер блокирующих операций.
- jolt: Java 17+, преобразование JSON-деревьев; артефакт reactive-pipeline-kit-jolt.
- java25: Java 25+, ScopedValue; включается через -Pjava25. Для самих виртуальных потоков модуль не нужен.
- java27: Java 27+, JFR-диагностика; включается через -Pjava27. Минимум для core остаётся Java 17.

> **Контекст примеров**
>
> Java-блоки ниже — тела методов, если явно не показан класс или команда. В Example.java есть импорты, main и вспомогательный check; HTTP-пример использует только локальный сервер. JoltExample.java, ScopedExample.java и JfrExample.java скачиваются отдельно. Вызовы block здесь относятся к синхронному запуску демонстрации, а не к event loop Reactor.

## 2. Соберите типизированные и условные шаги

Pipeline<I,O> фиксирует тип входа и выхода в Java. map может изменить тип результата; thenIf запускает шаг того же типа только при выполнении условия.

```java
Pipeline<String, Integer> sizes = Pipeline.<String>start()
    .map("trim", String::trim)
    .thenIf("remove-prefix", value -> value.startsWith("id:"),
        (value, context) -> Mono.just(value.substring(3)), StepPolicy.defaults())
    .map("length", String::length);
Integer result = sizes.run(" id:java ").block();
check(Objects.equals(4, result), "typed result");
System.out.println("typed=" + result);
```

Example.java: typed(). Ожидаемый вывод: typed=4.

> **Неизменяемое описание и отложенный запуск**
>
> Добавление шага возвращает новое описание. Каждая подписка на run запускает отдельное выполнение; результат не кэшируется. Шаг должен вернуть одно ненулевое значение. Ложное условие пропускает исходное значение дальше, а предикат вычисляется один раз на запись.

## 3. Повторяйте только упавший шаг

Политика применяется к одному шагу: повтор обогащения не запускает заново предыдущую валидацию или подготовку. Число повторов не включает первую попытку.

```java
AtomicInteger previousStep = new AtomicInteger();
AtomicInteger attempts = new AtomicInteger();
StepPolicy policy = new StepPolicy(Duration.ofSeconds(2), 2,
    Duration.ofMillis(10), IOException.class::isInstance);
var pipeline = Pipeline.<Integer>start()
    .map("prepare", value -> { previousStep.incrementAndGet(); return value + 1; })
    .then("enrich", (value, context) -> attempts.incrementAndGet() < 3
        ? Mono.error(new IOException("temporary")) : Mono.just(value * 2), policy);
Integer result = pipeline.run(4).block();
check(Objects.equals(10, result) && previousStep.get() == 1 && attempts.get() == 3,
    "retry boundary");
System.out.println("retry=" + result + "; previous=" + previousStep + "; attempts=" + attempts);
```

Example.java: retry(). Ожидается: retry=10; previous=1; attempts=3.

```java
var pipeline = Pipeline.<Integer>start().<Integer>then("slow",
    (value, context) -> Mono.never(), StepPolicy.once(Duration.ofMillis(50)));
try {
    pipeline.run(1).block();
    throw new AssertionError("timeout expected");
} catch (StepFailure failure) {
    check(failure.getCause() instanceof TimeoutException, "timeout cause");
    System.out.println("timeout=" + failure.step() + "; attempts=" + failure.attempts());
}
```

Example.java: timeout(). Ожидается: timeout=slow; attempts=1. Исходная причина — TimeoutException.

> **Граница надёжности**
>
> Таймаут действует на каждую попытку; задержка между повторами добавляется отдельно. По умолчанию: 30 секунд и без повторов. Повтор использует ту же ссылку на вход и может повторить удалённый эффект после неоднозначной ошибки. Сохраняйте вход неизменяемым и применяйте идемпотентность для побочных эффектов. Таймаут запрашивает отмену, но не может принудительно остановить блокирующий Java-код.

## 4. Ограничьте параллельность и выберите порядок

process одновременно подписывается не более чем на заданное число записей. Порядок входа или порядок завершения выбирается явно.

```java
var delayed = Pipeline.<Integer>start().then("delay",
    (value, context) -> Mono.delay(Duration.ofMillis((4 - value) * 20L)).thenReturn(value));
List<Integer> ordered = delayed.process(Flux.just(1, 2, 3), 3, Ordering.INPUT)
    .collectList().block();
List<Integer> completed = delayed.process(Flux.just(1, 2, 3), 3, Ordering.COMPLETION)
    .collectList().block();
check(List.of(1, 2, 3).equals(ordered), "input order");
check(completed != null && Set.copyOf(completed).equals(Set.of(1, 2, 3)), "all results");
System.out.println("ordered=" + ordered + "; completionContainsAll=true");
```

Example.java: ordering(). INPUT выдаёт [1, 2, 3]. COMPLETION содержит те же значения, но их порядок зависит от времени завершения.

- INPUT может задерживать готовые записи за медленной предыдущей; порядок внешних побочных эффектов он не задаёт.
- Первая ошибка завершает поток и отменяет остальные активные записи. Уже выданные результаты не откатываются.
- Лимит измеряется записями, а не байтами или потоками. Крупные данные и операции, игнорирующие отмену, всё ещё могут занимать ресурсы.

## 5. Подключите HTTP и передавайте отмену

HttpSteps.jdk адаптирует HttpClient, которым владеет приложение. Пример запускает локальный сервер и не требует внешнего сервиса.

```java
HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
server.createContext("/value", exchange -> {
    byte[] body = "try later".getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(503, body.length);
    try (var output = exchange.getResponseBody()) { output.write(body); }
});
server.start();
try {
    var client = HttpSteps.jdk(HttpClient.newHttpClient(), HttpResponse.BodyHandlers.ofString());
    var fetch = Pipeline.<HttpRequest>start()
        .then("fetch", client.asStep(), StepPolicy.once(Duration.ofSeconds(2)));
    URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/value");
    var response = fetch.run(HttpRequest.newBuilder(endpoint).GET().build()).block();
    check(response != null && response.statusCode() == 503, "HTTP status remains data");
    System.out.println("http=" + response.statusCode() + "; body=" + response.body());
} finally {
    server.stop(0);
}
```

Example.java: http(). Ожидается: http=503; body=try later. Ответ 503 — данные, а не автоматическая ошибка Reactor.

> **Статус, таймаут и размер тела**
>
> Чтобы повторять запросы при выбранных HTTP-статусах, преобразуйте их в ошибки внутри HTTP-шага. Ошибка последующего преобразования не повторяет запрос. Таймаут или отмена подписки доходят до HTTP future, но сервер уже мог обработать запрос. BodyHandlers.ofString загружает тело целиком; этот пример не обещает обработку потока байтов.

## 6. Изолируйте блокирующий ввод-вывод и используйте виртуальные потоки

BlockingSteps.call создаёт отдельную задачу на каждую попытку и явно передаёт ExecutionContext. Executor принадлежит приложению.

```java
var executor = Executors.newFixedThreadPool(2);
try {
    var lookup = Pipeline.<String>start().then("blocking", BlockingSteps.call(executor,
        (value, context) -> {
            Thread.sleep(5);
            return context.subscriberContext().<String>get("tenant") + ":" + value;
        }));
    String result = lookup.run("java").contextWrite(Context.of("tenant", "team")).block();
    check("team:java".equals(result), "explicit context");
    System.out.println("blocking=" + result);
} finally {
    executor.shutdownNow();
    check(executor.awaitTermination(5, TimeUnit.SECONDS), "executor shutdown");
}
```

Example.java: blocking(), Java 17+. Ожидается: blocking=team:java. Executor останавливается после завершения подписанного выполнения.

```java
var executor = Executors.newVirtualThreadPerTaskExecutor();
```

Java 21/25/27: замените только создание executor в предыдущем примере. API core остаётся тем же; дополнительный модуль не требуется.

> **Кооперативная отмена**
>
> Отмена запрашивает прерывание и учитывает гонку во время передачи задачи executor. Код, игнорирующий прерывание, может продолжиться после отмены и пересечься с повтором. Виртуальные потоки подходят для блокирующего ввода-вывода, но не ускоряют CPU-работу и не заменяют лимит process. Не передавайте executor, выполняющий блокирующий код прямо в потоке подписчика.

## 7. Необязательный контекст ScopedValue на Java 25

Используйте java25, когда вложенным синхронным вызовам нужен контекст без передачи параметра через каждый метод. Запуск задачи и отмена остаются в core.

```bash
./gradlew -Pjava25 -PtestJavaVersion=25 build publishToMavenLocal
```

Выполните в исходниках библиотеки с JDK 25. Адаптер также поддерживает Java 27.

```kotlin
implementation("io.github.59alex:reactive-pipeline-kit-java25:0.3.0")
```

Добавьте внутрь dependencies приложения; не подключайте этот артефакт на Java 17/21.

```java
import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.java25.ScopedBlockingSteps;
import java.util.concurrent.Executors;
import reactor.util.context.Context;

public final class ScopedExample {
    public static void main(String[] args) {
        if (ScopedBlockingSteps.currentContext().isPresent()) throw new AssertionError("unexpected binding");
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var lookup = Pipeline.<String>start().then("lookup", ScopedBlockingSteps.call(executor, value -> {
                if (!Thread.currentThread().isVirtual()) throw new AssertionError("virtual thread expected");
                var context = ScopedBlockingSteps.currentContext().orElseThrow();
                return context.subscriberContext().<String>get("tenant") + ":" + value;
            }));
            String result = lookup.run("java").contextWrite(Context.of("tenant", "team")).block();
            if (!"team:java".equals(result)) throw new AssertionError(result);
            if (ScopedBlockingSteps.currentContext().isPresent()) throw new AssertionError("binding escaped");
            System.out.println("scoped=" + result + "; outsideBound=false");
        }
    }
}

```

Полный ScopedExample.java, Java 25+. Ожидается: scoped=team:java; outsideBound=false.

> **Только синхронная область**
>
> Привязка заканчивается при возврате или исключении операции, до уведомления следующего шага. Она не переносится автоматически между scheduler Reactor или независимыми задачами executor. Через асинхронные границы передавайте Reactor context; в core нет неявного метода currentContext.

## 8. Преобразуйте JSON-деревья через Jolt

Необязательный адаптер Jolt копирует спецификацию и входное дерево, затем создаёт отдельный экземпляр преобразования на каждую попытку.

```kotlin
implementation("io.github.59alex:reactive-pipeline-kit-jolt:0.3.0")
```

Добавьте внутрь dependencies. Модуль входит в обычную локальную сборку и требует Java 17+.

```java
import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.jolt.JoltStep;
import java.util.List;
import java.util.Map;

public final class JoltExample {
    public static void main(String[] args) {
        var reshape = JoltStep.fromSpec(List.of(
            Map.of("operation", "shift", "spec", Map.of("id", "record.key"))));
        var pipeline = Pipeline.<Object>start().then("reshape", reshape);
        Map<String, Object> input = Map.of("id", 7);
        Object result = pipeline.run(input).block();
        if (!Map.of("record", Map.of("key", 7)).equals(result)) throw new AssertionError(result);
        if (!Map.of("id", 7).equals(input)) throw new AssertionError("input changed");
        System.out.println("jolt=" + result + "; inputUnchanged=true");
    }
}

```

Полный JoltExample.java. Ожидается: jolt={record={key=7}}; inputUnchanged=true.

> **Преобразование дерева, а не потоковый JSON-парсер**
>
> Поддерживаются shift, default, remove, cardinality и sort. Деревья целиком находятся в памяти; произвольные Java-классы и скрипты не загружаются. Нечисловые и бесконечные Float/Double, пользовательские реализации Number и глубина больше 128 отклоняются. CPU-работа идёт в потоке подписки, если приложение явно не выбрало scheduler.

## 9. Наблюдайте выполнение и записывайте JFR на Java 27

Observer получает события жизненного цикла с общим execution ID для шагов и повторов одного выполнения. Reactor context переносит метаданные приложения.

```java
var events = new ConcurrentLinkedQueue<StepEvent>();
var lookup = Pipeline.<String>start()
    .then("tenant", (value, context) -> Mono.just(
        context.subscriberContext().<String>get("tenant") + ":" + value))
    .observe(events::add);
String result = lookup.run("java").contextWrite(Context.of("tenant", "team")).block();
check("team:java".equals(result) && events.size() == 2, "diagnostic events");
check(events.stream().map(StepEvent::executionId).distinct().count() == 1, "execution identity");
System.out.println("events=" + events.stream().map(StepEvent::kind).toList());
```

Example.java: diagnostics(), Java 17+. Ожидается: events=[STARTED, SUCCEEDED]. Новая подписка создаёт новый execution ID.

Observer должен быть неблокирующим и потокобезопасным. Его ошибки попадают в Reactor onErrorDropped и не вызывают повтор бизнес-операции. События шагов не включают payload, но объект исключения всё ещё может содержать данные приложения. Если hook тоже бросает нефатальное исключение, оно отбрасывается без повторного вызова hook. Ошибки, которые Reactor считает фатальными, пробрасываются.

```bash
./gradlew -Pjava27 -PtestJavaVersion=27 build publishToMavenLocal
./gradlew -Pjava27 -PtestJavaVersion=27 :java27:profileJava27
```

Java 27, исходники библиотеки. Задача профилирования записывает java27/build/recordings/pipeline.jfr на синтетических данных и с очищенным окружением дочернего процесса.

```kotlin
implementation("io.github.59alex:reactive-pipeline-kit-java27:0.3.0")
```

Необязательная зависимость для JfrExample.java; минимум core остаётся Java 17.

```java
import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.java27.JfrStepObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordingFile;

public final class JfrExample {
    public static void main(String[] args) throws Exception {
        Path destination = Path.of(args.length == 0 ? "pipeline-example.jfr" : args[0]).toAbsolutePath();
        Files.createDirectories(destination.getParent());
        try (Recording recording = new Recording()) {
            recording.enable(JfrStepObserver.EVENT_NAME).withoutStackTrace();
            recording.start();
            Integer result = Pipeline.<Integer>start().map("increment", value -> value + 1)
                .observe(new JfrStepObserver()).run(6).block();
            if (!Integer.valueOf(7).equals(result)) throw new AssertionError(result);
            recording.stop();
            recording.dump(destination);
        }
        long count = RecordingFile.readAllEvents(destination).stream()
            .filter(event -> event.getEventType().getName().equals(JfrStepObserver.EVENT_NAME)).count();
        if (count != 2) throw new AssertionError("expected 2 step events, got " + count);
        System.out.println("jfrEvents=" + count);
    }
}

```

Полный JfrExample.java, Java 27. Ожидается: jfrEvents=2. Запись запускает и останавливает приложение.

> **Граница маскирования JFR**
>
> JfrStepObserver копирует только executionId, step, kind, attempt, elapsedNanos и errorType вместе со стандартными метаданными JFR. Payload и сообщения исключений не записываются. Маскирование при старте JVM в Java 27 относится к выбранным встроенным событиям запуска, а не к этим пользовательским полям. Используйте статичные имена шагов без чувствительных данных; библиотека не меняет параметры JVM и не обещает измеренного прироста производительности.

## 10. Разбирайте ошибки и исправляйте конфигурацию

StepFailure называет упавший шаг, показывает число попыток и сохраняет исходную причину. Пустой результат — ошибка, а не незаметная потеря записи.

```java
var invalid = Pipeline.<String>start().then("lookup", (value, context) -> Mono.empty());
try {
    invalid.run("java").block();
    throw new AssertionError("empty step must fail");
} catch (StepFailure failure) {
    check("lookup".equals(failure.step()), "failed step name");
    check(failure.getCause() instanceof NoSuchElementException, "empty completion cause");
    System.out.println("failure=" + failure.step() + "; cause=" + failure.getCause().getClass().getSimpleName());
}
```

Example.java: failure(). Ожидается: failure=lookup; cause=NoSuchElementException.

- Step completed without a value: верните одно значение либо введите явный тип результата вместо Mono.empty.
- duplicate step name: задайте каждому шагу уникальное непустое имя до запуска пайплайна.
- RejectedExecutionException: executor приложения закрыт или отклоняет задачи. Исправьте его жизненный цикл или ёмкость перед повторами.
- No matching variant или UnsupportedClassVersionError: модуль Java 25/27 подключён к более старой среде. Уберите его либо используйте нужный JDK; core и Jolt по-прежнему достаточно Java 17.
- Артефакт 0.3.0 не найден: сначала опубликуйте нужный модуль локально и добавьте mavenLocal в приложение. Для соответствующего модуля включите -Pjava25 или -Pjava27.
- Ничего не выполняется: run и process возвращают холодные publisher. Подпишитесь на границе приложения; не добавляйте внутренние subscribe в отдельные шаги.

> **Чего библиотека не гарантирует**
>
> Здесь нет постоянного журнала выполнения, восстановления после перезапуска, распределённой транзакции или гарантии exactly-once для побочных эффектов. Если записи должны переживать падение процесса, используйте внешний устойчивый источник и контрольные точки, а повторяемые эффекты делайте идемпотентными.

## Скачиваемые примеры

[Запустить примеры и вернуться к руководству](examples/README.md#русский).

- [Example.java](examples/Example.java)
- [JfrExample.java](examples/JfrExample.java)
- [JoltExample.java](examples/JoltExample.java)
- [ScopedExample.java](examples/ScopedExample.java)

[MIT](../../LICENSE)

[← Back to documentation and versions / Назад к документации и версиям](../index.md)
