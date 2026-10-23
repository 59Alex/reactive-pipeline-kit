# Runnable examples / Запускаемые примеры

[English guide](../en.md) · [Руководство на русском](../ru.md) · [Versions / Версии](../../index.md) · [Library README](../../../README.md)

## English

This directory is an independent Gradle consumer. It resolves published local
JARs and their transitive dependencies; it does not compile the library sources
or substitute sibling projects. Each selected Java file is self-contained.

Run from the library repository root, with JDK 17+:

```sh
./gradlew build publishToMavenLocal
./gradlew -p docs/0.3.0/examples run
./gradlew -p docs/0.3.0/examples run -Pexample=JoltExample
```

| Source | Minimum JVM | Assertion and output |
| --- | --- | --- |
| [Example.java](Example.java) | 17 | Eight scenarios: typed=4, retry=10 with previous=1 and attempts=3, timeout=slow, ordered=[1, 2, 3], http=503, blocking=team:java, STARTED/SUCCEEDED events, named empty-result failure |
| [JoltExample.java](JoltExample.java) | 17 | jolt={record={key=7}}; inputUnchanged=true |
| [ScopedExample.java](ScopedExample.java) | 25 | scoped=team:java; outsideBound=false; operation runs on a virtual thread |
| [JfrExample.java](JfrExample.java) | 27 | jfrEvents=2; recording under this consumer's build/recordings |

Build and run optional examples with their required installed JDKs:

```sh
./gradlew -Pjava25 -PtestJavaVersion=25 build publishToMavenLocal
./gradlew -p docs/0.3.0/examples run -Pexample=ScopedExample -PexampleJava=25

./gradlew -Pjava27 -PtestJavaVersion=27 build publishToMavenLocal
./gradlew -p docs/0.3.0/examples run -Pexample=JfrExample -PexampleJava=27
```

Set -PexampleJava=17, 21, 25 or 27 to choose an installed consumer toolchain.
Example and JoltExample compile with --release 17 regardless of that runtime.
ScopedExample requires at least 25; JfrExample requires at least 27. No preview.
Gradle itself runs on the JDK selected by JAVA_HOME. Optional modules must be
built before their consumer; otherwise dependency resolution fails explicitly.

The HTTP demonstration binds only a loopback ephemeral port. There are no remote
calls or credentials. Calls to block are confined to the executable demo boundary.
JFR uses synthetic metadata; this example does not enable startup environment or
system-property recording. The separate [Java 27 profiling task](../../JFR-JAVA27.md)
demonstrates startup redaction.

To use a fresh directory, copy settings.gradle.kts, build.gradle.kts and just the
selected Java file there, then run it with Gradle 9.8.2 (or the repository wrapper
with -p pointing at that directory). The library must already be in Maven Local.

[← Back to the English guide](../en.md)

## Русский

Этот каталог — отдельный Gradle consumer. Он использует локально опубликованные
JAR и их транзитивные зависимости, без компиляции исходников библиотеки и подмены
зависимостей соседними проектами. Каждый выбранный Java-файл самодостаточен.

Из корня библиотеки с JDK 17+:

```sh
./gradlew build publishToMavenLocal
./gradlew -p docs/0.3.0/examples run
./gradlew -p docs/0.3.0/examples run -Pexample=JoltExample
```

| Исходник | Минимальная JVM | Проверка и результат |
| --- | --- | --- |
| [Example.java](Example.java) | 17 | Восемь сценариев: typed=4, retry=10 при previous=1 и attempts=3, timeout=slow, ordered=[1, 2, 3], http=503, blocking=team:java, события STARTED/SUCCEEDED, ошибка пустого результата с именем шага |
| [JoltExample.java](JoltExample.java) | 17 | jolt={record={key=7}}; inputUnchanged=true |
| [ScopedExample.java](ScopedExample.java) | 25 | scoped=team:java; outsideBound=false; операция работает на виртуальном потоке |
| [JfrExample.java](JfrExample.java) | 27 | jfrEvents=2; запись в build/recordings этого consumer |

Необязательные примеры требуют установки соответствующих JDK:

```sh
./gradlew -Pjava25 -PtestJavaVersion=25 build publishToMavenLocal
./gradlew -p docs/0.3.0/examples run -Pexample=ScopedExample -PexampleJava=25

./gradlew -Pjava27 -PtestJavaVersion=27 build publishToMavenLocal
./gradlew -p docs/0.3.0/examples run -Pexample=JfrExample -PexampleJava=27
```

Параметр -PexampleJava=17, 21, 25 или 27 выбирает установленный JDK consumer.
Example и JoltExample компилируются с --release 17 независимо от выбранной JVM.
ScopedExample требует минимум 25, JfrExample — 27. Preview не используется.
Сам Gradle запускается на JDK из JAVA_HOME. Сначала соберите нужный необязательный
модуль: иначе consumer сообщит об отсутствующей зависимости.

HTTP-пример использует только loopback и случайный свободный порт. Внешних запросов
и credentials нет. Вызовы block находятся на границе демонстрационной программы.
JFR записывает синтетические данные; этот пример не включает запись окружения и
системных свойств при старте. Для startup redaction есть отдельная
[задача профилирования Java 27](../../JFR-JAVA27.md).

Для проверки в новом каталоге скопируйте туда settings.gradle.kts, build.gradle.kts
и только выбранный Java-файл. Запустите Gradle 9.8.2 либо wrapper библиотеки с -p,
указав новый каталог. Библиотека должна быть уже собрана в Maven Local.

[← Назад к русскому руководству](../ru.md) · [README библиотеки](../../../README.md)
