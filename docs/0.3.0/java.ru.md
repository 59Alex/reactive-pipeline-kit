# reactive-pipeline-kit 0.3.0 — Java: совместимость и используемые возможности

[English](java.en.md) · [Русский](java.ru.md)<br>
[Руководство](ru.md) · [Версии библиотеки](../README.md)

[Java 17](#java-17) · [Java 21](#java-21) · [Java 25](#java-25) · [Java 27](#java-27)

Версия библиотеки и версия Java — разные вещи. Артефакты 0.3.0 имеют базу Java 17. Запуск на более новой JVM сам по себе не означает, что библиотека использует каждый новый API или даёт измеренный прирост производительности.

## Java 17

Минимальная версия ядра. Компиляция с --release 17, class-file 61.0, без preview. Все основные возможности из руководства доступны на этой версии.

```sh
./gradlew build -PtestJavaVersion=17
```

## Java 21

Добавляется возможность передать ядру executor виртуальных потоков из Java 21. BlockingSteps принимает обычный ExecutorService, поэтому ядро сохраняет Java 17. Потоки создаёт и закрывает приложение; ограничение конкурентности задаётся отдельно.

```java
var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
try {
    var step = io.github.alex59.pipeline.BlockingSteps.<String, String>call(
        executor, (input, context) -> input.trim());
    var result = io.github.alex59.pipeline.Pipeline.<String>start()
        .then("normalize", step).run(" hello ").block();
    System.out.println(result); // hello
} finally {
    executor.shutdownNow();
}
```

Это пример внутри main приложения на Java 21+. Виртуальные потоки полезны для блокирующего I/O; в примере операция намеренно проста. Отмена просит прервать работу, но не откатывает внешние эффекты.

```sh
./gradlew build -PtestJavaVersion=21
```

## Java 25

Необязательный reactive-pipeline-kit-java25 использует ScopedValue для контекста выполнения внутри синхронной blocking operation. Привязка очищается при успехе и ошибке; через асинхронные переключения Reactor она автоматически не передаётся. Минимум модуля — Java 25, ядра — 17.

```sh
./gradlew -Pjava25 -PtestJavaVersion=25 build publishToMavenLocal
```

[Полный пример ScopedValue](examples/ScopedExample.java) · [Руководство](ru.md)

## Java 27

Необязательный reactive-pipeline-kit-java27 добавляет JfrStepObserver и пример профилирования с очисткой чувствительных стартовых параметров средствами JDK 27 (JEP 536). Observer использует стабильный JFR API; новые фильтры задаются JVM при запуске. Они не очищают произвольные поля собственных событий: имена шагов должны быть нечувствительными. Payload и сообщения исключений observer не записывает.

```sh
./gradlew -Pjava27 -PtestJavaVersion=27 build :java27:profileJava27
```

[Полный пример JFR](examples/JfrExample.java) · [Границы redaction](../JFR-JAVA27.md)

## Что изменилось между версиями библиотеки

| Версия | Изменение требований и возможностей Java |
| --- | --- |
| 0.1.0 | База Java 17. |
| 0.2.0 | Историческая версия с обязательной Java 27; текущей рекомендацией не является. |
| 0.3.0 | Минимум снова Java 17; Java 21 virtual threads через executor приложения, ScopedValue вынесен в java25, JFR — в java27. |

[Совместимость и миграция](../JAVA-SUPPORT.md)
