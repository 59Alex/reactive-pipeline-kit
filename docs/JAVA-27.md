# Historical Java 27 migration (superseded)

This note describes the superseded **0.2.0** release, which required Java 27 for
all modules and put scoped context in the core blocking adapter.

**Version 0.3.0 restores Java 17 as the minimum for core and Jolt and supports
LTS runtimes 17, 21, and 25, plus Java 27.** Scoped values now live in the optional
`reactive-pipeline-kit-java25` artifact. Core blocking operations take an explicit
`ExecutionContext` parameter, and the core/Jolt default build needs no Java 25
or 27 toolchain.

Java 27 remains supported and has an opt-in `reactive-pipeline-kit-java27`
[JFR diagnostics profile](JFR-JAVA27.md). Restoring the minimum does not remove
Java 27 support; it keeps newer capabilities in separately selected modules.

Use [Java support and migration](JAVA-SUPPORT.md) for current requirements,
commands, and API migration. The former Java 27 requirement is not the current
support contract.
