# Repository Guidelines

## Project Structure & Module Organization
The root Gradle build keeps module configuration minimal; the `tools` module houses all active code. Kotlin sources live in `tools/src/main/kotlin/com/reggie/bricks/tools`, Vaadin UI assets in `tools/src/main/frontend`, and Spring configuration in `tools/src/main/resources`. Python utilities that orchestrate Databricks workflows sit under `tools/src/main/python/reggie_tools`, while the `prpr` directory stores prebuilt integration artifacts consumed at runtime. Build outputs land in the root `build/` folder; keep generated files out of version control.

## Build, Test, and Development Commands
Use `./gradlew build` to compile Kotlin, run checks, and assemble artifacts. `./gradlew :tools:bootRun` launches the Vaadin/Spring Boot app locally, picking up Kotlin and frontend changes. When frontend bindings change, execute `./gradlew :tools:vaadinBuildFrontend` to regenerate `tools/src/main/frontend/generated`. For Python helpers, run `python -m reggie_tools.<module>` from `tools/src/main/python` to validate scripts in isolation.

## Coding Style & Naming Conventions
Follow standard Kotlin style: four-space indentation, explicit visibility on public APIs, and PascalCase for classes such as `ZerobusService`. Align Kotlin and Vaadin code with Spring idioms (constructor injection, `@Route` annotations). Python modules use snake_case names and prefer type-annotated functions. Avoid mixing Kotlin and Python responsibilities in the same directory; keep shared constants in Kotlin `object`s or Python `constants.py` siblings.

## Testing Guidelines
Add Kotlin tests under `tools/src/test/kotlin`, mirroring the production package. Use JUnit 5 and AssertJ-style assertions; name tests with the pattern `ClassNameTests`. For Vaadin components, prefer integration-level tests that exercise routing and RPC callbacks. Python utilities should ship with `pytest` suites stored alongside the modules (e.g., `test_configs.py`); run them via `pytest tools/src/main/python`.

## Commit & Pull Request Guidelines
Write concise, imperative commit messages (`refine camera bootstrap`, `fix vaadin build`). Reference linked issues in the body when applicable. Pull requests should summarise the change set, note affected modules, list manual validation (`./gradlew build`, `pytest`), and include screenshots or logs for UI or workflow updates. Keep PRs focused; defer unrelated refactors to follow-up branches.
