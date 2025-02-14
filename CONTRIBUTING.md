# Contributing to Aura

Thank you for your interest in contributing to the Metaplex Aura Network! This document outlines our development process and guidelines.

## Getting Started

### Prerequisites

-   **Rust:** This project requires Rust **version 1.84**. We recommend using `rustup` to manage your Rust installation. You can install `rustup` by following the instructions at [https://rustup.rs/](https://rustup.rs/). After installing `rustup`, ensure you have the correct version:

    ```bash
    rustup toolchain install 1.84
    rustup default 1.84
    ```

-   **Docker and Docker Compose:** Many components of Aura are run within Docker containers. Install Docker and Docker Compose from [https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/).
-   **System Libraries:** You may need to install additional system libraries depending on your operating system. (We should add specific instructions for common OSes here). Specifically, you'll need `protobuf-compiler`:
    ```bash
    sudo apt-get update && sudo apt-get install -y protobuf-compiler # Example for Debian/Ubuntu
    ```
-   **Git:** You'll need Git to clone the repository and manage your changes.

### Setup

1.  **Clone the Repository:**

    ```bash
    git clone <repository_url>
    cd <repository_name>
    ```

2.  **Build the Project:**

    ```bash
    cargo build
    ```

    This will download dependencies and build the project. The first build may take a while.

3.  **Run Unit Tests:**

    ```bash
    cargo test
    ```

4.  **Run Integration Tests:**

    ```bash
    cargo test -F integration_tests
    ```

    Note: Integration tests may require specific environment variables or setup. See the `README.md` and `docker-compose.yaml` for details. The integration tests use a local PostgreSQL instance, as configured in the `rust.yml` CI workflow.

### IDE Setup (Recommended)

We recommend using VS Code with the Rust Analyzer extension for development. Install the extension from the VS Code marketplace.

## GitFlow Workflow

We follow the GitFlow branching model for our development process. This provides a robust framework for managing larger projects.

### Main Branches

-   `main` - Production-ready code. This branch contains the latest released version.
-   `develop` - Main development branch. Features and fixes are merged here before release.

### Supporting Branches

1.  **Feature Branches**
    -   Branch from: `develop`
    -   Merge back into: `develop`
    -   Naming convention: `feature/MTG-<ticket_number>-description-of-feature` (Include the ticket number!)
    -   Example: `feature/MTG-123-add-compression-support`

2.  **Release Branches**
    -   Branch from: `develop`
    -   Merge back into: `main` and `develop`
    -   Naming convention: `release/version-number`
    -   Example: `release/1.2.0`

3.  **Hotfix Branches**
    -   Branch from: `main`
    -   Merge back into: `main` and `develop`
    -   Naming convention: `hotfix/MTG-<ticket_number>-description`
    -   Example: `hotfix/MTG-456-fix-critical-security-issue`

### Development Process

1.  **Starting a New Feature**

    ```bash
    git checkout develop
    git pull origin develop
    git checkout -b feature/MTG-<ticket_number>-your-feature-name
    ```

2.  **Working on Your Feature**
    -   Make your changes.
    -   Commit regularly with meaningful messages (see Commit Messages below).
    -   Keep your branch up to date with `develop`:

    ```bash
    git checkout develop
    git pull origin develop
    git checkout feature/MTG-<ticket_number>-your-feature-name
    git rebase develop
    ```

3.  **Completing a Feature**
    -   Ensure all tests pass (`cargo test` and `cargo test -F integration_tests`).
    -   Format your code using **nightly** `rustfmt`:
        ```bash
        cargo +nightly fmt
        ```
    -   Run `clippy` to check for potential issues:
        ```bash
        cargo clippy --all -- -D warnings
        ```
    -   Create a pull request to merge into `develop`.
    -   Request code review.
    -   Address review comments.
    -   Merge only after approval.

4.  **Creating a Release**

    ```bash
    git checkout develop
    git pull origin develop
    git checkout -b release/x.y.z
    # Make release-specific changes (version numbers, etc.)
    ```

    -   Test thoroughly.
    -   Merge into `main` and `develop`.
    -   Tag the release in `main`.

5.  **Hotfix Process**

    ```bash
    git checkout main
    git pull origin main
    git checkout -b hotfix/MTG-<ticket_number>-issue-description
    ```

    -   Fix the issue.
    -   Merge into both `main` and `develop`.
    -   Tag the new version in `main`.

### Commit Messages

We follow [Conventional Commits](https://www.conventionalcommits.org/) specification for our commit messages. This leads to more readable messages that are easy to follow when looking through the project history and enables automatic generation of changelogs.

Each commit message should be structured as follows:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

Common types:

-   `feat`: A new feature
-   `fix`: A bug fix
-   `docs`: Documentation only changes
-   `style`: Changes that do not affect the meaning of the code (formatting, etc.)
-   `refactor`: A code change that neither fixes a bug nor adds a feature
-   `perf`: A code change that improves performance
-   `test`: Adding missing tests or correcting existing tests
-   `chore`: Changes to the build process or auxiliary tools

Example:

```
feat(api): add pagination to assets endpoint

- Implement cursor-based pagination
- Add limit parameter
- Update API documentation

Closes #123
```

Additionally, follow these guidelines for the message content:

-   Use the present tense ("Add feature" not "Added feature").
-   Use the imperative mood ("Move cursor to..." not "Moves cursor to...").
-   Limit the first line to 72 characters or less.
-   Reference issues and pull requests liberally after the first line.

### Code Style

We use `rustfmt` and `clippy` to enforce a consistent code style. Run these tools before submitting a pull request:

```bash
cargo +nightly fmt
cargo clippy --all -- -D warnings
```

The `rustfmt.toml` file in the repository defines the formatting rules. We use a minimal `.clippy.toml` that allows deprecated code, mirroring the CI configuration.

### Continuous Integration

All branches must pass our CI pipeline before being merged. The CI pipeline includes:

-   **Code Style Checks:** `rustfmt` (nightly) and `clippy`.
-   **Unit Tests:** `cargo test`.
-   **Integration Tests:** `cargo test -F integration_tests`.
-   **Build Verification:** Ensures the project builds successfully.

CI is automatically triggered for:

-   All pull requests targeting `develop` or `main`.
-   Release branches.
-   Hotfix branches.

Note: Direct pushes to `develop` and `main` branches are prohibited. All changes must go through pull requests.

### Pull Request Process

1.  Update the `README.md` or relevant documentation (e.g., `architecture.md`) with details of changes if needed.
2.  Add tests for new functionality.
3.  Ensure the test suite passes (`cargo test` and `cargo test -F integration_tests`).
4.  Get approval from at least two code reviewers.
5.  Update your branch with the latest `develop` before merging (`git rebase develop`).
6.  All CI checks must pass before merging.

### Code Review Guidelines

When reviewing code:

-   Check for technical correctness.
-   Verify test coverage.
-   Validate documentation updates.
-   Consider performance implications.
-   Look for security considerations.

### Additional Guidelines

-   Write clean, maintainable, and testable code.
-   Follow the existing code style and patterns.
-   Document new features or significant changes.
-   Include tests for new functionality.
-   Keep pull requests focused and reasonably sized.

## Troubleshooting

-   **macOS Build Issues (OpenSSL/protobuf-src):** If you encounter build failures related to C/C++ libraries like OpenSSL or `protobuf-src` on macOS, try reinstalling the command-line tools:

    ```bash
    sudo rm -rf /Library/Developer/CommandLineTools
    xcode-select --install
    ```

## Questions or Need Help?

Feel free to reach out to the maintainers on [#aura Discord Channel](https://discord.gg/metaplex) or open an issue if you have questions about the contribution process.
