# Contributing to Aura

Thank you for your interest in contributing to the Metaplex Aura Network! This document outlines our development process and guidelines.

## Getting Started

### Prerequisites

-   **Rust:** This project requires Rust. We recommend using `rustup` to manage your Rust installation. You can install `rustup` by following the instructions at [https://rustup.rs/](https://rustup.rs/). The `rust-toolchain.toml` file in the repository specifies the required Rust version.
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

    Note: Integration tests may require specific environment variables or setup. **Before running integration tests locally, ensure you have bootstrapped the PostgreSQL database using `integration_tests/run_postgres.sh`.** See the `README.md` and `docker-compose.yaml` for details. The integration tests use a local PostgreSQL instance, as configured in the `rust.yml` CI workflow.

### IDE Setup (Recommended)

We recommend using VS Code with the Rust Analyzer extension for development. Install the extension from the VS Code marketplace.

## GitFlow Workflow

We follow the GitFlow branching model, with specific merge strategies for different branch types (detailed below).

### Main Branches

-   `main` - Production-ready code. This branch contains the latest released version.
-   `develop` - Main development branch. Features and fixes are merged here before release.

### Supporting Branches

1.  **Feature Branches**
    -   Branch from: `develop`
    -   Merge back into: `develop`
    -   Naming convention: `feature/[MTG-<ticket_number>-]description-of-feature` (Ticket number is preferred, but omit `MTG-<ticket_number>-` if you don't have one.)
    -   Example: `feature/MTG-123-add-compression-support` or `feature/add-compression-support`

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

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification for our commit messages. This leads to more readable messages that are easy to follow when looking through the project history and enables automatic generation of changelogs.

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

The `rustfmt.toml` file in the repository defines the formatting rules.

### Continuous Integration

All branches must pass our CI pipeline before being merged. The CI pipeline includes:

-   **Code Style Checks:** `rustfmt` (nightly) and `clippy`.
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
7.  **Merge Strategy:** The merge strategy depends on the target branch:
    -   **Feature branches into `develop`:** Squash merge. This creates a single, clean commit representing the feature.
    -   **Release branches into `main`:** *No squash* merge (a regular merge commit). This preserves the history of the release preparation. Tag the merge commit on `main` with the release version.
    -   **Release branches into `develop`:** *No squash* merge (a regular merge commit). This ensures the release history is incorporated into `develop`.
    -   **Hotfix branches into `main`:** *No squash* merge (a regular merge commit). This preserves the history of the hotfix. Tag the merge commit on `main` with the hotfix version.
    -   **Hotfix branches into `develop`:** *No squash* merge (a regular merge commit). This ensures the hotfix is incorporated into `develop`.

    In summary:
    - Squash merge: Feature branches into `develop`.
    - No squash (regular merge): Release and Hotfix branches into `main` and `develop`.

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

### Release Process

1.  **Creating a Release Branch:**

    ```bash
    git checkout develop
    git pull origin develop
    git checkout -b release/x.y.z  # e.g., release/1.2.0
    ```

2.  **Stabilization and Bug Fixes:**
    -   The release branch is used for final testing and stabilization.
    -   **Do not add new features to a release branch.**
    -   If bugs are found during this phase:
        -   Fix the bug on `develop` (ideally in a feature branch). Create a pull request for this fix, targeting `develop`. Once approved and merged (squash merge), cherry-pick the commit onto the release branch:
            ```bash
            git checkout release/x.y.z
            git cherry-pick <commit_hash_from_develop>
            ```

3.  **Merging the Release:**
    -   Once the release is stable, merge it into `main`:
        ```bash
        git checkout main
        git pull origin main
        git merge --no-ff release/x.y.z  # --no-ff creates a merge commit
        git tag -a x.y.z -m "Release x.y.z"  # Tag the release
        git push origin main --tags
        ```
    -   Then, merge the release branch into `develop`:
        ```bash
        git checkout develop
        git pull origin develop
        git merge --no-ff release/x.y.z
        git push origin develop
        ```

### Troubleshooting

- **macOS Build Issues (OpenSSL/protobuf-src):** If you encounter build failures related to C/C++ libraries like OpenSSL or `protobuf-src` on macOS, try reinstalling the command-line tools:

    ```bash
    sudo rm -rf /Library/Developer/CommandLineTools
    xcode-select --install
    ```

## Questions or Need Help?

Feel free to reach out to the maintainers on [#aura Discord Channel](https://discord.gg/metaplex) or open an issue if you have questions about the contribution process.