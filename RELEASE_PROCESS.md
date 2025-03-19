# Release Process

This document outlines the process to follow when releasing a new version of the project.

## Releasing a New Version

### 1. Prepare for Release

1. Decide on a version number for the new release following [Semantic Versioning](https://semver.org/) (e.g., `0.5.0`).

2. Locally or using the GitHub UI, trigger the `release-prepare` workflow:
   ```bash
   make release VERSION=0.5.0
   ```
   or manually trigger the workflow via GitHub Actions UI.

3. This will:
   - Create a new branch `release/v0.5.0` from the latest `develop` branch
   - Update version numbers in all `Cargo.toml` files to `0.5.0`
   - Generate a CHANGELOG.md file
   - Create a pull request from `release/v0.5.0` to `main`

### 2. Review and Merge the Release PR

1. Review the generated PR to ensure the changelog and version changes are correct.
2. Request reviews from other team members as necessary.
3. Make any final adjustments directly to the `release/v0.5.0` branch.
4. Once approved, merge the PR into `main`.

### 3. Automatic Release Finalization

When the release PR is merged to `main`, the `release-finalize` workflow will automatically:

1. Create and push a tag for the release (e.g., `v0.5.0`)
2. Create a GitHub Release with the changelog content
3. Build and publish Docker images for the release
4. Create a PR to merge changes back to `develop`
5. Automatically merge this PR into `develop`
6. Automatically bump the version on `develop` to the next development version (e.g., `0.5.1-dev`)

No manual intervention is required for these steps unless there are conflicts when merging back to `develop`.

## Hotfix Process

For urgent fixes that need to bypass the normal release flow:

1. Create a branch `hotfix/v0.5.1` from `main`
2. Make your changes and commit them
3. Update version numbers in all `Cargo.toml` files to `0.5.1`
4. Create a PR from `hotfix/v0.5.1` to `main`
5. After the PR is merged, the same automatic finalization steps listed above will occur

## Version Numbering

We follow [Semantic Versioning](https://semver.org/):

- MAJOR version for incompatible API changes
- MINOR version for backwards-compatible functionality
- PATCH version for backwards-compatible bug fixes

Development versions on the `develop` branch have a `-dev` suffix appended to the patch number (e.g., `0.5.1-dev`).

## Changelog Generation

The changelog is generated automatically using [git-cliff](https://github.com/orhun/git-cliff), which parses [Conventional Commits](https://www.conventionalcommits.org/) to generate a structured changelog.

To ensure your commits appear correctly in the changelog, prefix them with:

- `feat:` for new features
- `fix:` for bug fixes
- `chore:` for maintenance tasks
- `docs:` for documentation updates
- `refactor:` for code refactoring
- `test:` for adding or updating tests
- `perf:` for performance improvements 