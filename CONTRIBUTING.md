# Contributing to Aura

Thank you for your interest in contributing to the Metaplex Aura Network! This document outlines our development process and guidelines.

## GitFlow Workflow

We follow the GitFlow branching model for our development process. This provides a robust framework for managing larger projects.

### Main Branches

- `main` - Production-ready code. This branch contains the latest released version.
- `develop` - Main development branch. Features and fixes are merged here before release.

### Supporting Branches

1. **Feature Branches**
   - Branch from: `develop`
   - Merge back into: `develop`
   - Naming convention: `feature/description-of-feature`
   - Example: `feature/add-compression-support`

2. **Release Branches**
   - Branch from: `develop`
   - Merge back into: `main` and `develop`
   - Naming convention: `release/version-number`
   - Example: `release/1.2.0`

3. **Hotfix Branches**
   - Branch from: `main`
   - Merge back into: `main` and `develop`
   - Naming convention: `hotfix/description`
   - Example: `hotfix/fix-critical-security-issue`

### Development Process

1. **Starting a New Feature**
   ```bash
   git checkout develop
   git pull origin develop
   git checkout -b feature/your-feature-name
   ```

2. **Working on Your Feature**
   - Make your changes
   - Commit regularly with meaningful messages
   - Keep your branch up to date with develop:
   ```bash
   git checkout develop
   git pull origin develop
   git checkout feature/your-feature-name
   git rebase develop
   ```

3. **Completing a Feature**
   - Ensure all tests pass
   - Create a pull request to merge into `develop`
   - Request code review
   - Address review comments
   - Merge only after approval

4. **Creating a Release**
   ```bash
   git checkout develop
   git pull origin develop
   git checkout -b release/x.y.z
   # Make release-specific changes (version numbers, etc.)
   ```
   - Test thoroughly
   - Merge into `main` and `develop`
   - Tag the release in `main`

5. **Hotfix Process**
   ```bash
   git checkout main
   git pull origin main
   git checkout -b hotfix/issue-description
   ```
   - Fix the issue
   - Merge into both `main` and `develop`
   - Tag the new version in `main`

### Commit Messages

Follow these guidelines for commit messages:
- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters or less
- Reference issues and pull requests liberally after the first line

Example:
```
Add compression support for NFT metadata

- Implement ZSTD compression for metadata storage
- Add decompression utilities
- Update tests to cover compression/decompression
- Fixes #123
```

### Pull Request Process

1. Update the README.md or relevant documentation with details of changes if needed
2. Add tests for new functionality
3. Ensure the test suite passes
4. Get approval from at least two code reviewers
5. Update your branch with the latest `develop` before merging
6. All CI checks must pass before merging

### Code Review Guidelines

When reviewing code:
- Check for technical correctness
- Verify test coverage
- Validate documentation updates
- Consider performance implications
- Look for security considerations

### Additional Guidelines

- Write clean, maintainable, and testable code
- Follow the existing code style and patterns
- Document new features or significant changes
- Include tests for new functionality
- Keep pull requests focused and reasonably sized

## Questions or Need Help?

Feel free to reach out to the maintainers or open an issue if you have questions about the contribution process. 
