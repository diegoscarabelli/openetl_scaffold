# Contributing

Guidelines for contributing to this project. Follows conventions from [openetl](https://github.com/diegoscarabelli/openetl).

## Table of Contents

- [How to Contribute](#how-to-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Features](#suggesting-features)
  - [Submitting Pull Requests](#submitting-pull-requests)
- [Development Guidelines](#development-guidelines)
  - [Formatting](#formatting)
  - [Testing](#testing)
  - [Documentation](#documentation)
- [Code Review Process](#code-review-process)

## How to Contribute

Before contributing code, complete the setup steps in the [README Quick Start](README.md#quick-start) section, then install development dependencies:

```bash
make venv
pre-commit install
```

### Reporting Bugs

When reporting a bug, include:

- Clear description of the bug.
- Steps to reproduce.
- Expected vs actual behavior.
- Environment details (OS, Python version, PostgreSQL version, etc.).
- Relevant logs or error messages.

### Suggesting Features

When suggesting a feature, include:

- Problem description.
- Proposed solution.
- Use case and benefits.
- Any alternatives you have considered.

### Submitting Pull Requests

Before submitting a PR, ensure:

1. **Your code follows the formatting standards** (see [Formatting](#formatting) below).
2. **All tests pass**: `make test`.
3. **Code is properly formatted**: `make format` and `make check-format`.
4. **New code has tests** with adequate coverage.
5. **Documentation is updated** (README, docstrings, etc.).

## Development Guidelines

### Formatting

Python code formatting is handled using the following tools:

- **[Black](https://github.com/psf/black)**: Formats Python code with a focus on readability and consistency.
- **[Autoflake](https://github.com/myint/autoflake)**: Removes unused imports and variables to keep the code clean.
- **[Docformatter](https://github.com/PyCQA/docformatter)**: Formats docstrings to ensure compliance with `black` and maintain a consistent style.

For SQL files, **[SQLFluff](https://docs.sqlfluff.com/en/stable/)** is used to enforce consistent formatting. Some files are ignored by SQLFluff as specified in the [`.sqlfluffignore`](.sqlfluffignore) file.

The configuration for all these formatting tools is defined in the [`pyproject.toml`](pyproject.toml) file.

#### Running Formatters

The [`Makefile`](Makefile) defines `format` and `check-format` targets.

To format all Python and SQL files:
```bash
make format
```

To verify compliance without making changes:
```bash
make check-format
```

#### Automating Formatting

Install **[pre-commit](https://pre-commit.com/)** hooks to run formatters automatically before each commit:

```bash
pip install pre-commit
pre-commit install
```

The hooks are configured in [`.pre-commit-config.yaml`](.pre-commit-config.yaml) and use the same `make format` target.

#### Formatting Standards

##### Code Style Rules

- **Use Black formatting** with a maximum of 88 characters per line.
- **Apply docformatter style** per [pyproject.toml](pyproject.toml) configuration:
  - Add a blank line after each docstring.
  - Ensure Black-compatible formatting.
  - Use multi-line summary format.
  - Add a pre-summary newline for improved readability.
- **Include type hints** for all function parameters and return values.
- **Use `:param` and `:return`** tags in docstrings (not Google or NumPy style).
- **Automatically remove unused imports** using autoflake.
- **Use f-strings for string interpolation** instead of other formatting methods.
- **Print full tracebacks for exceptions** rather than only the exception message.

##### Import Organization

1. **Standard library imports** (alphabetically sorted)
   - `import` statements first.
   - `from` imports second.
2. **Blank line**
3. **Third-party imports** (alphabetically sorted)
   - `import` statements first.
   - `from` imports second.
4. **Blank line**
5. **Local application imports** (alphabetically sorted)
   - `import` statements first.
   - `from` imports second.
6. **Two blank lines** before first class/function.

##### Spacing Rules

- **Module docstring**: Immediately at the top.
- **Two blank lines**: After module docstring, before imports.
- **Two blank lines**: After imports, before first class/function.
- **Two blank lines**: Between classes and top-level functions.
- **One blank line**: Between methods within a class.
- **One blank line**: After class docstrings, before first attribute or method (PEP 257).

##### File Structure Example

```python
"""
Module description goes here.
Explain the purpose and main functionality of this module.
"""


import os
import sys
from pprint import pformat
from typing import Union

import pendulum
import sqlalchemy
from requests import Session
from sqlalchemy import create_engine

from .local_module import LocalClass
from .utils import helper_function


class ExampleClass:
    """
    Class description that demonstrates the multi-line format with pre-summary
    newline as configured in pyproject.toml.

    :param param1: Description of param1.
    :return: Description of return value if applicable.
    """

    def __init__(self, param1: int) -> None:
        """
        Initialize the class with the provided parameter following docformatter
        configuration.

        :param param1: Description of param1.
        """

        self.param1 = param1


def example_function(arg1: int, arg2: str) -> bool:
    """
    Function description that follows the project's docformatter settings for
    multi-line summaries.

    :param arg1: Description of arg1.
    :param arg2: Description of arg2.
    :return: True if successful, False otherwise.
    """

    return True
```

##### Quality Checklist

Before pushing any changes, verify:

**Manual Checks (REQUIRED before automated tools):**
- [ ] Module docstring present (see [File Structure Example](#file-structure-example)).
- [ ] Proper spacing and blank lines throughout file (see [Spacing Rules](#spacing-rules)).
- [ ] No trailing whitespace.
- [ ] Periods at the end of sentences such as comments, docstrings, bullet lists, log messages (not the header of the file).
- [ ] No line in Python and SQL files longer than 88 characters (see [Code Style Rules](#code-style-rules)).

**Automated Checks:**
- [ ] Imports properly organized and sorted, no unused imports (see [Import Organization](#import-organization)).
- [ ] All functions have type hints (see [Code Style Rules](#code-style-rules)).
- [ ] All docstrings use `:param` and `:return` format (see [Code Style Rules](#code-style-rules)).
- [ ] Black-compliant format (see [Code Style Rules](#code-style-rules)).
- [ ] Consistent indentation (4 spaces).

### Testing

All code changes should include appropriate tests. We use [pytest](https://docs.pytest.org/) for testing.

#### Running Tests

```bash
# Run all tests with coverage.
make test

# Run specific test file.
pytest tests/path/to/test_file.py

# Run specific test function.
pytest tests/path/to/test_file.py::test_function_name

# Run with verbose output.
pytest -v
```

#### Test Requirements

- **New features** should include tests demonstrating the functionality.
- **Bug fixes** should include tests that would have caught the bug.
- **Aim for high coverage** on new code.
- **Use fixtures** from `conftest.py` when appropriate.
- **Follow existing test patterns** in the repository.

#### Test Environment

Tests require a PostgreSQL database. Point `SQL_DB_NAME` in `.env` at `system2_dev` (created by `database.ddl`).

### Documentation

Update documentation when making changes:

- **README.md**: Update if adding new features, changing setup steps, or modifying core functionality.
- **Docstrings**: Add/update for all new/modified functions and classes.
- **Pipeline READMEs**: Update pipeline-specific documentation in `dags/pipelines/*/README.md`.
- **CLAUDE.md**: Update if changing development workflows or standards.

## Code Review Process

1. **Automated checks** via pre-commit hooks.
   - Code formatting verification.
   - Must pass before commit.

2. **Manual review** by project maintainers.
   - Code quality and style.
   - Test coverage and quality.
   - Documentation completeness.
   - Architectural fit.

3. **Feedback and iteration**.
   - Address reviewer comments.
   - Make requested changes.
   - Push updates to your PR branch.

4. **Approval and merge**.
   - PRs require approval from a maintainer.
   - Maintainer will merge once approved.
