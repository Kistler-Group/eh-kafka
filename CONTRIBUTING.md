# Contributing to Event Horizon

First off, thanks for taking the time to contribute!

The following is a set of guidelines for contributing to Event Horizon Kafka, which are hosted in the [Kistler-Group/eh-kafka](https://github.com/Kistler-Group/eh-kafka) on GitHub. These are mostly guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.

#### Table Of Contents

[Code of Conduct](#code-of-conduct)

[How Can I Contribute?](#how-can-i-contribute)

* [Reporting Bugs](#reporting-bugs)
* [Pull Requests](#pull-requests)

[Styleguides](#styleguides)

* [Git Commit Messages](#git-commit-messages)
* [Golang Styleguide](#golang-styleguide)
* [Documentation Styleguide](#documentation-styleguide)

## Code of Conduct

This project and everyone participating in it is governed by the [Event Horizon Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

If you find a bug report it by creating a new Github issue.

> **Note:** If you find a **Closed** issue that seems like it is the same thing that you're experiencing, open a new issue and include a link to the original issue in the body of your new one.

### Pull Requests

* Fill in [the required template](PULL_REQUEST_TEMPLATE.md)
* Follow the [Golang Styleguide](#golang-styleguide)
* Document new code based on the [Documentation Styleguide](#documentation-styleguide)
* End all files with a newline

## Styleguides

### Git Commit Messages

* Use the present tense ("Add feature" not "Added feature")
* Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
* Limit the first line to 72 characters or less
* Reference issues and pull requests liberally after the first line

### Golang Styleguide

All Golang code should adhere to [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments). Package imports should be ordered with a blank line between each block:

* stdlib
* 3rd party
* internal

### Documentation Styleguide

Documentation should be provided in the Godoc format in the source files for all public interfaces. Other documentation should be written as Markdown files in the `docs` folder.
