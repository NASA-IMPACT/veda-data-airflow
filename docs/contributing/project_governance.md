# SM2A Project Governance

## 1. Introduction

This is a evolving document that outlines the governance structure, development workflow, and best practices for the SM2A project. It serves as a guide for contributors to understand how to effectively participate in the project.

## 2. Code Ownership

* **Module Ownership**

  * `self-managed-apache-airflow` (Terraform module for underlying infrastructure patterns): governed by **@amarouane-ABDELHAK**
* **Contact**

  * Open an issue in GitHub.
  * Reach out on Slack in **#veda-data-services** (VEDA data services team).

## 3. Development Workflow

### 3.1 Branching Strategy

* `dev` is the primary branch used for active development, deployment, and release tagging.
* Releases are tagged directly off `dev`.
* New branches should be prefixed with descriptors such as `fix/`, `feature/`, `demo/`, etc.
* Delete feature branches immediately after they are merged.

### 3.2 Code Review Guidelines

* Every pull request must receive at least one approving review from a maintainer who is not the author.
* If multiple developers contributed to a PR's commits, at least one review should come from an uninvolved maintainer.
* Reviewers focus on correctness, readability, security, and alignment with project conventions and docs.
* Authors should aim to keep PRs small and focused; larger changes should be split when possible.
* All tests must pass before a PR is merged.
* Review comments should be constructive and reference specific code lines or documentation.
* Use GitHub suggestions or follow‑up commits for requested changes.

### 3.3 Code Style & Conventions

* Follow the best practices for authoring and maintaining Airflow DAGs outlined in the project `docs/` directory.

## 4. Testing Standards

### 4.1 Unit Testing

* PRs which add new DAGs or tasks must include at least one unit test written with **pytest**.
* New DAGs must pass the existing tests, which validate the DAG structure and task dependencies.
* Infrastructure changes must run `terraform validate` and succeed before merge.
* The project currently has no formal coverage target, but contributors are encouraged to expand coverage wherever practical.

## 4.2 Integration Testing

* DAGs are automatically tested for structural correctness and import validity.
* The `dev` or `sit` environments should be used for manual integration tests against the dev environment from `veda-backend`

## 5. Environments & Deployment

### 5.1 Environments

* The `dev` environment is used for development and continuous deployment.
* The `sit` environment is used for integration testing, where changes might interfere with continuous changes being made to the dev environment.
* Additional "production" environments are deployed using [veda-deploy](https://github.com/NASA-IMPACT/veda-deploy) and are not managed by this project.

### 5.2 Deployment Procedures

* The `dev` environment is continuously deployed using GitHub Actions. Manual deployments are possible, but not recommended.
* The `sit` environment is manually deployed using `make sm2a-deploy`. This requires updating the Makefile with a secret name corresponding to the live environment (currently `veda-sm2a-sit-deployment-secrets`). 

### 5.3. Releases

* Releases are tagged directly off `dev` and are automatically deployed to the `dev` environment.

### 5.4 Secrets Handling

* Updates to secrets should be made in AWS secrets manager, which will automatically update environment for subsequent deployments.

## 6 Architecture Decision Records

* Architecture decisions are documented in the veda-architecture [repository](https://github.com/NASA-IMPACT/veda-architecture).

## 7. Change Log & Versioning

- Does not exist yet, but should be automated. To support this, we should use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) to generate a changelog.
- The changelog should be generated automatically using semantic-release.
- To help maintain a clean history, PRs should be rebased and squashed to consolidate unneeded commits before merging.
