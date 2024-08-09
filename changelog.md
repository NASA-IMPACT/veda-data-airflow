# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- Support for multi-asset item ingests ([#86](https://github.com/NASA-IMPACT/veda-data-airflow/pull/86))
- Simple single-asset item ingests supported again ([#87](https://github.com/NASA-IMPACT/veda-data-airflow/pull/87))

### Changed 

- Insert collections via ingestor API rather than directly with pgstac ([#13](https://github.com/NASA-IMPACT/veda-data-airflow/pull/13))
- The `vector` and `cogify` fields are now optional in the payload