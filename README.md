# StatecraftAI Maps

Transform raw election data into beautiful, interactive web maps with comprehensive demographic analysis in just a few commands.

[![CodeQL](https://github.com/StatecraftAI/maps/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/StatecraftAI/maps/actions/workflows/github-code-scanning/codeql)     [![Deploy static content to Pages](https://github.com/StatecraftAI/maps/actions/workflows/static.yml/badge.svg)](https://github.com/StatecraftAI/maps/actions/workflows/static.yml)

## Quick Start

## 1. Environment Variables

1. After installing 1Password CLI and setting up dev vault, run `op inject -i .env_template -o .env`
2. Confirm overwrite of existing `.env` -- **this is destructive!**

## 2. Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pre-commit
pre-commit clean
pre-commit install
pre-commit run --all-files
ops/setup_tools.sh
```

## License

Copyright 2025 StatecraftAI. All rights reserved. See [LICENSE.md](LICENSE.md) for details.
