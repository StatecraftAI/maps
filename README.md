# StatecraftAI Maps - True One-Stop Shop 🎯

**ONE directory. ONE command. ALL your political data.**

Transform raw election data into beautiful Supabase PostGIS datasets ready for mapping and analysis in just a few commands.

[![CodeQL](https://github.com/StatecraftAI/maps/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/StatecraftAI/maps/actions/workflows/github-code-scanning/codeql)     [![Deploy static content to Pages](https://github.com/StatecraftAI/maps/actions/workflows/static.yml/badge.svg)](https://github.com/StatecraftAI/maps/actions/workflows/static.yml)

## 🚀 Quick Start (The ONLY Way)

### 1. Setup Environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Your Data

```bash
cd processing/
# Edit config.yaml with your file paths
```

### 3. Run Everything

```bash
cd processing/
python run_all_data_pipeline.py
```

**That's it!** Your data is now in Supabase PostGIS ready for analysis.

## 📁 The One-Stop Shop Structure

```
processing/     ← EVERYTHING IS HERE
  ├── config.yaml              ← Your settings (19 lines)
  ├── run_all_data_pipeline.py ← The complete pipeline
  ├── prepare_election_data.py  ← Election analysis
  ├── prepare_households_data.py ← Demographics
  ├── prepare_voterfile_data.py ← Voter registration
  ├── geo_upload.py            ← Universal uploader
  └── supabase_integration.py  ← Database magic
```

**Total**: 2,506 lines of focused, working code. **NO** complex abstractions.

## 🎯 MVP Philosophy Achieved

- ✅ **Less, smarter code** - Not thousands of lines of abstraction
- ✅ **One directory** - True consolidation
- ✅ **Simple imports** - No cross-directory complexity
- ✅ **Immediate value** - Working pipeline, not framework

## License

Copyright 2025 StatecraftAI. All rights reserved. See [LICENSE.md](LICENSE.md) for details.
