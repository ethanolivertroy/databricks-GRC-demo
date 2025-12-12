# Authoritative Framework Sources

This demo ships with framework catalogs and crosswalks in `data/frameworks/`. NIST content is authoritative and redistributable. SOC2, ISO 27001, and PCI‑DSS files are intentionally **synthetic/simplified** to respect licensing, so their coverage and wording are not audit‑grade. Replace any file with your own licensed extract if you need full fidelity.

## NIST 800-53 Rev. 5 (authoritative, redistributable)

- **Source**: NIST Computer Security Resource Center (CSRC) control catalog.
- **License**: U.S. Government work / public domain.
- **How to refresh**: Download the official 800‑53 Rev 5 control spreadsheet/JSON from CSRC and export to a CSV with columns matching `nist_800_53_rev5_controls.csv`.

## SOC 2 Trust Services Criteria 2017 (copyrighted)

- **Source**: AICPA Trust Services Criteria.
- **License**: Copyright AICPA; full text is not freely redistributable.
- **Demo stance**: `soc2_tsc_2017.csv` is a lightweight synthetic extract (IDs + short names/blurbs). It is only meant to illustrate mapping and rollups. For authoritative use, purchase the TSC and export an internal CSV keeping the same column names.

## ISO/IEC 27001 Annex A Controls (copyrighted)

- **Source**: ISO/IEC 27001 standard (Annex A controls, 2013/2022).
- **License**: Copyright ISO/IEC; paywalled.
- **Demo stance**: `iso_27001_controls.csv` is a minimal synthetic subset for demo purposes. Replace with your licensed extract if required.

## PCI‑DSS v4.0 Requirements (copyrighted but publicly available)

- **Source**: PCI Security Standards Council (PCI SSC) PCI‑DSS v4.0 standard.
- **License**: Copyright PCI SSC; public download but redistribution of full text may be restricted.
- **Demo stance**: `pci_dss_v4_controls.csv` contains IDs and short names only; requirement text is omitted. If your org permits, replace with a richer internal extract.

## Cross‑Framework Mappings

- `nist_to_soc2_mapping.csv`, `nist_to_iso_mapping.csv`, `nist_to_pci_mapping.csv` are **illustrative crosswalks** for demo purposes and are not authoritative.
- For audit‑grade mappings, substitute your own crosswalks from authoritative internal sources or vetted publications, keeping the generic mapping schema:
  `mapping_id, source_framework, source_control_id, target_framework, target_control_id, mapping_type, mapping_notes`.
