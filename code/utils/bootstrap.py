# Databricks bootstrap helpers.
#
# These functions make it easier for notebooks in different folders to import
# shared code (like `utils.config`) whether they're run in Databricks Repos
# or locally for static checks.

from __future__ import annotations

import os
import sys
from typing import Optional


def get_repo_root(dbutils: Optional[object] = None) -> str:
    """
    Best-effort detection of the repo root in Databricks Repos.
    Falls back to CWD when not running in Databricks.
    """
    if dbutils is not None:
        try:
            notebook_path = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .notebookPath()
                .get()
            )
            # Notebooks live in `code/<layer>/...`; repo root is two levels up.
            repo_root = "/Workspace" + "/".join(notebook_path.split("/")[:-2])
            return repo_root
        except Exception:
            pass
    return os.getcwd()


def ensure_code_on_path(dbutils: Optional[object] = None) -> str:
    """
    Ensure `<repo_root>/code` is on sys.path so `from utils...` imports work.
    Returns the detected repo root.
    """
    repo_root = get_repo_root(dbutils=dbutils)
    code_path = os.path.join(repo_root, "code")
    if code_path not in sys.path:
        sys.path.insert(0, code_path)
    return repo_root

