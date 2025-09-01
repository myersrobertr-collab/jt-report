import sys
import os
from pathlib import Path

def _resource_path(rel_path: str) -> str:
    base = getattr(sys, "_MEIPASS", Path(__file__).parent)
    return str(Path(base) / rel_path)

def main():
    app_path = _resource_path("main.py")
    os.environ["STREAMLIT_SERVER_HEADLESS"] = "true"
    os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
    from streamlit.web.bootstrap import run as st_run
    st_run(file=app_path, command_line=None, args=[], flag_options={})

if __name__ == "__main__":
    main()
