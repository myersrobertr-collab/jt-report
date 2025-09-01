@echo off
pyinstaller ^
  --noconfirm ^
  --onefile ^
  --windowed ^
  --name "JT Pilot Report" ^
  --add-data "main.py;." ^
  --collect-all streamlit ^
  --collect-all pandas ^
  --collect-all numpy ^
  --collect-all openpyxl ^
  --collect-all xlsxwriter ^
  run_app.py
