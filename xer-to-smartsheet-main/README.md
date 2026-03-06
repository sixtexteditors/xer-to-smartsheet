# XER → Smartsheet

Import Primavera P6 schedules (XER files) directly into Smartsheet, preserving:
- Activity name, WBS, start, finish, duration
- Dependencies (predecessors with relationship type + lag)
- Resource assignments

## Local dev

```bash
cd backend
pip install -r requirements.txt
python app.py
```
Open http://localhost:5000

## Deploy to Railway

1. Push this repo to GitHub
2. In Railway: New Project → Deploy from GitHub repo → select this repo
3. Railway will auto-detect the Dockerfile and build
4. No environment variables needed (API key is entered by each user in the UI)

## How it works

- User uploads a .xer file and pastes their Smartsheet API key
- Backend parses the XER (no P6 install needed)
- If a sheet with the same name already exists, rows are cleared and re-imported
- If not, a new sheet is created in My Sheets
- A direct link to the sheet is returned on success
