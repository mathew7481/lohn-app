# lohn-app

Dieses Projekt scannt Lohnabrechnungen als PDF, prüft Inhalt und Dateinamensmuster, schreibt die Daten in PostgreSQL und archiviert die Dateien anschließend.

## Voraussetzungen

- Python 3.10+
- PostgreSQL erreichbar (direkt oder via SSH)

Pakete installieren:

```bash
pip install pypdf2 psycopg2-binary python-dotenv sshtunnel
```

## Konfiguration über `.env`

Lege im Projektordner eine Datei `.env` an (Vorlage: `.env.example`).

### Allgemein

- `SCAN_ROOT_PATH` – Wurzelordner für den Scan
- `ARCHIVE_PATH` – Zielordner für Archivierung
- `FILE_PATTERN` – Regex für Dateinamen (Default: `.*?-(\d{6})-.*\.pdf`)

Pfade sind plattformübergreifend nutzbar:

- Linux/macOS z. B. `~/lohn/scanordner`
- Windows z. B. `%USERPROFILE%\\lohn\\scanordner` oder `C:/Users/<Name>/lohn/scanordner`
- `~` und Umgebungsvariablen werden automatisch aufgelöst.

### Mailer

- `SIGNATURE_HTML_PATH`
- `SMTP_SERVER`
- `SMTP_PORT` (Default: `587`)
- `SMTP_USER`
- `SMTP_PASSWORD`
- `SMTP_USE_STARTTLS` (`true`/`false`, Default: `true`)
- `EMAIL_SUBJECT_TEMPLATE` (Default: `Ihre Entgeltabrechnung{periode}`)
- `EMAIL_TEXT_FALLBACK` (Plaintext-Text mit Platzhaltern `{name}` und `{periode}`)

### PostgreSQL

- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `DB_HOST` (Default: `127.0.0.1`)
- `DB_PORT` (Default: `5432`)

### SSH-Tunnel (optional)

- `USE_SSH_TUNNEL` – `true`/`false`
- `SSH_HOST`
- `SSH_PORT` (Default: `22`)
- `SSH_USER`
- `SSH_PASSWORD` **oder** `SSH_PKEY`
- `SSH_PKEY_PASSWORD` (optional)
- `SSH_REMOTE_BIND_HOST` (Default: `DB_HOST`)
- `SSH_REMOTE_BIND_PORT` (Default: `DB_PORT`)
- `SSH_LOCAL_BIND_HOST` (Default: `127.0.0.1`)
- `SSH_LOCAL_BIND_PORT` (Default: `0` = dynamisch)

## Workflow

### 1) Scanner ausführen

Der Scanner sucht PDFs, prüft den Inhalt, schreibt Datensätze in PostgreSQL und archiviert Dateien.

```bash
python scanner.py
```

### 2) Mailer ausführen

Der Mailer holt alle Abrechnungen mit Status `neu`, versendet sie per SMTP und setzt den Status auf `gesendet` oder `fehler`.

```bash
python mailer.py
```

### Empfohlene Reihenfolge

1. `python scanner.py`
2. `python mailer.py`

## Hinweise

- Der Scanner überspringt Ordner mit Namen `archiv` rekursiv.
- Gefundene PDFs werden nur verarbeitet, wenn im Inhalt der ersten Seite `Entgeltabrechnung` vorkommt.
- Bei Duplicate-Fehlern (`IntegrityError`) wird die Datei trotzdem archiviert.
- Der Mailer verarbeitet ausschließlich Datensätze mit Status `neu`.