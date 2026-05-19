# Copilot Instructions

## Projektkontext

- Dieses Projekt verarbeitet Lohnabrechnungs-PDFs und schreibt Metadaten + PDF-Inhalt in PostgreSQL.
- Der Versand erfolgt über `mailer.py` per SMTP auf Basis der in der Datenbank gespeicherten Abrechnungen.
- Konfiguration erfolgt ausschließlich über `.env`.
- Datenbankzugriff kann direkt oder über SSH-Tunnel (`sshtunnel`) erfolgen.

## Entwicklungsrichtlinien

- Keine Zugangsdaten oder Pfade im Code hardcoden.
- Neue Konfigurationswerte immer in `.env.example` und `README.md` dokumentieren.
- Änderungen in `scanner.py` und `mailer.py` möglichst klein und fokussiert halten.
- Pfadverarbeitung plattformunabhängig umsetzen (Windows/Linux kompatibel).
- Fehlerausgaben klar und deutschsprachig formulieren.
- Bestehende Tabellen- und Spaltennamen unverändert lassen (`mitarbeiter`, `abrechnungen`).
- Beim Mailversand bestehende Statuslogik beibehalten (`neu`, `gesendet`, `fehler`).

## Python-Abhängigkeiten

- `pypdf2`
- `psycopg2-binary`
- `python-dotenv`
- `sshtunnel`

## Qualität

- Bei Änderungen immer kurz auf Syntaxfehler prüfen.
- Keine unnötigen Refactorings außerhalb des angefragten Umfangs.
- Für `mailer.py` besonders auf sichere Nutzung von `.env`-Werten (SMTP/DB/SSH) achten.
