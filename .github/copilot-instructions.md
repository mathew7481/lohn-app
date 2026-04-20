# Copilot Instructions für lohn-app

## Projektkontext
- Dieses Repository enthält eine kleine Python-Anwendung für Lohnabrechnungen.
- `scanner.py` sucht PDF-Abrechnungen, prüft Inhalt/Dateiname, schreibt Datensätze in PostgreSQL und archiviert Dateien.
- `mailer.py` lädt neue Datensätze (`status = 'neu'`), versendet E-Mails mit PDF-Anhang und setzt den Versandstatus.

## Technische Leitlinien
- Änderungen klein und gezielt halten; keine großen Refactorings ohne klaren Bedarf.
- Bestehende Namensgebung und Sprache beibehalten (deutsche Bezeichner, deutsche Konsolenausgaben).
- Fehlerbehandlung beibehalten oder verbessern (klare `print`-Fehlermeldungen, keine stillen Fehler).
- Bei Datenbankzugriffen immer Transaktionen korrekt behandeln (`commit`/`rollback`).
- SQL-Statements nur parametrisiert schreiben (keine String-Konkatenation für Query-Werte).

## Sicherheitsregeln (verbindlich)
- Keine Zugangsdaten, Passwörter, Tokens oder personenbezogenen Daten hartkodieren.
- Neue Konfigurationen über Umgebungsvariablen oder eine lokale, nicht versionierte Konfigurationsdatei einlesen.
- Keine echten Beispielwerte für sensible Daten in Commits oder Vorschlägen verwenden.
- Bei Log-Ausgaben sensible Inhalte maskieren (z. B. E-Mail-Adressen nur teilweise anzeigen, keine PDF-Inhalte loggen).

## Verhaltensregeln für Änderungen
- Keine Änderungen am fachlichen Workflow ohne ausdrückliche Anforderung:
  - Scanner: finden → validieren → speichern → archivieren.
  - Mailer: laden → senden → Status setzen.
- Dateipfade und Archivstruktur nur ändern, wenn explizit angefragt.
- Neue Abhängigkeiten nur einführen, wenn sie klaren Mehrwert bieten.

## Code-Stil
- Python-Code einfach und direkt halten, bevorzugt gut benannte Funktionen.
- Keine unnötige Komplexität oder zusätzliche Architektur-Schichten einführen.
- Nur knappe, hilfreiche Kommentare; keine redundanten Kommentare.

## Validierung
- Nach Änderungen mindestens den betroffenen Script-Flow lokal prüfen.
- Wenn möglich, Import- und Syntax-Check sicherstellen.
- Unabhängige, nicht angefragte Probleme im Repository nicht automatisch mitändern.
