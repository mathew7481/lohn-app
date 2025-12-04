import smtplib
import psycopg2
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# --- ANPASSUNGEN HIER VORNEHMEN ---

# 1. Pfad zu deiner HTML-Signaturdatei
SIGNATUR_HTML_PFAD = '/media/texte/07 Mitarbeiter/Matthias/Signatur.html'

# 2. E-Mail-Server-Einstellungen (SMTP)
SMTP_SERVER = 'mx.hv-niklas.com'
SMTP_PORT = 587
SMTP_BENUTZER = 'matthias.niklas@hv-niklas.com'
SMTP_PASSWORT = 'M.n1kla5' # BITTE ANPASSEN

# 3. PostgreSQL-Datenbankverbindung
DB_CONFIG = {
    'dbname': 'gehälter',
    'user': 'gehälter',
    'password': 'hvngehälter2025',
    'host': '192.168.1.200',  # oder die IP-Adresse deines DB-Servers
    'port': '5432'
}

# 4. Betreff und TEXT-Teil der E-Mail (HTML-Teil kommt aus der Datei)
EMAIL_BETREFF = 'Deine Entgeltabrechnung {periode}'
EMAIL_TEXT_FALLBACK = """
Hallo {name},

im Anhang findest du deine Entgeltabrechnung{periode}.

"""

# --- ENDE DER ANPASSUNGEN ---


def lade_html_signatur():
    """Lädt die HTML-Signatur aus einer Datei."""
    try:
        with open(SIGNATUR_HTML_PFAD, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"FEHLER: Die Signaturdatei unter '{SIGNATUR_HTML_PFAD}' wurde nicht gefunden.")
        return None
    except Exception as e:
        print(f"FEHLER beim Lesen der Signaturdatei: {e}")
        return None


def sende_neue_abrechnungen():
    """Holt neue Abrechnungen aus der DB, versendet sie und aktualisiert den Status."""

    # Lade die HTML-Signatur einmal zu Beginn
    html_signatur = lade_html_signatur()
    if not html_signatur:
        print("Abbruch des Skripts, da die HTML-Signatur nicht geladen werden konnte.")
        return

    conn = None
    try:
        # Baue eine Verbindung zur Datenbank auf
        print("Verbinde mit der PostgreSQL-Datenbank...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("Verbindung erfolgreich.")
    except Exception as e:
        print(f"KRITISCHER FEHLER: Konnte keine Verbindung zur Datenbank herstellen: {e}")
        return

    # SQL-Abfrage, um alle neuen Abrechnungen mit den zugehörigen Mitarbeiterdaten zu holen
    sql_query = """
        SELECT
            a.id,
            a.pdf_inhalt,
            a.dateiname_original,
            a.monat,
            a.jahr,
            m.voller_name,
            m.email
        FROM
            abrechnungen AS a
        JOIN
            mitarbeiter AS m ON a.mitarbeiter_id = m.id
        WHERE
            a.status = 'neu';
    """

    abrechnungen_zum_senden = []
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            abrechnungen_zum_senden = cursor.fetchall()
    except Exception as e:
        print(f"Fehler beim Abrufen der Abrechnungen aus der DB: {e}")
        conn.close()
        return

    if not abrechnungen_zum_senden:
        print("Keine neuen Abrechnungen zum Versenden gefunden.")
        conn.close()
        return

    print(f"{len(abrechnungen_zum_senden)} neue Abrechnung(en) gefunden. Starte Versand...")

    # Verbinde mit dem SMTP-Server
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_BENUTZER, SMTP_PASSWORT)
            print("SMTP-Login erfolgreich.")

            # Gehe jede Abrechnung durch
            for abrechnung in abrechnungen_zum_senden:
                (ab_id, pdf_inhalt, dateiname, monat, jahr, voller_name, email) = abrechnung
                
                print(f"\nVerarbeite Abrechnung ID: {ab_id} für {voller_name}...")

                periode_text = f" für {monat} {jahr}" if monat and jahr else ""

                # --- E-Mail zusammenbauen ---
                msg = MIMEMultipart('mixed')
                msg['From'] = SMTP_BENUTZER
                msg['To'] = email
                msg['Subject'] = EMAIL_BETREFF.format(periode=periode_text)

                html_body = f"""
                <html><body>
                <p>Hallo,</p>
                <p>im Anhang findest du deine Entgeltabrechnung {periode_text}.</p>
                <p></p><br>
                {html_signatur}
                </body></html>"""
                
                msg_alternative = MIMEMultipart('alternative')
                msg.attach(msg_alternative)
                msg_alternative.attach(MIMEText(EMAIL_TEXT_FALLBACK.format(name=voller_name, periode=periode_text), 'plain', 'utf-8'))
                msg_alternative.attach(MIMEText(html_body, 'html', 'utf-8'))

                # PDF-Anhang aus den Datenbank-Bytes erstellen
                anhang = MIMEApplication(pdf_inhalt, _subtype="pdf")
                anhang.add_header('Content-Disposition', 'attachment', filename=dateiname)
                msg.attach(anhang)

                # --- Senden und Status aktualisieren ---
                neuer_status = 'fehler'
                try:
                    server.send_message(msg)
                    print(f"E-Mail an {email} erfolgreich gesendet.")
                    neuer_status = 'gesendet'
                except Exception as e:
                    print(f"FEHLER beim Senden der E-Mail an {email}: {e}")
                
                # Update den Status in der Datenbank
                try:
                    with conn.cursor() as cursor:
                        update_sql = "UPDATE abrechnungen SET status = %s, gesendet_am = CURRENT_TIMESTAMP WHERE id = %s"
                        cursor.execute(update_sql, (neuer_status, ab_id))
                    conn.commit() # Speichere die Änderung für diese eine Abrechnung
                    print(f"Status für Abrechnung ID {ab_id} auf '{neuer_status}' gesetzt.")
                except Exception as e:
                    print(f"KRITISCHER DB-FEHLER beim Status-Update für ID {ab_id}: {e}")
                    conn.rollback() # Mache die Änderung rückgängig

    except Exception as e:
        print(f"Ein schwerwiegender SMTP-Fehler ist aufgetreten: {e}")
    finally:
        if conn:
            conn.close()
            print("\nDatenbankverbindung geschlossen. Mailer-Vorgang beendet.")


if __name__ == "__main__":
    try:
        import psycopg2
    except ImportError:
        print("Fehler: Das Modul 'psycopg2' ist nicht installiert.")
        print("Bitte installiere es mit: pip install psycopg2-binary")
        exit(1)
        
    sende_neue_abrechnungen()
