import smtplib
import psycopg2
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# --- ANPASSUNGEN HIER VORNEHMEN ---
SIGNATUR_HTML_PFAD = '/media/texte/07\ Mitarbeiter/Signatur.html'

SMTP_SERVER = 'mx.niklas-niklas.com'
SMTP_PORT = 587
SMTP_BENUTZER = 'matthias.niklas@hv-niklas.com'
SMTP_PASSWORT = 'M.n1kla5'

DB_CONFIG = {
    'dbname': 'gehälter',
    'user': 'mniklas',
    'password': 'MPfPOSTGRES2023!',
    'host': 'localhost',
    'port': '5432'
}

EMAIL_BETREFF = 'Ihre Entgeltabrechnung{periode}'
EMAIL_TEXT_FALLBACK = """
Hallo {name},

im Anhang findest du deine Entgeltabrechnung{periode}.

Bei Fragen stehe ich dir gerne zur Verfügung.

Viele Grüße
Matthias Niklas
"""
# --- ENDE DER ANPASSUNGEN ---

def lade_html_signatur():
    """Lädt die HTML-Signatur aus einer Datei."""
    try:
        with open(SIGNATUR_HTML_PFAD, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"FEHLER: Die Signaturdatei {SIGNATUR_HTML_PFAD} wurde nicht gefunden.")
        return None
    except Exception as e:
        print(f"FEHLER beim Laden der Signaturdatei: {e}")
        return None


def sende_neue_abrechnungen():
    """Holt neue Abrechnungen aus der DB, versendet sie und aktualisiert den Status."""
    html_signatur = lade_html_signatur()
    if not html_signatur:
        print("Keine Signatur gefunden. Abbruch.")
        return

    conn = None
    try:
        print("Verbinde mit PostgreSQL-Datenbank...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("Verbindung erfolgreich.")
    except Exception as e:
        print(f"FEHLER: Konnte keine Verbindung zur Datenbank herstellen: {e}")
        return

    sql_query = """
        SELECT
            a.id, a.pdf_inhalt, a.dateiname_original, a.monat, a.jahr,
            m.voller_name, m.email
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
        print(f"FEHLER beim Abrufen der Abrechnungen: {e}")
        conn.close()
        return

    if not abrechnungen_zum_senden:
        print("Keine neuen Abrechnungen gefunden.")
        conn.close()
        return

    print(f"{len(abrechnungen_zum_senden)} neue Abrechnungen gefunden: Starte Versand...")

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_BENUTZER, SMTP_PASSWORT)
        print("SMTP-Login erfolgreich.")

        for abrechnung in abrechnungen_zum_senden:
            ab_id, pdf_inhalt, dateiname, monat, jahr, voller_name, email = abrechnung
            print(f"Verarbeite Abrechnung ID {ab_id} für {voller_name} ({email})...")

            periode = f" für {monat} {jahr}" if monat and jahr else ""

            # E-Mail erstellen
            msg = MIMEMultipart('mixed')
            msg['From'] = SMTP_BENUTZER
            msg['To'] = email
            msg['Subject'] = EMAIL_BETREFF.format(periode=periode)

            html_body = f"""
            <html><body>
            <p>Hallo {voller_name},</p>
            <p>im Anhang findest du deine Entgeltabrechnung{periode}.</p>
            <p>Bei Fragen stehe ich dir gerne zur Verfügung.</p>
            <br>
            {html_signatur}
            </body></html>
            """

            msg.attach(MIMEText(EMAIL_TEXT_FALLBACK.format(name=voller_name, periode=periode), 'plain', 'utf-8'))
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))

            # PDF-Anhang
            anhang = MIMEApplication(pdf_inhalt, 'pdf')
            anhang.add_header('Content-Disposition', 'attachment', filename=dateiname)
            msg.attach(anhang)

            try:
                server.send_message(msg)
                print(f"✅ E-Mail an {email} wurde erfolgreich gesendet.")
                neuer_status = 'gesendet'
            except Exception as e:
                print(f"❌ Fehler beim Senden an {email}: {e}")
                neuer_status = 'fehler'

            # Status in der DB aktualisieren
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE abrechnungen SET status = %s, gesendet_am = CURRENT_TIMESTAMP WHERE id = %s",
                        (neuer_status, ab_id)
                    )
                conn.commit()
                print(f"Status in DB aktualisiert: {neuer_status}")
            except Exception as e:
                print(f"FEHLER beim Aktualisieren des Status in der DB für ID {ab_id}: {e}")
                conn.rollback()

    print("Versand abgeschlossen.")
    conn.close()


if __name__ == "__main__":
    try:
        sende_neue_abrechnungen()
    except Exception as e:
        print(f"Ein unerwarteter Fehler ist aufgetreten: {e}")
