import smtplib
import os
import subprocess
import time
import psycopg2
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from config_loader import lade_lokale_umgebungsvariablen

lade_lokale_umgebungsvariablen()


def lese_int_umgebungsvariable(name, standardwert):
    wert = os.getenv(name, str(standardwert))
    try:
        return int(wert)
    except ValueError:
        print(f"Warnung: Ungültiger Wert für {name} ('{wert}'). Verwende {standardwert}.")
        return standardwert


def lese_bool_umgebungsvariable(name, standard=False):
    wert = os.getenv(name, str(standard)).strip().lower()
    return wert in ('1', 'true', 'ja', 'yes', 'on')


def normalisiere_pfad(pfad):
    if not pfad:
        return ''

    benutzerprofil = os.getenv('USERPROFILE', '')
    ersetzt = pfad.replace('$env:USERPROFILE', benutzerprofil)
    ersetzt = os.path.expandvars(ersetzt)
    return os.path.expanduser(ersetzt)

# --- ANPASSUNGEN HIER VORNEHMEN ---
SIGNATUR_HTML_PFAD = os.getenv('SIGNATUR_HTML_PFAD', '')

SMTP_SERVER = os.getenv('SMTP_SERVER', '')
SMTP_PORT = lese_int_umgebungsvariable('SMTP_PORT', 587)
SMTP_BENUTZER = os.getenv('SMTP_BENUTZER', '')
SMTP_PASSWORT = os.getenv('SMTP_PASSWORT', '')

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', ''),
    'user': os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'host': os.getenv('DB_HOST', ''),
    'port': os.getenv('DB_PORT', '')
}

SSH_TUNNEL_AKTIV = lese_bool_umgebungsvariable('SSH_TUNNEL_AKTIV', False)
SSH_HOST = os.getenv('SSH_HOST', '')
SSH_BENUTZER = os.getenv('SSH_BENUTZER', '')
SSH_KEY_PFAD = normalisiere_pfad(os.getenv('SSH_KEY_PFAD', os.path.join('~', '.ssh', 'id_pguser')))
SSH_LOCAL_PORT = lese_int_umgebungsvariable('SSH_LOCAL_PORT', 5432)
SSH_ZIEL_HOST = os.getenv('SSH_ZIEL_HOST', '')
SSH_ZIEL_PORT = lese_int_umgebungsvariable('SSH_ZIEL_PORT', 5432)
SSH_SERVER_ALIVE_INTERVAL = lese_int_umgebungsvariable('SSH_SERVER_ALIVE_INTERVAL', 60)
SSH_SERVER_ALIVE_COUNT_MAX = lese_int_umgebungsvariable('SSH_SERVER_ALIVE_COUNT_MAX', 3)

EMAIL_BETREFF = os.getenv('EMAIL_BETREFF', 'Ihre Entgeltabrechnung{periode}')
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


def starte_ssh_tunnel():
    if not SSH_TUNNEL_AKTIV:
        return None

    fehlende_werte = []
    if not SSH_HOST:
        fehlende_werte.append('SSH_HOST')
    if not SSH_BENUTZER:
        fehlende_werte.append('SSH_BENUTZER')
    if not SSH_ZIEL_HOST:
        fehlende_werte.append('SSH_ZIEL_HOST')
    if not SSH_KEY_PFAD:
        fehlende_werte.append('SSH_KEY_PFAD')

    if fehlende_werte:
        print(f"KRITISCHER FEHLER: Fehlende SSH-Konfiguration: {', '.join(fehlende_werte)}")
        return None

    if not os.path.exists(SSH_KEY_PFAD):
        print(f"KRITISCHER FEHLER: SSH-Key wurde nicht gefunden: {SSH_KEY_PFAD}")
        return None

    befehl = [
        'ssh',
        '-N',
        '-o', f'ServerAliveInterval={SSH_SERVER_ALIVE_INTERVAL}',
        '-o', f'ServerAliveCountMax={SSH_SERVER_ALIVE_COUNT_MAX}',
        '-i', SSH_KEY_PFAD,
        '-L', f'{SSH_LOCAL_PORT}:{SSH_ZIEL_HOST}:{SSH_ZIEL_PORT}',
        f'{SSH_BENUTZER}@{SSH_HOST}'
    ]

    try:
        print(f"Starte SSH-Tunnel auf localhost:{SSH_LOCAL_PORT} ...")
        prozess = subprocess.Popen(
            befehl,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True
        )

        time.sleep(1)
        if prozess.poll() is not None:
            fehlertext = (prozess.stderr.read() or '').strip()
            print(f"KRITISCHER FEHLER: SSH-Tunnel konnte nicht gestartet werden: {fehlertext}")
            return None

        print("SSH-Tunnel aktiv.")
        return prozess
    except FileNotFoundError:
        print("KRITISCHER FEHLER: 'ssh' wurde nicht gefunden. Bitte OpenSSH installieren oder in PATH aufnehmen.")
        return None
    except Exception as e:
        print(f"KRITISCHER FEHLER beim Start des SSH-Tunnels: {e}")
        return None


def stoppe_ssh_tunnel(prozess):
    if not prozess:
        return

    try:
        prozess.terminate()
        prozess.wait(timeout=5)
    except Exception:
        try:
            prozess.kill()
        except Exception:
            pass
    print("SSH-Tunnel beendet.")


def sende_neue_abrechnungen():
    """Holt neue Abrechnungen aus der DB, versendet sie und aktualisiert den Status."""
    conn = None
    ssh_tunnel_prozess = None

    fehlende_smtp_werte = []
    if not SIGNATUR_HTML_PFAD:
        fehlende_smtp_werte.append('SIGNATUR_HTML_PFAD')
    if not SMTP_SERVER:
        fehlende_smtp_werte.append('SMTP_SERVER')
    if not SMTP_BENUTZER:
        fehlende_smtp_werte.append('SMTP_BENUTZER')
    if not SMTP_PASSWORT:
        fehlende_smtp_werte.append('SMTP_PASSWORT')

    db_config = DB_CONFIG.copy()

    if SSH_TUNNEL_AKTIV:
        ssh_tunnel_prozess = starte_ssh_tunnel()
        if not ssh_tunnel_prozess:
            return
        db_config['host'] = 'localhost'
        db_config['port'] = str(SSH_LOCAL_PORT)

    fehlende_db_werte = [schluessel for schluessel, wert in db_config.items() if not wert]

    if fehlende_smtp_werte or fehlende_db_werte:
        fehlend = fehlende_smtp_werte + fehlende_db_werte
        print(f"KRITISCHER FEHLER: Fehlende Konfiguration: {', '.join(fehlend)}")
        stoppe_ssh_tunnel(ssh_tunnel_prozess)
        return

    try:
        html_signatur = lade_html_signatur()
        if not html_signatur:
            print("Keine Signatur gefunden. Abbruch.")
            return

        try:
            print("Verbinde mit PostgreSQL-Datenbank...")
            conn = psycopg2.connect(**db_config)
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
            return

        if not abrechnungen_zum_senden:
            print("Keine neuen Abrechnungen gefunden.")
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
    finally:
        if conn:
            conn.close()
        stoppe_ssh_tunnel(ssh_tunnel_prozess)


if __name__ == "__main__":
    try:
        sende_neue_abrechnungen()
    except Exception as e:
        print(f"Ein unerwarteter Fehler ist aufgetreten: {e}")
