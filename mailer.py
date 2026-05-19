import smtplib
import os
import psycopg2
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

def _as_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "ja", "on"}


def _as_int(value, default):
    if value in (None, ""):
        return default
    return int(value)


def _normalize_path(value):
    if not value:
        return value
    expanded = os.path.expandvars(os.path.expanduser(value))
    return os.path.normpath(os.path.abspath(expanded))


def lade_konfiguration():
    load_dotenv()

    config = {
        "signatur_html_pfad": _normalize_path(os.getenv("SIGNATURE_HTML_PATH")),
        "smtp": {
            "server": os.getenv("SMTP_SERVER"),
            "port": _as_int(os.getenv("SMTP_PORT"), 587),
            "user": os.getenv("SMTP_USER"),
            "password": os.getenv("SMTP_PASSWORD"),
            "use_starttls": _as_bool(os.getenv("SMTP_USE_STARTTLS"), True),
        },
        "mail": {
            "betreff": os.getenv("EMAIL_SUBJECT_TEMPLATE", "Ihre Entgeltabrechnung{periode}"),
            "fallback": os.getenv(
                "EMAIL_TEXT_FALLBACK",
                "Hallo {name},\n\nim Anhang findest du deine Entgeltabrechnung{periode}.\n\n"
                "Bei Fragen stehe ich dir gerne zur Verfügung.\n\nViele Grüße",
            ),
        },
        "db": {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST", "127.0.0.1"),
            "port": _as_int(os.getenv("DB_PORT"), 5432),
        },
        "ssh": {
            "use_tunnel": _as_bool(os.getenv("USE_SSH_TUNNEL"), False),
            "host": os.getenv("SSH_HOST"),
            "port": _as_int(os.getenv("SSH_PORT"), 22),
            "user": os.getenv("SSH_USER"),
            "password": os.getenv("SSH_PASSWORD"),
            "pkey": _normalize_path(os.getenv("SSH_PKEY")),
            "pkey_password": os.getenv("SSH_PKEY_PASSWORD"),
            "remote_bind_host": os.getenv("SSH_REMOTE_BIND_HOST", os.getenv("DB_HOST", "127.0.0.1")),
            "remote_bind_port": _as_int(os.getenv("SSH_REMOTE_BIND_PORT"), _as_int(os.getenv("DB_PORT"), 5432)),
            "local_bind_host": os.getenv("SSH_LOCAL_BIND_HOST", "127.0.0.1"),
            "local_bind_port": _as_int(os.getenv("SSH_LOCAL_BIND_PORT"), 0),
        },
    }

    pflichtfelder = {
        "SIGNATURE_HTML_PATH": config["signatur_html_pfad"],
        "SMTP_SERVER": config["smtp"]["server"],
        "SMTP_USER": config["smtp"]["user"],
        "SMTP_PASSWORD": config["smtp"]["password"],
        "DB_NAME": config["db"]["dbname"],
        "DB_USER": config["db"]["user"],
        "DB_PASSWORD": config["db"]["password"],
    }
    fehlend = [name for name, wert in pflichtfelder.items() if not wert]
    if fehlend:
        raise ValueError("Fehlende .env-Konfiguration: " + ", ".join(fehlend))

    if config["ssh"]["use_tunnel"]:
        ssh_fehler = []
        for feld in ["host", "user"]:
            if not config["ssh"][feld]:
                ssh_fehler.append(f"SSH_{feld.upper()}")
        if not config["ssh"]["password"] and not config["ssh"]["pkey"]:
            ssh_fehler.append("SSH_PASSWORD oder SSH_PKEY")
        if ssh_fehler:
            raise ValueError(
                "USE_SSH_TUNNEL=true, aber folgende Angaben fehlen: " + ", ".join(ssh_fehler)
            )

    return config


def _erstelle_ssh_tunnel(ssh_config):
    tunnel_kwargs = {
        "ssh_address_or_host": (ssh_config["host"], ssh_config["port"]),
        "ssh_username": ssh_config["user"],
        "remote_bind_address": (ssh_config["remote_bind_host"], ssh_config["remote_bind_port"]),
        "local_bind_address": (ssh_config["local_bind_host"], ssh_config["local_bind_port"]),
    }

    if ssh_config["pkey"]:
        tunnel_kwargs["ssh_pkey"] = ssh_config["pkey"]
        if ssh_config["pkey_password"]:
            tunnel_kwargs["ssh_private_key_password"] = ssh_config["pkey_password"]
    else:
        tunnel_kwargs["ssh_password"] = ssh_config["password"]

    tunnel = SSHTunnelForwarder(**tunnel_kwargs)
    tunnel.start()
    return tunnel


def _verbinde_db(config):
    tunnel = None
    db_config = dict(config["db"])

    if config["ssh"]["use_tunnel"]:
        print("Baue SSH-Tunnel zur Datenbank auf...")
        tunnel = _erstelle_ssh_tunnel(config["ssh"])
        db_config["host"] = config["ssh"]["local_bind_host"]
        db_config["port"] = tunnel.local_bind_port
        print(
            f"SSH-Tunnel aktiv: {db_config['host']}:{db_config['port']} -> "
            f"{config['ssh']['remote_bind_host']}:{config['ssh']['remote_bind_port']}"
        )

    print("Verbinde mit PostgreSQL-Datenbank...")
    conn = psycopg2.connect(**db_config)
    print("Verbindung erfolgreich.")
    return conn, tunnel

def lade_html_signatur(signatur_html_pfad):
    """Lädt die HTML-Signatur aus einer Datei."""
    try:
        with open(signatur_html_pfad, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"FEHLER: Die Signaturdatei {signatur_html_pfad} wurde nicht gefunden.")
        return None
    except Exception as e:
        print(f"FEHLER beim Laden der Signaturdatei: {e}")
        return None


def sende_neue_abrechnungen():
    """Holt neue Abrechnungen aus der DB, versendet sie und aktualisiert den Status."""
    try:
        config = lade_konfiguration()
    except Exception as e:
        print(f"Konfigurationsfehler: {e}")
        return

    html_signatur = lade_html_signatur(config["signatur_html_pfad"])
    if not html_signatur:
        print("Keine Signatur gefunden. Abbruch.")
        return

    conn = None
    tunnel = None
    try:
        conn, tunnel = _verbinde_db(config)
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

    with smtplib.SMTP(config["smtp"]["server"], config["smtp"]["port"]) as server:
        if config["smtp"]["use_starttls"]:
            server.starttls()
        server.login(config["smtp"]["user"], config["smtp"]["password"])
        print("SMTP-Login erfolgreich.")

        for abrechnung in abrechnungen_zum_senden:
            ab_id, pdf_inhalt, dateiname, monat, jahr, voller_name, email = abrechnung
            print(f"Verarbeite Abrechnung ID {ab_id} für {voller_name} ({email})...")

            periode = f" für {monat} {jahr}" if monat and jahr else ""

            # E-Mail erstellen
            msg = MIMEMultipart('mixed')
            msg['From'] = config["smtp"]["user"]
            msg['To'] = email
            msg['Subject'] = config["mail"]["betreff"].format(periode=periode)

            html_body = f"""
            <html><body>
            <p>Hallo {voller_name},</p>
            <p>im Anhang findest du deine Entgeltabrechnung{periode}.</p>
            <p>Bei Fragen stehe ich dir gerne zur Verfügung.</p>
            <br>
            {html_signatur}
            </body></html>
            """

            msg.attach(MIMEText(config["mail"]["fallback"].format(name=voller_name, periode=periode), 'plain', 'utf-8'))
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
    if tunnel:
        tunnel.stop()
        print("SSH-Tunnel geschlossen.")


if __name__ == "__main__":
    try:
        sende_neue_abrechnungen()
    except Exception as e:
        print(f"Ein unerwarteter Fehler ist aufgetreten: {e}")
