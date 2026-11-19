import os
import re
import shutil

import PyPDF2
import psycopg
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder


DEUTSCHE_MONATE = [
    "Januar",
    "Februar",
    "März",
    "April",
    "Mai",
    "Juni",
    "Juli",
    "August",
    "September",
    "Oktober",
    "November",
    "Dezember",
]

MONAT_VARIANTEN = {
    "jan": "Januar",
    "feb": "Februar",
    "mär": "März",
    "apr": "April",
    "mai": "Mai",
    "jun": "Juni",
    "jul": "Juli",
    "aug": "August",
    "sep": "September",
    "okt": "Oktober",
    "nov": "November",
    "dez": "Dezember",
}


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

    file_pattern = os.getenv("FILE_PATTERN", r".*?-(\d{6})-.*\.pdf")
    config = {
        "ordner_pfad": _normalize_path(os.getenv("SCAN_ROOT_PATH")),
        "archiv_pfad": _normalize_path(os.getenv("ARCHIVE_PATH")),
        "muster": re.compile(file_pattern),
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
        "SCAN_ROOT_PATH": config["ordner_pfad"],
        "ARCHIVE_PATH": config["archiv_pfad"],
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


def extrahiere_monat_jahr_aus_pdf(dateipfad):
    try:
        with open(dateipfad, "rb") as datei:
            pdf_reader = PyPDF2.PdfReader(datei)
            if not pdf_reader.pages:
                return None, None
            text = pdf_reader.pages[0].extract_text() or ""
            for zeile in text.split("\n"):
                jahr_match = re.search(r"\b(202[0-9]|203[0-9])\b", zeile)
                if jahr_match:
                    jahr = int(jahr_match.group(1))
                    for monat in DEUTSCHE_MONATE:
                        if monat.lower() in zeile.lower():
                            return monat, jahr
                    for abk, vollname in MONAT_VARIANTEN.items():
                        if abk in zeile.lower():
                            return vollname, jahr
    except Exception as e:
        print(f"Fehler beim Lesen der PDF {dateipfad}: {e}")
    return None, None


def pruefe_entgeltabrechnung_im_inhalt(dateipfad):
    try:
        with open(dateipfad, "rb") as datei:
            pdf_reader = PyPDF2.PdfReader(datei)
            if not pdf_reader.pages:
                return False
            text = pdf_reader.pages[0].extract_text() or ""
            for zeile in text.split("\n")[:10]:
                if "entgeltabrechnung" in zeile.lower():
                    return True
    except Exception as e:
        print(f"Fehler beim Prüfen der PDF {dateipfad}: {e}")
    return False


def get_mitarbeiter_id(cursor, personalnummer):
    try:
        cursor.execute("SELECT id FROM mitarbeiter WHERE name_im_dateinamen = %s", (personalnummer,))
        result = cursor.fetchone()
        if result:
            return result[0]
        print(
            f"Warnung: Mitarbeiter mit Personalnummer '{personalnummer}' nicht in der DB gefunden. Überspringe Datei."
        )
        return None
    except Exception as e:
        print(f"DB-Fehler bei der Suche nach Personalnummer '{personalnummer}': {e}")
        return None


def archiviere_datei(quellpfad, monat, jahr, archiv_pfad):
    try:
        if not monat or not jahr:
            monat, jahr = "Unsortiert", ""
        unterordner = os.path.join(archiv_pfad, str(jahr), monat)
        os.makedirs(unterordner, exist_ok=True)
        dateiname = os.path.basename(quellpfad)
        zielpfad = os.path.join(unterordner, dateiname)
        shutil.move(quellpfad, zielpfad)
        print(f"Datei erfolgreich nach '{zielpfad}' archiviert.")
        return zielpfad
    except Exception as e:
        print(f"FEHLER beim Archivieren der Datei {quellpfad}: {e}")
        return None


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
    conn = psycopg.connect(**db_config)
    print("Verbindung erfolgreich.")
    return conn, tunnel


def finde_und_speichere_abrechnungen():
    try:
        config = lade_konfiguration()
    except Exception as e:
        print(f"Konfigurationsfehler: {e}")
        return

    conn = None
    tunnel = None
    try:
        conn, tunnel = _verbinde_db(config)
    except Exception as e:
        print(f"KRITISCHER FEHLER: Konnte keine Verbindung zur Datenbank herstellen: {e}")
        return

    try:
        with conn:
            with conn.cursor() as cursor:
                for ordner, unterordner, dateien in os.walk(config["ordner_pfad"]):
                    if os.path.basename(ordner).lower() == "archiv":
                        unterordner[:] = []
                        continue

                    for datei in dateien:
                        match = config["muster"].match(datei)
                        if not match:
                            continue

                        personalnummer = match.group(1)
                        voller_pfad = os.path.join(ordner, datei)

                        if not pruefe_entgeltabrechnung_im_inhalt(voller_pfad):
                            print(f"Info: Schlüsselwort nicht im Inhalt gefunden, überspringe: {datei}")
                            continue

                        print(f"\nGefunden: {datei} -> Personalnummer: {personalnummer}")

                        mitarbeiter_id = get_mitarbeiter_id(cursor, personalnummer)
                        if not mitarbeiter_id:
                            continue

                        monat, jahr = extrahiere_monat_jahr_aus_pdf(voller_pfad)
                        try:
                            with open(voller_pfad, "rb") as f:
                                pdf_inhalt = f.read()
                        except Exception as e:
                            print(f"Fehler beim Einlesen der Datei {voller_pfad}: {e}")
                            continue

                        try:
                            cursor.execute(
                                """
                                INSERT INTO abrechnungen (mitarbeiter_id, monat, jahr, pdf_inhalt, dateiname_original, status)
                                VALUES (%s, %s, %s, %s, %s, 'neu') RETURNING id;
                                """,
                                (mitarbeiter_id, monat, jahr, pdf_inhalt, datei),
                            )
                            neue_id = cursor.fetchone()[0]
                            print(f"Abrechnung erfolgreich in DB gespeichert (ID: {neue_id}).")

                            archivpfad = archiviere_datei(voller_pfad, monat, jahr, config["archiv_pfad"])
                            if archivpfad:
                                cursor.execute(
                                    "UPDATE abrechnungen SET archivpfad = %s WHERE id = %s",
                                    (archivpfad, neue_id),
                                )

                        except psycopg.IntegrityError:
                            print(
                                f"Info: Diese Abrechnung (ID {mitarbeiter_id}, {monat} {jahr}) existiert bereits in der DB. Archiviere trotzdem."
                            )
                            archiviere_datei(voller_pfad, monat, jahr, config["archiv_pfad"])
                            conn.rollback()
                        except Exception as e:
                            print(f"FEHLER beim Speichern in der DB für Datei {datei}: {e}")
                            conn.rollback()
    finally:
        if conn:
            conn.close()
        if tunnel:
            tunnel.stop()
            print("SSH-Tunnel geschlossen.")

    print("\nScan-Vorgang abgeschlossen.")


if __name__ == "__main__":
    try:
        import PyPDF2  # noqa: F401
        import psycopg  # noqa: F401
        from dotenv import load_dotenv  # noqa: F401
        from sshtunnel import SSHTunnelForwarder  # noqa: F401
    except ImportError as e:
        modul_name = str(e).split("'")[1]
        print(f"Fehler: Das Modul '{modul_name}' ist nicht installiert.")
        print("Bitte installieren: pip install -r requirements.txt")
        raise SystemExit(1)

    finde_und_speichere_abrechnungen()
