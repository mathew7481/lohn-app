import os
from pathlib import Path


def lade_lokale_umgebungsvariablen(dateiname='.env'):
    """Lädt Umgebungsvariablen aus einer lokalen .env-Datei, falls vorhanden."""
    env_pfad = Path(__file__).resolve().parent / dateiname
    if not env_pfad.exists():
        return

    try:
        with open(env_pfad, 'r', encoding='utf-8') as env_datei:
            for zeile in env_datei:
                inhalt = zeile.strip()
                if not inhalt or inhalt.startswith('#') or '=' not in inhalt:
                    continue

                schluessel, wert = inhalt.split('=', 1)
                schluessel = schluessel.strip()
                wert = wert.strip().strip('"').strip("'")

                if schluessel and schluessel not in os.environ:
                    os.environ[schluessel] = wert
    except Exception as e:
        print(f"Warnung: Konnte lokale Umgebungsvariablen aus {env_pfad} nicht laden: {e}")
