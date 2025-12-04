import os
import re
import shutil
import PyPDF2
import psycopg2 # Der Treiber für PostgreSQL

# --- ANPASSUNGEN HIER VORNEHMEN ---

# 1. Der oberste Ordner, in dem die Suche gestartet werden soll.
ORDNER_PFAD = '/media/texte/07 Mitarbeiter/Matthias/HR/Lohn/HV Niklas/'

# 2. Zentraler Archivordner
ARCHIV_PFAD = '/media/texte/07 Mitarbeiter/Matthias/HR/Lohn/HV Niklas/Archiv'


# 3. PostgreSQL-Datenbankverbindung
DB_CONFIG = {
    'dbname': 'gehälter',
    'user': 'gehälter',
    'password': 'hvngehälter2025',
    'host': '192.168.1.200',  # oder die IP-Adresse deines DB-Servers
    'port': '5432'
}

# --- ENDE DER ANPASSUNGEN ---

# +++ NEUES, PRÄZISES REGEX +++
muster = re.compile(r".*?-(\d{6})-.*\.pdf")

DEUTSCHE_MONATE = ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni', 'Juli', 'August', 'September', 'Oktober', 'November', 'Dezember']
MONAT_VARIANTEN = {'jan': 'Januar', 'feb': 'Februar', 'mär': 'März', 'apr': 'April', 'mai': 'Mai', 'jun': 'Juni', 'jul': 'Juli', 'aug': 'August', 'sep': 'September', 'okt': 'Oktober', 'nov': 'November', 'dez': 'Dezember'}

# ... (die anderen Funktionen bleiben unverändert) ...
def extrahiere_monat_jahr_aus_pdf(dateipfad):
    try:
        with open(dateipfad, 'rb') as datei:
            pdf_reader = PyPDF2.PdfReader(datei)
            if not pdf_reader.pages: return None, None
            text = pdf_reader.pages[0].extract_text()
            for zeile in text.split('\n'):
                jahr_match = re.search(r'\b(202[0-9]|203[0-9])\b', zeile)
                if jahr_match:
                    jahr = int(jahr_match.group(1))
                    for monat in DEUTSCHE_MONATE:
                        if monat.lower() in zeile.lower(): return monat, jahr
                    for abk, vollname in MONAT_VARIANTEN.items():
                        if abk in zeile.lower(): return vollname, jahr
    except Exception as e: print(f"Fehler beim Lesen der PDF {dateipfad}: {e}")
    return None, None

def prüfe_entgeltabrechnung_im_inhalt(dateipfad):
    try:
        with open(dateipfad, 'rb') as datei:
            pdf_reader = PyPDF2.PdfReader(datei)
            if not pdf_reader.pages: return False
            text = pdf_reader.pages[0].extract_text()
            for zeile in text.split('\n')[:10]:
                if "Entgeltabrechnung" in zeile: return True
    except Exception as e: print(f"Fehler beim Prüfen der PDF {dateipfad}: {e}")
    return False

def get_mitarbeiter_id(cursor, personalnummer):
    try:
        cursor.execute("SELECT id FROM mitarbeiter WHERE name_im_dateinamen = %s", (personalnummer,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            print(f"Warnung: Mitarbeiter mit Personalnummer '{personalnummer}' nicht in der DB gefunden. Überspringe Datei.")
            return None
    except Exception as e:
        print(f"DB-Fehler bei der Suche nach Personalnummer '{personalnummer}': {e}")
        return None

def archiviere_datei(quellpfad, monat, jahr):
    try:
        if not monat or not jahr:
             # Fallback, falls Monat/Jahr nicht extrahiert werden konnten
            monat, jahr = "Unsortiert", ""
        unterordner = os.path.join(ARCHIV_PFAD, str(jahr), monat)
        os.makedirs(unterordner, exist_ok=True)
        dateiname = os.path.basename(quellpfad)
        zielpfad = os.path.join(unterordner, dateiname)
        shutil.move(quellpfad, zielpfad)
        print(f"Datei erfolgreich nach '{zielpfad}' archiviert.")
        return zielpfad
    except Exception as e:
        print(f"FEHLER beim Archivieren der Datei {quellpfad}: {e}")
        return None

def finde_und_speichere_abrechnungen():
    conn = None
    try:
        print("Verbinde mit PostgreSQL-Datenbank...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("Verbindung erfolgreich.")
    except Exception as e:
        print(f"KRITISCHER FEHLER: Konnte keine Verbindung zur Datenbank herstellen: {e}")
        return

    with conn:
        with conn.cursor() as cursor:
            for ordner, unterordner, dateien in os.walk(ORDNER_PFAD):
                if os.path.basename(ordner).lower() == 'archiv':
                    unterordner[:] = [] # Verhindert das Betreten von 'archiv'-Ordnern
                    continue

                for datei in dateien:
                    # Schritt 1: Passt die Datei auf das Namensmuster?
                    match = muster.match(datei)
                    if not match:
                        continue # Nächste Datei, wenn Muster nicht passt

                    personalnummer = match.group(1)
                    voller_pfad = os.path.join(ordner, datei)

                    # Schritt 2: Ist es eine Entgeltabrechnung (Inhaltsprüfung)?
                    if not prüfe_entgeltabrechnung_im_inhalt(voller_pfad):
                        print(f"Info: Schlüsselwort nicht im Inhalt gefunden, überspringe: {datei}")
                        continue
                        
                    print(f"\nGefunden: {datei} -> Personalnummer: {personalnummer}")

                    # Schritt 3: Gibt es den Mitarbeiter in der DB?
                    mitarbeiter_id = get_mitarbeiter_id(cursor, personalnummer)
                    if not mitarbeiter_id:
                        continue

                    # Schritt 4: Daten extrahieren und speichern
                    monat, jahr = extrahiere_monat_jahr_aus_pdf(voller_pfad)
                    try:
                        with open(voller_pfad, 'rb') as f:
                            pdf_inhalt = f.read()
                    except Exception as e:
                        print(f"Fehler beim Einlesen der Datei {voller_pfad}: {e}")
                        continue

                    # Schritt 5: In Datenbank einfügen
                    try:
                        cursor.execute(
                            """
                            INSERT INTO abrechnungen (mitarbeiter_id, monat, jahr, pdf_inhalt, dateiname_original, status)
                            VALUES (%s, %s, %s, %s, %s, 'neu') RETURNING id;
                            """, (mitarbeiter_id, monat, jahr, pdf_inhalt, datei)
                        )
                        neue_id = cursor.fetchone()[0]
                        print(f"Abrechnung erfolgreich in DB gespeichert (ID: {neue_id}).")
                        
                        archivpfad = archiviere_datei(voller_pfad, monat, jahr)
                        if archivpfad:
                            cursor.execute("UPDATE abrechnungen SET archivpfad = %s WHERE id = %s", (archivpfad, neue_id))
                        
                    except psycopg2.IntegrityError:
                        print(f"Info: Diese Abrechnung (ID {mitarbeiter_id}, {monat} {jahr}) existiert bereits in der DB. Archiviere trotzdem.")
                        archiviere_datei(voller_pfad, monat, jahr)
                        conn.rollback() # Wichtig: die fehlgeschlagene INSERT-Transaktion zurückrollen
                    except Exception as e:
                        print(f"FEHLER beim Speichern in der DB für Datei {datei}: {e}")
                        conn.rollback()
    
    if conn: conn.close()
    print("\nScan-Vorgang abgeschlossen.")


if __name__ == "__main__":
    try:
        import PyPDF2, psycopg2
    except ImportError as e:
        modul_name = str(e).split("'")[1]
        print(f"Fehler: Das Modul '{modul_name}' ist nicht installiert.")
        exit(1)
    
    finde_und_speichere_abrechnungen()
