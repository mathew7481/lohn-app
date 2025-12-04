import email
import sys

print("--- Python Such-Pfade (sys.path) ---")
for path in sys.path:
    print(path)

print("\n--- Speicherort des 'email'-Moduls ---")
try:
    # __file__ zeigt auf den Ort des Moduls (entweder email.py oder __init__.py in einem Ordner)
    print(email.__file__)
except AttributeError:
    print("Das geladene 'email'-Modul hat kein '__file__'-Attribut. Das ist sehr ungewöhnlich.")
    print("Das Modul ist: ", email)
