import os
import shutil
import kagglehub

TARGET_DIR = "../data/raw"  # Cartella relativa al progetto

os.makedirs(TARGET_DIR, exist_ok=True)

print("1. Avvio del download del dataset tramite kagglehub...")
try:
    # Scaricare l'ultima versione del dataset nella cache di kagglehub
    cache_path = kagglehub.dataset_download("hrishitpatil/flight-data-2024")
    print(f"-> Dataset scaricato nella cache temporanea: {cache_path}\n")

    print(f"2. Spostamento dei file in corso verso: {os.path.abspath(TARGET_DIR)}")
    
    # Iteriamo sui file scaricati per spostarli nella cartella target
    for file_name in os.listdir(cache_path):
        source_file = os.path.join(cache_path, file_name)
        destination_file = os.path.join(TARGET_DIR, file_name)
        
        # Gestione del trasferimento (sovrascrive se già presente)
        if os.path.isdir(source_file):
            if os.path.exists(destination_file):
                shutil.rmtree(destination_file)
            shutil.move(source_file, destination_file)
        else:
            shutil.move(source_file, destination_file)
        print(f"   [OK] Spostato: {file_name}")

    print("\n[COMPLETATO] Tutti i file sono pronti nella cartella:", os.path.abspath(TARGET_DIR))

except Exception as e:
    print(f"\n[ERRORE] Si è verificato un problema: {e}")
    print("Verifica che le tue credenziali di Kaggle (token API) siano configurate correttamente sul computer.")