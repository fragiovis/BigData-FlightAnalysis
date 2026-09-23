# ✈️ Flight Big Data Benchmark Pipeline (Local vs Cloud AWS EMR)

Questo progetto implementa una **pipeline automatizzata di benchmark** per l'elaborazione, l'analisi e il data warehousing massivo di dataset relativi ai voli aerei commerciali americani. L'obiettivo accademico e ingegneristico è mettere a confronto le prestazioni computazionali e la scalabilità di tre dei principali motori di calcolo dell'ecosistema Big Data:
* **Spark Core (RDD):** Approccio a basso livello basato su strutture dati resilienti e distribuite (Resilient Distributed Datasets).
* **Spark SQL (DataFrame):** Ottimizzazione dichiarativa di alto livello basata sul motore Catalyst.
* **Apache Hive:** Data Warehousing distribuito che traduce query SQL-like in job MapReduce eseguiti tramite l'engine di nuova generazione **Tez**.

Il confronto viene effettuato mettendo sotto stress i tre framework all'interno di due contesti infrastrutturali diametralmente opposti:
1. **Ambiente Locale Pseudo-Distribuito:** Simulazione di un intero cluster Hadoop (HDFS + YARN) su una singola macchina host con sistema operativo Ubuntu.
2. **Ambiente Cloud Distribuito (AWS EMR):** Cluster elastico reale ad alte prestazioni composto da 1 Master Node e 2 Core Nodes basati su istanze Amazon EC2 `m5.xlarge` (4 vCPU, 16 GB di RAM e storage EBS dedicato).

La suite esegue i medesimi algoritmi di analisi (Job 1 e Job 2) su 5 frazioni progressive del dataset, raccogliendo i tempi di risposta (espressi in secondi) e autogenerando report testuali e grafici comparativi tramite *Matplotlib*.

---

## 📊 Dataset Considerato

Il benchmark si basa sul popolare dataset pubblico **"Flight Delay Dataset — 2024"** disponibile sulla piattaforma Kaggle. Il file raccoglie i record dettagliati di tutti i voli di linea interni degli Stati Uniti, tracciando ritardi, cancellazioni e metriche operative delle compagnie aeree.

* **Link Ufficiale al Dataset:** [Kaggle - Flight Delay Dataset — 2024](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024)
* **Dataset Shape (Matrice dei Dati):**
  * **Numero di Righe (Record):** ~7 milioni di righe nella versione completa (`flights_cleaned.csv`).
  * **Numero di Colonne (Attributi):** 9 features selezionate per l'analisi (`month`, `op_unique_carrier`, `origin`, `dest`, `dep_delay`, `arr_delay`, `cancelled`, `cancellation_code`, `delay_code`).
* **Frazionamento per il Benchmark:** Per valutare la scalabilità in modo analitico, il dataset originale è stato campionato in 5 frazioni progressive caricate su HDFS:
  * `flights_1.csv` (1% del dataset)
  * `flights_20.csv` (20% del dataset)
  * `flights_50.csv` (50% del dataset)
  * `flights_70.csv` (70% del dataset)
  * `flights_cleaned.csv` (100% del dataset)

---

## 🏗️ Struttura della Repository

La cartella del progetto è organizzata in moduli indipendenti e isolati per tecnologia. Questa separazione architetturale consente di mantenere intatti gli script di lancio locali (`run.sh`), introducendo in parallelo i moduli nativi per il Cloud (`run_aws.sh`) per garantire lo switch DevOps senza conflitti:

```text
flight-bigdata-benchmark/
├── dataset/               
│   ├── download.py        # Script Python per il download automatizzato del dataset da Kaggle
│   ├── generate_data.sh   # Script Bash per orchestrare la sequenza di scaricamento e preparazione
│   ├── preprocessing.py   # Logica di pulizia iniziale, rimozione record inconsistenti e selezione feature
│   └── generate_portions.py # Algoritmo di campionamento statistico per generare i file all'1%, 20%, 50%, 70%
├── docs/                  # Documenti di progetto
├── data/                  
│   ├── raw/               # Contiene il file ZIP originario e il CSV grezzo scaricato da Kaggle
│   └── processed/         # Contiene i file finali pronti per HDFS (flights_1.csv, flights_20.csv, ecc.)
├── spark-core/            
│   ├── job_1.py           # Algoritmo di analisi Job 1
│   ├── job_2.py           # Algoritmo di analisi Job 2
│   ├── run.sh             # Script di lancio per PC Locale (Pseudo-Cluster)
│   └── run_aws.sh         # Script nativo ottimizzato per AWS EMR
├── spark-sql/             
│   ├── job_1.py           # Algoritmo di analisi Job 1
│   ├── job_2.py           # Algoritmo di analisi Job 2
│   ├── run.sh             # Script di lancio per PC Locale (Pseudo-Cluster)
│   └── run_aws.sh         # Script nativo ottimizzato per AWS EMR
├── hive/                  
│   ├── job_1.hql          # Query SQL di analisi Job 1
│   ├── job_2.hql          # Query SQL di analisi Job 2
│   ├── run.sh             # Script locale con patch per Java 21 e database Derby
│   └── run_aws.sh         # Script nativo per AWS EMR (connessione a HiveServer2)
├── logs/                  
│   ├── local/             # Log ed esecuzioni in modalità Single Thread locale
│   ├── yarn/              # Log ed esecuzioni in modalità Pseudo-Cluster YARN locale
│   └── aws/               # Risultati, metriche e grafici reali del Cloud Amazon
├── benchmark.py           # Motore Python core per il monitoraggio dei tempi e plot dei grafici
├── experiments.sh         # Orchestratore generale della suite (Accetta parametri: local[*], yarn, aws)
├── requirements.txt       # Dipendenze Python necessarie per la reportistica (Matplotlib, Pandas)
└── setup.sh               # Automazione di boot, pulizia e formattazione del cluster locale
```

## 💻 Configurazione dello Stack Tecnologico sull'Host (Ambiente Locale)

Questa sezione descrive la procedura essenziale per scaricare, configure e avviare l'infrastruttura Big Data in modalità **Pseudo-Distribuita** su sistema operativo Ubuntu, emulando un intero cluster su una singola macchina host.

⚠️ **ATTENZIONE (NOTA CRITICA SUI PERCORSI):** Per garantire il corretto funzionamento degli script automatizzati e l'avvio dei servizi senza errori di permessi negati, tutti i framework devono essere tassativamente scaricati e scompattati all'interno della tua **Home Directory centrale** (`~/`). Non utilizzare sottocartelle come "Scaricati" o "Documenti".

### 1. Framework da Scaricare e Versioni

Esegui questi comandi in sequenza per posizionarti nella tua Home, scaricare i pacchetti stabili e scompattarli nella posizione corretta:

* **Java OpenJDK 21** (Ambiente di runtime fondamentale):
```bash
sudo apt update && sudo apt install openjdk-21-jdk -y
```
* **Apache Hadoop 3.4.1** (Storage HDFS e Gestore Risorse YARN):
```bash
cd ~
wget [https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz](https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz)
tar -xzf hadoop-3.4.1.tar.gz && rm hadoop-3.4.1.tar.gz
  ```
* **Apache Spark 3.5.5** (Motore di calcolo in-memory, pre-built per Hadoop 3):
```bash
cd ~
wget [https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz](https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz)
tar -xzf spark-3.5.5-bin-hadoop3.tgz && rm spark-3.5.5-bin-hadoop3.tgz
```
* **Apache Hive 4.0.0** (Data Warehousing SQL-like locale):
```bash
cd ~
wget [https://archive.apache.org/dist/hive/hive-4.0.0/apache-hive-4.0.0-bin.tar.gz](https://archive.apache.org/dist/hive/hive-4.0.0/apache-hive-4.0.0-bin.tar.gz)
tar -xzf apache-hive-4.0.0-bin.tar.gz && rm apache-hive-4.0.0-bin.tar.gz
```

### 2. Configurazione dell'Ambiente e Automazione File XML

Tutte le altre configurazioni avanzate (come le patch di sicurezza per Java 21, i Classpath e l'iniezione dei parametri in mapred-site.xml) vengono applicate automaticamente a caldo durante l'esecuzione. All'utente è richiesto solo di impostare le variabili e lanciare i comandi di predisposizione dei file XML.

* #### A. Esportazione delle Variabili d'Ambiente (~/.bashrc)
Apri il file ~/.bashrc, aggiungi in fondo i seguenti percorsi per mappare i comandi di sistema e salva:
```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop-3.4.1
export SPARK_HOME=$HOME/spark-3.5.5-bin-hadoop3
export HIVE_HOME=$HOME/apache-hive-4.0.0-bin
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$HIVE_HOME/bin
```
Applica immediatamente le modifiche lanciando: **source ~/.bashrc**

* #### B. Modifica di core-site.xml ($HADOOP_HOME/etc/hadoop/core-site.xml)

Apri il file e inserisci questa proprietà tra i tag <configuration> per mappare l'indirizzo del File System locale:
```bash
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

* #### C. Modifica di hdfs-site.xml ($HADOOP_HOME/etc/hadoop/hdfs-site.xml)

Apri il file e inserisci questa proprietà per impostare il fattore di replica dei dati a 1, ideale per l'uso su una macchina singola:

```bash
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
```

* #### D. Patch e Configurazione Automatizzata di yarn-site.xml

Per consentire lo scambio corretto dei dati (fase di Shuffle) durante i job di calcolo senza dover modificare manualmente i file XML interni, esegui questo comando Python direttamente nel tuo terminale. Lo script configurerà in automatico i servizi ausiliari di YARN:

```bash
python3 -c "
import xml.etree.ElementTree as ET
import os
file_path = os.path.expanduser('~/hadoop-3.4.1/etc/hadoop/yarn-site.xml')
tree = ET.parse(file_path)
root = tree.getroot()
properties = {
    'yarn.nodemanager.aux-services': 'mapreduce_shuffle',
    'yarn.nodemanager.aux-services.mapreduce_shuffle.class': 'org.apache.hadoop.mapred.ShuffleHandler'
}
for name, val in properties.items():
    for prop in root.findall('property'):
        n = prop.find('name')
        if n is not None and n.text == name:
            root.remove(prop)
    p = ET.SubElement(root, 'property')
    ET.SubElement(p, 'name').text = name
    ET.SubElement(p, 'value').text = val
tree.write(file_path, encoding='utf-8', xml_declaration=True)
print('\n[OK] yarn-site.xml patchato con successo!')
"
```

### 3. Inizializzazione dello Pseudo-Cluster via setup.sh

La formattazione iniziale del File System, l'azzeramento dei residui temporanei e l'avvio sequenziale dei servizi HDFS e YARN sono interamente automatizzati. Spostati nella radice del progetto ed esegui:

```bash
bash setup.sh
```

### 4. Validazione dell'Infrastruttura Locale tramite jps

Per verificare che lo pseudo-cluster sia partito correttamente e che tutti i servizi siano attivi, lancia il comando di controllo di Java:

```bash
jps
```
Il terminale deve mostrare i 5 processi core di Hadoop attivi:

```text
- NameNode (Supervisore delle directory HDFS)

- DataNode (Responsabile della scrittura dei dati su disco)

- SecondaryNameNode (Gestore dei checkpoint dei log HDFS)

- ResourceManager (Orchestratore globale delle risorse YARN)

- NodeManager (Esecutore locale dei container di calcolo YARN)
```

Se tutti e cinque i processi sono presenti in lista, lo pseudo-cluster è pronto per eseguire la pipeline di benchmark.

## 📊 Preparazione Dataset

Questa sezione descrive i comandi per configurare l'ambiente Python, scaricare i dati ed eseguire la pipeline di preparazione e caricamento su HDFS.

### 1. Configurazione Ambiente Virtuale e Installazione Dipendenze

Dalla radice del progetto, esegui i seguenti comandi per isolare l'ambiente e installare i pacchetti necessari:
```bash
python3 -m venv env
source env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```
### 2. Download, Preprocessing e Frazionamento del Dataset

Esegui lo script Python per scaricare il file originale da Kaggle e, successivamente, l'orchestratore Bash per ripulire i dati, generare le frazioni (1%, 20%, 50%, 70%, 100%) e caricarle automaticamente sia in locale (**data/processed/**) che su **HDFS**:
```bash
cd dataset/

# 1. Download del dataset grezzo dentro data/raw/
python3 download.py

# 2. Esecuzione del preprocessing, frazionamento e upload su HDFS
bash generate_data.sh local[*]
```

### 3. Verifica dei File su HDFS

Per assicurarti che tutte le porzioni di dataset siano state caricate correttamente nello storage distribuito, lancia il comando di controllo:
```bash
hdfs dfs -ls -h /user/hadoop/data/
```

## 🚀 Esecuzione della Pipeline (HOST)

Questa sezione descrive come lanciare la suite di benchmark sulla macchina host locale e come interpretare i report generati automaticamente al termine dei test.

⚠️ **PREREQUISITO FONDAMENTALE:** Prima di procedere con l'esecuzione, l'ambiente deve essere stato preventivamente configurato per l'avvio dello pseudo-cluster Hadoop (HDFS + YARN) come descritto nella sezione precedente. Assicurati inoltre che la cinquina di demoni sia attiva verificando con il comando `jps`.

### 1. Comando di Avvio e Modalità di Esecuzione

L'intera esecuzione dei test (Job 1 e Job 2 applicati a tutte e 5 le porzioni del dataset per Spark Core, Spark SQL e Hive) è centralizzata nell'orchestratore `experiments.sh`. Prima di lanciarlo, assicurati di aver attivato l'ambiente virtuale (`source env/bin/activate`).

A seconda dell'architettura che desideri testare, esegui uno dei seguenti comandi dalla radice del progetto:

* **Esecuzione in modalità Locale Pura (Single Machine):**
  ```bash
  bash experiments.sh local[*]
  ```
  In questa modalità, Apache Spark esegue i calcoli sfruttando il multi-threading direttamente sulla macchina host, isolando l'esecuzione all'interno di un singolo processo e utilizzando tutti i core della CPU disponibili (indicati da *). Non viene coinvolto il resource manager YARN.

* **Esecuzione in modalità Pseudo-Distribuita (YARN):**
  ```bash
  bash experiments.sh yarn
  ```
  I Job vengono sottomessi formalmente all'orchestratore locale Hadoop YARN. Questa modalità simula il comportamento di un vero cluster di produzione distributed, allocando dinamicamente i container di calcolo operai sulla macchina host e testando i limiti di gestione delle risorse dell'infrastruttura locale.

### 2. Output dei Test e Reportistica Grafica (logs/)

Al termine della suite di esperimenti, il motore Python integrato (benchmark.py) raccoglie i tempi di risposta (espressi in secondi) di ciascun framework e genera automaticamente i report all'interno della directory logs/ (suddivisi nelle sottocartelle logs/local/ o logs/yarn/ in base alla modalità scelta).
All'interno della rispettiva cartella troverai:
* File di Log Testuali: I log dettagliati con i tempi esatti di computazione registrati per ogni singolo Job su ogni frazione di dataset.
* Grafici Comparativi PNG: Immagini autogenerate tramite Matplotlib che mostrano le metriche di esecuzione. I grafici mettono in relazione i tempi di calcolo (sull'asse Y) delle tre tecnologie utilizzate (Spark Core, Spark SQL, Hive) al variare della dimensione del dataset (1%, 20%, 50%, 70%, 100% sull'asse X), permettendo di valutare analiticamente la scalabilità delle diverse tecnologie.

## ☁️ Esecuzione della Pipeline su Cloud (AWS - Cluster EMR)

Questa sezione descrive la procedura end-to-end per configurare l'infrastruttura Cloud reale su Amazon Web Services, importare i dati dal servizio di storage S3, configurare il cluster distribuito EMR ed eseguire la suite di benchmark.

### 1. Configurazione di Amazon S3 e Caricamento Dati
Prima di avviare le macchine computazionali, è necessario memorizzare i dati su Object Storage per renderli accessibili al cluster.
* Accedi alla console AWS e crea un **Bucket S3** (es. `my-flight-benchmark-bucket`).
* All'interno del bucket, crea una cartella denominata `data/`.
* Carica all'interno di questa cartella i 5 file CSV processati in precedenza sull'host locale (all'interno di `data/processed/`), ovvero: `flights_1.csv`, `flights_20.csv`, `flights_50.csv`, `flights_70.csv` e `flights_cleaned.csv`.



### 2. Creazione del Cluster AWS EMR
Accedi al pannello di controllo di AWS EMR e avvia la creazione guidata di un cluster personalizzato (Custom) con i seguenti parametri:
* **Applicazioni (Software Configuration):** Seleziona il pacchetto che include **Hadoop**, **Spark**, **Hive** e **Hue**. Assicurati che l'engine di Hive sia impostato su **Tez**.
* **Hardware Configuration:** * Configura **1 Primary Node (Master)** e **2 Core Nodes (Worker)**.
  * Seleziona per tutte le macchine il tipo di istanza **`m5.xlarge`** (4 vCPU, 16 GB RAM).
  * Verifica che sia associata la chiave EC2 (`.pem`) per l'accesso SSH primario.


### 3. Configurazione di Sicurezza e Connessione SSH
Di default, AWS blocca le connessioni in ingresso verso il cluster. Per abilitare l'accesso da terminale:
* Nel riepilogo del cluster appena creato, clicca sul link del **Security Group del Master Node**.
* Seleziona il gruppo e modifica le **Inbound Rules** (Regole in ingresso).
* Aggiungi una regola che permetta il traffico sulla porta **22 (SSH)** impostando come sorgente il tuo IP attuale (`My IP`) o `0.0.0.0/0`.
* Apri il terminale del tuo computer locale e connettiti alla shell del Master Node tramite il comando SSH nativo fornito dalla console AWS:
```bash
ssh -i /percorso/tua-chiave.pem hadoop@IP-PUBBLICO-MASTER-NODE
```

### 4. Importazione Dati da S3 ad HDFS del Cluster
Una volta all'interno del Master Node, esegui i comandi in sequenza per prelevare i dati da S3 tramite il client AWS CLI preinstallato e caricarli nel File System distribuito del nuovo cluster:
```bash
# 1. Crea la cartella temporanea locale sul Master Node
mkdir -p ~/target_data

# 2. Scarica i file CSV da S3 alla cartella locale (Sostituisci il nome reale del tuo bucket)
aws s3 cp s3://IL-NOME-REALE-DEL-TUO-BUCKET/data/ ~/target_data/ --recursive --exclude "*" --include "*.csv"

# 3. Crea la directory di destinazione dentro HDFS
hdfs dfs -mkdir -p /user/hadoop/data/

# 4. Sposta i CSV dentro HDFS
hdfs dfs -put ~/target_data/*.csv /user/hadoop/data/

# 5. Pulisci la cartella temporanea locale per liberare spazio
rm -rf ~/target_data
```

Per accertarti che le porzioni di dataset siano state agganciate dal cluster Cloud, lancia il comando di controllo:
```bash
hdfs dfs -ls -h /user/hadoop/data/
```

### 5. Configurazione Ambiente e Clonazione Progetto
Sempre all'interno del terminale del Master Node, è necessario installare i requisiti software mancanti e scaricare il codice sorgente dell'applicazione:
```bash
# 1. Installa Git e gli strumenti di sviluppo sul Master Node
sudo yum install git -y

# 2. Installa le librerie Python per la gestione dei dati e della reportistica grafica
pip3 install matplotlib pandas --user

# 3. Clona la repository del progetto (Sostituisci con il link reale della tua repo)
git clone COPIA_IL_LINK_DELLA_TUA_REPO_GITHUB
cd flight-bigdata-benchmark/
```

### 6. Esecuzione del Benchmark Cloud ed Output (logs/aws/)
Avvia l'orchestratore generale passando il parametro specifico per AWS. Questo comando effettuerà lo switch DevOps bypassando i moduli locali e lanciando i job ottimizzati nativamente per parallelizzare il carico di calcolo sui nodi remoti:
```bash
bash experiments.sh aws
```
Al termine dei test, il framework raccoglierà le metriche aggregate di performance hardware e salverà i risultati testuali e i grafici comparativi PNG generati da Matplotlib all'interno del percorso standard **logs/aws**
