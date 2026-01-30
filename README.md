# Surveillance Pression Artérielle avec Kafka & Elasticsearch

Système de détection d'anomalies en temps réel pour les mesures de pression artérielle selon les standards AHA (American Heart Association).

## Architecture

```
FHIR Generator → Kafka Producer → Kafka → Consumer → Elasticsearch/JSON
                                                      ↓
                                                    Kibana
```

## Installation Rapide

### 1. Démarrer l'infrastructure Docker
```bash
docker-compose up -d
```

### 2. Installer les dépendances Python
```bash
pip install -r requirements.txt
```

### 3. Lancer le système
```bash
# Terminal 1: Consumer
python scripts/kafka_consumer.py

# Terminal 2: Producer
python scripts/kafka_producer.py --count 1000 --interval 0.2
```

### 4. Accéder à Kibana
1. Ouvrir http://localhost:5601
2. Importer le dashboard: `Management` → `Saved Objects` → `Import` → Sélectionner `dashboard/DASHBOARD.ndjson`

## Structure des Fichiers

```
LIVRABLES/
├── README.md                               # Ce fichier
├── docker-compose.yml                      # Infrastructure (Kafka, ES, Kibana)
├── requirements.txt                        # Dépendances Python
├── scripts/                                # Scripts Python
│   ├── fhir_generator.py                  # Génération de données FHIR
│   ├── kafka_producer.py                  # Publication Kafka
│   ├── kafka_consumer.py                  # Consommation et détection d'anomalies
│   ├── setup_kibana.py                    # Configuration Kibana
│   └── test_system.py                     # Tests système
├── config/                                 # Configuration
│   ├── elasticsearch_index_mapping.json   # Modèle d'index ES pour anomalies
│   └── kafka_topic_config.json            # Configuration topic Kafka
├── dashboard/                              # Dashboard Kibana
│   └── DASHBOARD.ndjson                   # Export du dashboard complet
└── exemples/                               # Exemples de données
    ├── exemple_observation_normale.json   # Observation normale
    └── exemple_anomalie_critique.json     # Anomalie critique
```

## Fonctionnement

### Détection des Anomalies (Standards AHA)

| Catégorie            | Systolique | Diastolique | Sévérité | Stockage      |
|---------------------|------------|-------------|----------|---------------|
| Normal              | <120       | AND <80     | -        | JSON local    |
| Elevated            | 120-129    | AND <80     | Moderate | Elasticsearch |
| Hypertension Stage 1| 130-139    | OR 80-89    | Moderate | Elasticsearch |
| Hypertension Stage 2| ≥140       | OR ≥90      | High     | Elasticsearch |
| Hypertensive Crisis | >180       | OR >120     | Critical | Elasticsearch |
| Hypotension         | <90        | OR <60      | Moderate | Elasticsearch |

### Stockage Différencié

- **Observations normales** → Fichiers JSON dans `data/normal_patients/`
- **Anomalies** → Elasticsearch (index `blood-pressure-anomalies`)

## Scripts Principaux

### kafka_producer.py
Génère et publie des observations FHIR sur Kafka.

```bash
python scripts/kafka_producer.py --count 1000 --interval 0.2
```

Options:
- `--count`: Nombre de messages (défaut: infini)
- `--interval`: Délai entre messages en secondes (défaut: 1.0)

### kafka_consumer.py
Consomme les messages, détecte les anomalies et stocke dans Elasticsearch/JSON.

```bash
python scripts/kafka_consumer.py
```

### test_system.py
Vérifie que tous les composants fonctionnent correctement.

```bash
python scripts/test_system.py
```

## Services Docker

| Service       | Port  | URL                      |
|--------------|-------|--------------------------|
| Kafka        | 9092  | localhost:9092           |
| Zookeeper    | 2181  | localhost:2181           |
| Elasticsearch| 9200  | http://localhost:9200    |
| Kibana       | 5601  | http://localhost:5601    |

## Modèle d'Index Elasticsearch

Le fichier `config/elasticsearch_index_mapping.json` définit la structure des anomalies stockées:

```json
{
  "mappings": {
    "properties": {
      "observation_id": {"type": "keyword"},
      "patient_id": {"type": "keyword"},
      "patient_name": {"type": "text"},
      "systolic_pressure": {"type": "integer"},
      "diastolic_pressure": {"type": "integer"},
      "category": {"type": "keyword"},
      "severity": {"type": "keyword"},
      "anomalies": {"type": "keyword"},
      "effectiveDateTime": {"type": "date"},
      "processed_datetime": {"type": "date"},
      "fhir_resource": {"type": "object", "enabled": false}
    }
  }
}
```

L'index est créé automatiquement par le consumer au premier lancement.

## Résultats Attendus (1000 observations)

- **~500 observations normales** → Fichiers JSON
- **~500 anomalies** → Elasticsearch
  - 150 Elevated (15%)
  - 150 Stage 1 (15%)
  - 150 Stage 2 (15%)
  - 30 Crisis (3%)
  - 20 Hypotension (2%)

## Troubleshooting

### Kafka ne démarre pas
```bash
docker-compose down -v
docker-compose up -d
```

### Elasticsearch refuse la connexion
Attendre 30-60 secondes après `docker-compose up -d`.

### Vérifier les logs
```bash
docker-compose logs -f
```

## Technologies

- Python 3.13+
- Apache Kafka 7.5.0
- Elasticsearch 8.11.0
- Kibana 8.11.0
- FHIR R4 (fhir.resources)
- Docker & Docker Compose
