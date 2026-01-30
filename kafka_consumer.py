"""
Consumer Kafka avec analyse des anomalies de pression artérielle
et stockage dans Elasticsearch ou fichiers JSON
"""
import json
import os
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, TransportError
import argparse

class BloodPressureAnalyzer:
    """Analyse les observations de pression artérielle selon les standards AHA"""

    # Seuils médicaux basés sur American Heart Association
    # NORMAL: < 120 systolic AND < 80 diastolic
    # ELEVATED: 120-129 systolic AND < 80 diastolic
    # HYPERTENSION STAGE 1: 130-139 systolic OR 80-89 diastolic
    # HYPERTENSION STAGE 2: >= 140 systolic OR >= 90 diastolic
    # HYPERTENSIVE CRISIS: > 180 systolic AND/OR > 120 diastolic

    SYSTOLIC_NORMAL_MAX = 120
    SYSTOLIC_ELEVATED_MAX = 129
    SYSTOLIC_STAGE1_MAX = 139
    SYSTOLIC_STAGE2_MIN = 140
    SYSTOLIC_CRISIS_MIN = 180

    DIASTOLIC_NORMAL_MAX = 80
    DIASTOLIC_STAGE1_MIN = 80
    DIASTOLIC_STAGE1_MAX = 89
    DIASTOLIC_STAGE2_MIN = 90
    DIASTOLIC_CRISIS_MIN = 120

    @staticmethod
    def extract_blood_pressure_values(observation):
        """
        Extrait les valeurs systolique et diastolique d'une observation FHIR

        Args:
            observation: Observation FHIR

        Returns:
            tuple: (systolic, diastolic)
        """
        systolic = None
        diastolic = None

        for component in observation.get('component', []):
            code = component.get('code', {}).get('coding', [{}])[0].get('code')
            value = component.get('valueQuantity', {}).get('value')

            if code == '8480-6':  # Systolic
                systolic = value
            elif code == '8462-4':  # Diastolic
                diastolic = value

        return systolic, diastolic

    @classmethod
    def analyze_observation(cls, observation):
        """
        Analyse une observation selon les standards AHA

        Args:
            observation: Observation FHIR

        Returns:
            dict: Résultat de l'analyse avec catégorie et anomalies
        """
        systolic, diastolic = cls.extract_blood_pressure_values(observation)

        if systolic is None or diastolic is None:
            return {
                'is_normal': False,
                'category': 'missing_values',
                'anomalies': ['missing_values'],
                'systolic': systolic,
                'diastolic': diastolic
            }

        # Détermination de la catégorie selon AHA
        category = 'unknown'
        anomalies = []

        # HYPERTENSIVE CRISIS - Urgence médicale (>180 systolic OR >120 diastolic)
        if systolic > cls.SYSTOLIC_CRISIS_MIN or diastolic > cls.DIASTOLIC_CRISIS_MIN:
            category = 'hypertensive_crisis'
            anomalies.append('hypertensive_crisis')

        # HYPERTENSION STAGE 2 (>=140 systolic OR >=90 diastolic)
        elif systolic >= cls.SYSTOLIC_STAGE2_MIN or diastolic >= cls.DIASTOLIC_STAGE2_MIN:
            category = 'hypertension_stage2'
            anomalies.append('hypertension_stage2')

        # HYPERTENSION STAGE 1 (130-139 systolic OR 80-89 diastolic)
        elif (systolic >= 130 and systolic <= cls.SYSTOLIC_STAGE1_MAX) or \
             (diastolic >= cls.DIASTOLIC_STAGE1_MIN and diastolic <= cls.DIASTOLIC_STAGE1_MAX):
            category = 'hypertension_stage1'
            anomalies.append('hypertension_stage1')

        # ELEVATED (120-129 systolic AND <80 diastolic)
        elif systolic >= cls.SYSTOLIC_NORMAL_MAX and systolic <= cls.SYSTOLIC_ELEVATED_MAX and \
             diastolic < cls.DIASTOLIC_NORMAL_MAX:
            category = 'elevated'
            anomalies.append('elevated_blood_pressure')

        # NORMAL (<120 systolic AND <80 diastolic)
        elif systolic < cls.SYSTOLIC_NORMAL_MAX and diastolic < cls.DIASTOLIC_NORMAL_MAX:
            category = 'normal'

        # HYPOTENSION (<90 systolic OR <60 diastolic)
        elif systolic < 90 or diastolic < 60:
            category = 'hypotension'
            anomalies.append('hypotension')

        return {
            'is_normal': category == 'normal',
            'category': category,
            'anomalies': anomalies,
            'systolic': systolic,
            'diastolic': diastolic
        }


class BloodPressureConsumer:
    """Consumer Kafka pour les observations de pression artérielle"""

    def __init__(self, bootstrap_servers='localhost:9092', topic='blood-pressure',
                 elasticsearch_host='localhost:9200', normal_data_dir='data/normal_patients'):
        """
        Initialise le consumer Kafka

        Args:
            bootstrap_servers: Adresse du broker Kafka
            topic: Nom du topic Kafka
            elasticsearch_host: Adresse d'Elasticsearch
            normal_data_dir: Répertoire pour sauvegarder les données normales
        """
        self.topic = topic
        self.normal_data_dir = normal_data_dir
        self.analyzer = BloodPressureAnalyzer()

        # Créer le répertoire pour les données normales
        os.makedirs(self.normal_data_dir, exist_ok=True)

        # Configuration du consumer Kafka
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='blood-pressure-consumer-group'
        )

        # Configuration Elasticsearch
        # Ajouter http:// si non présent
        if not elasticsearch_host.startswith(('http://', 'https://')):
            elasticsearch_host = f'http://{elasticsearch_host}'
        self.es = Elasticsearch([elasticsearch_host])
        self.es_index = 'blood-pressure-anomalies'

        print(f"Consumer Kafka initialisé. Topic: {self.topic}")
        self.check_elasticsearch_connection()
        self.create_elasticsearch_index()

    def check_elasticsearch_connection(self):
        """Vérifie la connexion à Elasticsearch"""
        try:
            if self.es.ping():
                print("[OK] Connexion à Elasticsearch réussie")
                return True
            else:
                print("[ERREUR] Impossible de se connecter à Elasticsearch")
                return False
        except ConnectionError as e:
            print(f"[ERREUR] Erreur de connexion à Elasticsearch: {e}")
            return False

    def create_elasticsearch_index(self):
        """Crée l'index Elasticsearch avec le mapping approprié"""
        mapping = {
            "mappings": {
                "properties": {
                    "observation_id": {"type": "keyword"},
                    "patient_id": {"type": "keyword"},
                    "patient_name": {"type": "text"},
                    "systolic_pressure": {"type": "integer"},
                    "diastolic_pressure": {"type": "integer"},
                    "category": {"type": "keyword"},
                    "anomaly_types": {"type": "keyword"},
                    "effective_datetime": {"type": "date"},
                    "issued_datetime": {"type": "date"},
                    "processed_datetime": {"type": "date"},
                    "severity": {"type": "keyword"}
                }
            }
        }

        try:
            if not self.es.indices.exists(index=self.es_index):
                self.es.indices.create(index=self.es_index, body=mapping)
                print(f"[OK] Index Elasticsearch '{self.es_index}' créé")
            else:
                print(f"[OK] Index Elasticsearch '{self.es_index}' existe déjà")
        except Exception as e:
            print(f"[ERREUR] Erreur lors de la création de l'index: {e}")

    def determine_severity(self, systolic, diastolic):
        """
        Détermine la sévérité de l'anomalie

        Args:
            systolic: Pression systolique
            diastolic: Pression diastolique

        Returns:
            str: Niveau de sévérité (critical, high, moderate)
        """
        if systolic >= 180 or diastolic >= 120:
            return 'critical'
        elif systolic >= 160 or diastolic >= 100 or systolic < 80 or diastolic < 50:
            return 'high'
        else:
            return 'moderate'

    def save_normal_observation(self, observation, analysis):
        """
        Sauvegarde une observation normale dans un fichier JSON

        Args:
            observation: Observation FHIR
            analysis: Résultat de l'analyse
        """
        patient_id = observation['subject']['reference'].split('/')[-1]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{patient_id}_{timestamp}.json"
        filepath = os.path.join(self.normal_data_dir, filename)

        data = {
            'observation': observation,
            'analysis': analysis,
            'processed_at': datetime.now().isoformat()
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"  [OK] Données normales sauvegardées: {filename}")

    def index_anomaly(self, observation, analysis):
        """
        Indexe une anomalie dans Elasticsearch

        Args:
            observation: Observation FHIR
            analysis: Résultat de l'analyse
        """
        patient_ref = observation['subject']['reference']
        patient_id = patient_ref.split('/')[-1]

        document = {
            'observation_id': observation['id'],
            'patient_id': patient_id,
            'patient_name': observation['subject'].get('display', 'Unknown'),
            'systolic_pressure': analysis['systolic'],
            'diastolic_pressure': analysis['diastolic'],
            'category': analysis.get('category', 'unknown'),
            'anomaly_types': analysis['anomalies'],
            'effective_datetime': observation.get('effectiveDateTime'),
            'issued_datetime': observation.get('issued'),
            'processed_datetime': datetime.now().isoformat(),
            'severity': self.determine_severity(analysis['systolic'], analysis['diastolic'])
        }

        try:
            response = self.es.index(index=self.es_index, document=document)
            print(f"  [OK] Anomalie indexée dans Elasticsearch (ID: {response['_id']})")
            return True
        except Exception as e:
            print(f"  [ERREUR] Erreur lors de l'indexation: {e}")
            return False

    def process_message(self, message):
        """
        Traite un message reçu de Kafka

        Args:
            message: Message Kafka
        """
        observation = message.value

        print(f"\n=== Traitement du message ===")
        print(f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
        print(f"Patient: {observation['subject']['reference']}")

        # Analyser l'observation
        analysis = self.analyzer.analyze_observation(observation)

        print(f"Pression artérielle: {analysis['systolic']}/{analysis['diastolic']} mmHg")

        if analysis['is_normal']:
            print("[OK] Pression artérielle normale")
            self.save_normal_observation(observation, analysis)
        else:
            print(f"[ALERTE] Anomalie détectée: {', '.join(analysis['anomalies'])}")
            self.index_anomaly(observation, analysis)

    def consume(self):
        """Consomme les messages du topic Kafka"""
        print(f"\n=== Démarrage de la consommation des messages ===")
        print("En attente de messages... (Ctrl+C pour arrêter)\n")

        message_count = 0
        normal_count = 0
        anomaly_count = 0

        try:
            for message in self.consumer:
                message_count += 1
                self.process_message(message)

                # Mettre à jour les compteurs
                observation = message.value
                analysis = self.analyzer.analyze_observation(observation)

                if analysis['is_normal']:
                    normal_count += 1
                else:
                    anomaly_count += 1

                # Afficher les statistiques périodiquement
                if message_count % 10 == 0:
                    print(f"\n--- Statistiques ---")
                    print(f"Messages traités: {message_count}")
                    print(f"Observations normales: {normal_count}")
                    print(f"Anomalies détectées: {anomaly_count}")
                    print("--------------------\n")

        except KeyboardInterrupt:
            print("\n\nInterruption par l'utilisateur...")
        finally:
            print(f"\n=== Résumé final ===")
            print(f"Total de messages traités: {message_count}")
            print(f"Observations normales: {normal_count}")
            print(f"Anomalies détectées: {anomaly_count}")
            self.close()

    def close(self):
        """Ferme le consumer proprement"""
        self.consumer.close()
        print("Consumer fermé")


def main():
    """Fonction principale"""
    parser = argparse.ArgumentParser(
        description='Consumer Kafka pour les observations de pression artérielle'
    )
    parser.add_argument(
        '--bootstrap-servers',
        default='localhost:9092',
        help='Adresse du broker Kafka (défaut: localhost:9092)'
    )
    parser.add_argument(
        '--topic',
        default='blood-pressure',
        help='Nom du topic Kafka (défaut: blood-pressure)'
    )
    parser.add_argument(
        '--elasticsearch-host',
        default='localhost:9200',
        help='Adresse d\'Elasticsearch (défaut: localhost:9200)'
    )
    parser.add_argument(
        '--normal-data-dir',
        default='data/normal_patients',
        help='Répertoire pour les données normales (défaut: data/normal_patients)'
    )

    args = parser.parse_args()

    # Créer et utiliser le consumer
    consumer = BloodPressureConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        elasticsearch_host=args.elasticsearch_host,
        normal_data_dir=args.normal_data_dir
    )

    consumer.consume()


if __name__ == "__main__":
    main()
