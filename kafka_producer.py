"""
Producer Kafka pour publier les messages FHIR de pression artérielle
"""
import json
import time
import argparse
from kafka import KafkaProducer
from kafka.errors import KafkaError
from fhir_generator import FHIRBloodPressureGenerator

class BloodPressureProducer:
    """Producer Kafka pour les observations de pression artérielle"""

    def __init__(self, bootstrap_servers='localhost:9092', topic='blood-pressure'):
        """
        Initialise le producer Kafka

        Args:
            bootstrap_servers: Adresse du broker Kafka
            topic: Nom du topic Kafka
        """
        self.topic = topic
        self.generator = FHIRBloodPressureGenerator()

        # Configuration du producer Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )

        print(f"Producer Kafka initialisé. Topic: {self.topic}")

    def send_observation(self, observation):
        """
        Envoie une observation sur le topic Kafka

        Args:
            observation: Observation FHIR à envoyer

        Returns:
            bool: True si l'envoi est réussi
        """
        try:
            # Utiliser l'ID du patient comme clé pour le partitionnement
            patient_id = observation['subject']['reference'].split('/')[-1]

            # Envoi asynchrone avec callback
            future = self.producer.send(
                self.topic,
                key=patient_id,
                value=observation
            )

            # Attendre la confirmation
            record_metadata = future.get(timeout=10)

            print(f"[OK] Message envoyé - Topic: {record_metadata.topic}, "
                  f"Partition: {record_metadata.partition}, "
                  f"Offset: {record_metadata.offset}")

            return True

        except KafkaError as e:
            print(f"[ERREUR] Erreur Kafka: {e}")
            return False
        except Exception as e:
            print(f"[ERREUR] Erreur: {e}")
            return False

    def send_batch(self, count=100, interval=1.0):
        """
        Envoie plusieurs observations en batch

        Args:
            count: Nombre d'observations à envoyer
            interval: Intervalle en secondes entre chaque envoi
        """
        print(f"\n=== Envoi de {count} observations ===")
        success_count = 0
        failure_count = 0

        try:
            for i in range(count):
                # Générer une observation
                observation = self.generator.generate_blood_pressure_observation()

                # Extraire les valeurs pour l'affichage
                systolic = observation['component'][0]['valueQuantity']['value']
                diastolic = observation['component'][1]['valueQuantity']['value']
                patient_id = observation['subject']['reference']

                print(f"\n[{i+1}/{count}] Patient: {patient_id}")
                print(f"  Pression artérielle: {systolic}/{diastolic} mmHg")

                # Envoyer l'observation
                if self.send_observation(observation):
                    success_count += 1
                else:
                    failure_count += 1

                # Attendre avant le prochain envoi (sauf pour le dernier)
                if i < count - 1:
                    time.sleep(interval)

        except KeyboardInterrupt:
            print("\n\nInterruption par l'utilisateur...")

        finally:
            # Forcer l'envoi des messages en attente
            self.producer.flush()

            print(f"\n=== Résumé ===")
            print(f"Messages envoyés avec succès: {success_count}")
            print(f"Échecs: {failure_count}")

    def close(self):
        """Ferme le producer proprement"""
        self.producer.close()
        print("Producer fermé")


def main():
    """Fonction principale"""
    parser = argparse.ArgumentParser(
        description='Producer Kafka pour les observations de pression artérielle'
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
        '--count',
        type=int,
        default=100,
        help='Nombre de messages à envoyer (défaut: 100)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=1.0,
        help='Intervalle en secondes entre chaque message (défaut: 1.0)'
    )

    args = parser.parse_args()

    # Créer et utiliser le producer
    producer = BloodPressureProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )

    try:
        producer.send_batch(count=args.count, interval=args.interval)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
