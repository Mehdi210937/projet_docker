"""
Script de test pour vérifier que tous les composants du système fonctionnent
"""
import sys
import time
import requests
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from elasticsearch import Elasticsearch


class SystemTester:
    """Teste tous les composants du système"""

    def __init__(self):
        self.results = []

    def test_kafka_connection(self):
        """Teste la connexion à Kafka"""
        print("\n[TEST] Connexion à Kafka...")
        try:
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                request_timeout_ms=5000
            )
            producer.close()
            print("[OK] Kafka est accessible")
            self.results.append(("Kafka", True))
            return True
        except Exception as e:
            print(f"[ERREUR] Erreur Kafka: {e}")
            self.results.append(("Kafka", False))
            return False

    def test_elasticsearch_connection(self):
        """Teste la connexion à Elasticsearch"""
        print("\n[TEST] Connexion à Elasticsearch...")
        try:
            es = Elasticsearch(['http://localhost:9200'])
            if es.ping():
                health = es.cluster.health()
                print(f"[OK] Elasticsearch est accessible")
                print(f"  Status: {health['status']}")
                print(f"  Nombre de nœuds: {health['number_of_nodes']}")
                self.results.append(("Elasticsearch", True))
                return True
            else:
                print("[ERREUR] Elasticsearch ne répond pas")
                self.results.append(("Elasticsearch", False))
                return False
        except Exception as e:
            print(f"[ERREUR] Erreur Elasticsearch: {e}")
            self.results.append(("Elasticsearch", False))
            return False

    def test_kibana_connection(self):
        """Teste la connexion à Kibana"""
        print("\n[TEST] Connexion à Kibana...")
        try:
            response = requests.get('http://localhost:5601/api/status', timeout=5)
            if response.status_code == 200:
                print("[OK] Kibana est accessible")
                self.results.append(("Kibana", True))
                return True
            else:
                print(f"[ERREUR] Kibana erreur: {response.status_code}")
                self.results.append(("Kibana", False))
                return False
        except Exception as e:
            print(f"[ERREUR] Erreur Kibana: {e}")
            self.results.append(("Kibana", False))
            return False

    def test_kafka_topic(self):
        """Teste la création et l'accès au topic Kafka"""
        print("\n[TEST] Topic Kafka 'blood-pressure'...")
        try:
            from kafka.admin import KafkaAdminClient
            admin = KafkaAdminClient(
                bootstrap_servers='localhost:9092',
                request_timeout_ms=5000
            )
            topics = admin.list_topics()
            if 'blood-pressure' in topics:
                print("[OK] Topic 'blood-pressure' existe")
                self.results.append(("Kafka Topic", True))
                return True
            else:
                print("[INFO] Topic 'blood-pressure' n'existe pas encore (sera créé automatiquement)")
                self.results.append(("Kafka Topic", True))
                return True
        except Exception as e:
            print(f"[ERREUR] Erreur lors de la vérification du topic: {e}")
            self.results.append(("Kafka Topic", False))
            return False

    def test_elasticsearch_index(self):
        """Teste l'existence de l'index Elasticsearch"""
        print("\n[TEST] Index Elasticsearch 'blood-pressure-anomalies'...")
        try:
            es = Elasticsearch(['http://localhost:9200'])
            if es.indices.exists(index='blood-pressure-anomalies'):
                count = es.count(index='blood-pressure-anomalies')
                print(f"[OK] Index 'blood-pressure-anomalies' existe")
                print(f"  Nombre de documents: {count['count']}")
                self.results.append(("ES Index", True))
                return True
            else:
                print("[INFO] Index 'blood-pressure-anomalies' n'existe pas encore (sera créé par le consumer)")
                self.results.append(("ES Index", True))
                return True
        except Exception as e:
            print(f"[ERREUR] Erreur lors de la vérification de l'index: {e}")
            self.results.append(("ES Index", False))
            return False

    def test_fhir_generator(self):
        """Teste le générateur FHIR"""
        print("\n[TEST] Générateur FHIR...")
        try:
            from fhir_generator import FHIRBloodPressureGenerator

            generator = FHIRBloodPressureGenerator()
            observation = generator.generate_blood_pressure_observation()

            # Vérifier la structure
            assert observation['resourceType'] == 'Observation'
            assert 'component' in observation
            assert len(observation['component']) == 2

            systolic = observation['component'][0]['valueQuantity']['value']
            diastolic = observation['component'][1]['valueQuantity']['value']

            print("[OK] Générateur FHIR fonctionne")
            print(f"  Exemple: {systolic}/{diastolic} mmHg")
            self.results.append(("FHIR Generator", True))
            return True
        except Exception as e:
            print(f"[ERREUR] Erreur générateur FHIR: {e}")
            self.results.append(("FHIR Generator", False))
            return False

    def test_python_dependencies(self):
        """Teste les dépendances Python"""
        print("\n[TEST] Dépendances Python...")
        dependencies = {
            'kafka': 'kafka-python',
            'elasticsearch': 'elasticsearch',
            'fhir.resources': 'fhir.resources',
            'faker': 'faker'
        }

        all_ok = True
        for module, package in dependencies.items():
            try:
                __import__(module)
                print(f"  [OK] {package}")
            except ImportError:
                print(f"  [ERREUR] {package} manquant")
                all_ok = False

        if all_ok:
            print("[OK] Toutes les dépendances sont installées")
            self.results.append(("Python Dependencies", True))
        else:
            print("[ERREUR] Certaines dépendances manquent")
            self.results.append(("Python Dependencies", False))

        return all_ok

    def print_summary(self):
        """Affiche le résumé des tests"""
        print("\n" + "=" * 60)
        print("RÉSUMÉ DES TESTS")
        print("=" * 60)

        passed = sum(1 for _, result in self.results if result)
        total = len(self.results)

        for name, result in self.results:
            status = "[OK] PASS" if result else "[ERREUR] FAIL"
            print(f"{status:10} | {name}")

        print("=" * 60)
        print(f"Résultat: {passed}/{total} tests réussis")
        print("=" * 60)

        if passed == total:
            print("\n[OK] Tous les tests sont passés! Le système est prêt.")
            return 0
        else:
            print(f"\n[ERREUR] {total - passed} test(s) ont échoué. Vérifiez la configuration.")
            return 1

    def run_all_tests(self):
        """Exécute tous les tests"""
        print("=" * 60)
        print("TEST DU SYSTÈME DE SURVEILLANCE")
        print("=" * 60)

        # Tests des dépendances Python
        self.test_python_dependencies()

        # Tests des services Docker
        self.test_kafka_connection()
        self.test_elasticsearch_connection()
        self.test_kibana_connection()

        # Tests des composants
        self.test_kafka_topic()
        self.test_elasticsearch_index()
        self.test_fhir_generator()

        # Afficher le résumé
        return self.print_summary()


def main():
    """Fonction principale"""
    tester = SystemTester()
    exit_code = tester.run_all_tests()

    print("\nPour plus d'informations:")
    print("- README.md : Documentation complète")
    print("- QUICKSTART.md : Guide de démarrage rapide")
    print("\nCommandes utiles:")
    print("- docker-compose ps : État des services")
    print("- docker-compose logs : Logs des services")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
