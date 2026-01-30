"""
Script de configuration Kibana pour créer les visualisations et dashboards
"""
import requests
import json
import time

class KibanaSetup:
    """Configuration automatique de Kibana"""

    def __init__(self, kibana_url='http://localhost:5601', elasticsearch_url='http://localhost:9200'):
        """
        Initialise la configuration Kibana

        Args:
            kibana_url: URL de Kibana
            elasticsearch_url: URL d'Elasticsearch
        """
        self.kibana_url = kibana_url
        self.elasticsearch_url = elasticsearch_url
        self.index_pattern = 'blood-pressure-anomalies'
        self.headers = {
            'kbn-xsrf': 'true',
            'Content-Type': 'application/json'
        }

    def wait_for_kibana(self, max_attempts=30):
        """
        Attend que Kibana soit prêt

        Args:
            max_attempts: Nombre maximum de tentatives

        Returns:
            bool: True si Kibana est prêt
        """
        print("Attente du démarrage de Kibana...")
        for attempt in range(max_attempts):
            try:
                response = requests.get(f"{self.kibana_url}/api/status")
                if response.status_code == 200:
                    print("[OK] Kibana est prêt")
                    return True
            except requests.exceptions.ConnectionError:
                pass

            print(f"  Tentative {attempt + 1}/{max_attempts}...")
            time.sleep(10)

        print("[ERREUR] Timeout: Kibana n'est pas prêt")
        return False

    def create_index_pattern(self):
        """
        Crée un index pattern dans Kibana

        Returns:
            bool: True si la création est réussie
        """
        print(f"\nCréation de l'index pattern '{self.index_pattern}'...")

        # Vérifier si l'index pattern existe déjà
        check_url = f"{self.kibana_url}/api/data_views"
        try:
            response = requests.get(check_url, headers=self.headers)
            if response.status_code == 200:
                data_views = response.json()
                for dv in data_views.get('data_view', []):
                    if dv.get('title') == self.index_pattern:
                        print(f"[OK] Index pattern '{self.index_pattern}' existe déjà")
                        return True
        except Exception as e:
            print(f"Erreur lors de la vérification: {e}")

        # Créer l'index pattern
        url = f"{self.kibana_url}/api/data_views/data_view"
        payload = {
            "data_view": {
                "title": self.index_pattern,
                "timeFieldName": "processed_datetime"
            }
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload)
            if response.status_code in [200, 201]:
                print(f"[OK] Index pattern '{self.index_pattern}' créé avec succès")
                return True
            else:
                print(f"[ERREUR] Erreur lors de la création de l'index pattern: {response.status_code}")
                print(f"  Réponse: {response.text}")
                return False
        except Exception as e:
            print(f"[ERREUR] Erreur: {e}")
            return False

    def get_visualization_config(self, viz_id, title, viz_type, config):
        """
        Génère la configuration d'une visualisation

        Args:
            viz_id: ID de la visualisation
            title: Titre de la visualisation
            viz_type: Type de visualisation
            config: Configuration spécifique

        Returns:
            dict: Configuration de la visualisation
        """
        return {
            "attributes": {
                "title": title,
                "visState": json.dumps({
                    "type": viz_type,
                    "params": config.get("params", {}),
                    "aggs": config.get("aggs", [])
                }),
                "uiStateJSON": "{}",
                "description": "",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": self.index_pattern,
                        "query": {
                            "query": "",
                            "language": "kuery"
                        },
                        "filter": []
                    })
                }
            }
        }

    def print_manual_instructions(self):
        """Affiche les instructions manuelles pour configurer Kibana"""
        print("\n" + "=" * 70)
        print("INSTRUCTIONS POUR CONFIGURER KIBANA MANUELLEMENT")
        print("=" * 70)
        print("\n1. Accédez à Kibana: http://localhost:5601")
        print("\n2. Créer un Data View (Index Pattern):")
        print("   - Allez dans 'Management' > 'Stack Management' > 'Data Views'")
        print("   - Cliquez sur 'Create data view'")
        print(f"   - Name: {self.index_pattern}")
        print(f"   - Index pattern: {self.index_pattern}")
        print("   - Timestamp field: processed_datetime")
        print("   - Cliquez sur 'Save data view to Kibana'")

        print("\n3. Créer les visualisations:")
        print("\n   a) Distribution des anomalies par type:")
        print("      - Type: Pie Chart")
        print("      - Buckets: Split slices > Terms > anomaly_types")
        print("      - Metrics: Count")

        print("\n   b) Évolution temporelle des anomalies:")
        print("      - Type: Line Chart")
        print("      - X-axis: Date Histogram > processed_datetime")
        print("      - Y-axis: Count")
        print("      - Split series: Terms > anomaly_types")

        print("\n   c) Distribution par sévérité:")
        print("      - Type: Pie Chart")
        print("      - Buckets: Split slices > Terms > severity")
        print("      - Metrics: Count")

        print("\n   d) Tableau des cas critiques:")
        print("      - Type: Data Table")
        print("      - Filter: severity:critical")
        print("      - Columns: patient_id, patient_name, systolic_pressure,")
        print("                 diastolic_pressure, anomaly_types, processed_datetime")

        print("\n   e) Pression systolique moyenne par patient:")
        print("      - Type: Bar Chart")
        print("      - X-axis: Terms > patient_id")
        print("      - Y-axis: Average > systolic_pressure")

        print("\n   f) Pression diastolique moyenne par patient:")
        print("      - Type: Bar Chart")
        print("      - X-axis: Terms > patient_id")
        print("      - Y-axis: Average > diastolic_pressure")

        print("\n4. Créer un Dashboard:")
        print("   - Allez dans 'Analytics' > 'Dashboard'")
        print("   - Cliquez sur 'Create dashboard'")
        print("   - Ajoutez toutes les visualisations créées ci-dessus")
        print("   - Organisez-les selon vos préférences")
        print("   - Sauvegardez le dashboard avec le nom:")
        print("     'Surveillance Pression Artérielle'")

        print("\n5. Créer des alertes (optionnel):")
        print("   - Allez dans 'Management' > 'Stack Management' > 'Rules'")
        print("   - Créez une règle pour les cas critiques:")
        print("     - Condition: severity is 'critical'")
        print("     - Action: Log, Email, ou autre selon vos besoins")

        print("\n" + "=" * 70)
        print("Pour plus d'informations, consultez la documentation Kibana:")
        print("https://www.elastic.co/guide/en/kibana/current/index.html")
        print("=" * 70 + "\n")


def main():
    """Fonction principale"""
    print("=== Configuration de Kibana ===\n")

    setup = KibanaSetup()

    # Attendre que Kibana soit prêt
    if not setup.wait_for_kibana():
        print("\nKibana n'est pas accessible. Assurez-vous que:")
        print("1. Docker Compose est lancé: docker-compose up -d")
        print("2. Kibana est démarré (peut prendre quelques minutes)")
        print("\nVous pouvez vérifier l'état avec: docker-compose ps")
        return

    # Créer l'index pattern
    setup.create_index_pattern()

    # Afficher les instructions manuelles
    setup.print_manual_instructions()

    print("\n[OK] Configuration Kibana terminée")
    print("\nNote: Les visualisations et dashboards doivent être créés manuellement")
    print("      en suivant les instructions ci-dessus.")


if __name__ == "__main__":
    main()
