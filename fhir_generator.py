"""
Générateur de messages FHIR pour les observations de pression artérielle
"""
import json
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('fr_FR')

class FHIRBloodPressureGenerator:
    """Génère des observations FHIR de pression artérielle"""

    def __init__(self):
        self.patient_ids = [f"patient-{i:04d}" for i in range(1, 101)]

    def generate_blood_pressure_observation(self, patient_id=None):
        """
        Génère une observation FHIR de pression artérielle

        Args:
            patient_id: ID du patient (généré aléatoirement si non fourni)

        Returns:
            dict: Observation FHIR au format JSON
        """
        if patient_id is None:
            patient_id = random.choice(self.patient_ids)

        # Génération selon catégories AHA (American Heart Association)
        # Distribution réaliste basée sur la population
        category_choice = random.random()

        if category_choice < 0.50:  # 50% NORMAL
            systolic = random.randint(90, 119)
            diastolic = random.randint(60, 79)
        elif category_choice < 0.65:  # 15% ELEVATED
            systolic = random.randint(120, 129)
            diastolic = random.randint(60, 79)
        elif category_choice < 0.80:  # 15% HYPERTENSION STAGE 1
            if random.random() < 0.5:
                systolic = random.randint(130, 139)
                diastolic = random.randint(60, 89)
            else:
                systolic = random.randint(120, 139)
                diastolic = random.randint(80, 89)
        elif category_choice < 0.95:  # 15% HYPERTENSION STAGE 2
            if random.random() < 0.5:
                systolic = random.randint(140, 179)
                diastolic = random.randint(60, 119)
            else:
                systolic = random.randint(130, 179)
                diastolic = random.randint(90, 119)
        elif category_choice < 0.98:  # 3% HYPERTENSIVE CRISIS
            if random.random() < 0.5:
                systolic = random.randint(181, 200)
                diastolic = random.randint(90, 140)
            else:
                systolic = random.randint(160, 200)
                diastolic = random.randint(121, 140)
        else:  # 2% HYPOTENSION
            systolic = random.randint(70, 89)
            diastolic = random.randint(40, 59)

        # Timestamp aléatoire dans les derniers 7 jours (pour variation temporelle)
        timestamp = datetime.now() - timedelta(
            days=random.randint(0, 7),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )

        observation = {
            "resourceType": "Observation",
            "id": f"bp-{fake.uuid4()}",
            "status": "final",
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "vital-signs",
                            "display": "Vital Signs"
                        }
                    ]
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "85354-9",
                        "display": "Blood pressure panel with all children optional"
                    }
                ],
                "text": "Blood Pressure"
            },
            "subject": {
                "reference": f"Patient/{patient_id}",
                "display": fake.name()
            },
            "effectiveDateTime": timestamp.isoformat(),
            "issued": datetime.now().isoformat(),
            "component": [
                {
                    "code": {
                        "coding": [
                            {
                                "system": "http://loinc.org",
                                "code": "8480-6",
                                "display": "Systolic blood pressure"
                            }
                        ],
                        "text": "Systolic Blood Pressure"
                    },
                    "valueQuantity": {
                        "value": systolic,
                        "unit": "mmHg",
                        "system": "http://unitsofmeasure.org",
                        "code": "mm[Hg]"
                    }
                },
                {
                    "code": {
                        "coding": [
                            {
                                "system": "http://loinc.org",
                                "code": "8462-4",
                                "display": "Diastolic blood pressure"
                            }
                        ],
                        "text": "Diastolic Blood Pressure"
                    },
                    "valueQuantity": {
                        "value": diastolic,
                        "unit": "mmHg",
                        "system": "http://unitsofmeasure.org",
                        "code": "mm[Hg]"
                    }
                }
            ]
        }

        return observation

    def generate_multiple_observations(self, count=100):
        """
        Génère plusieurs observations

        Args:
            count: Nombre d'observations à générer

        Returns:
            list: Liste d'observations FHIR
        """
        observations = []
        for _ in range(count):
            obs = self.generate_blood_pressure_observation()
            observations.append(obs)

        return observations


if __name__ == "__main__":
    # Test du générateur
    generator = FHIRBloodPressureGenerator()

    # Générer une observation unique
    single_obs = generator.generate_blood_pressure_observation()
    print("=== Observation FHIR unique ===")
    print(json.dumps(single_obs, indent=2, ensure_ascii=False))

    # Générer plusieurs observations
    multiple_obs = generator.generate_multiple_observations(5)
    print(f"\n=== {len(multiple_obs)} observations générées ===")
    for i, obs in enumerate(multiple_obs, 1):
        systolic = obs['component'][0]['valueQuantity']['value']
        diastolic = obs['component'][1]['valueQuantity']['value']
        print(f"{i}. Patient: {obs['subject']['reference']} - BP: {systolic}/{diastolic} mmHg")
