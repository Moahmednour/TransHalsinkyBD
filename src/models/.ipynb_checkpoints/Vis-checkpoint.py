import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Connexion à PostgreSQL Neon
DB_URL = "postgresql://neondb_owner:npg_QFVrW4E3SdPm@ep-mute-hill-a2eprr2t-pooler.eu-central-1.aws.neon.tech/neondb"
engine = create_engine(DB_URL)

# Charger les prédictions
df = pd.read_sql("SELECT * FROM predictions", engine)

# Visualisation des prédictions de retard
plt.figure(figsize=(10, 5))
plt.hist(df["predicted_delay"], bins=30, color="blue", alpha=0.7)
plt.xlabel("Retard Prédit (sec)")
plt.ylabel("Nombre d'Occurences")
plt.title("Distribution des Retards Prédits")
plt.grid()
plt.show()
