import pandas as pd
from sklearn.datasets import load_breast_cancer

data = load_breast_cancer()
df = pd.DataFrame(data.data, columns=data.feature_names)
df["target"] = data.target
df.to_csv("data/new_data.csv", index=False)

print("Zapisano zbiór danych do data/new_data.csv")
print(df.head())
print("Liczba rekordow:", len(df))
print("Liczba cech:", len(data.feature_names))