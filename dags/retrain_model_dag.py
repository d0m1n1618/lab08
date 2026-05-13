import datetime
import os
import shutil

import joblib
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split


DATA_PATH = "/opt/airflow/data/new_data.csv"
MODELS_DIR = "/opt/airflow/models"
PRODUCTION_DIR = "/opt/airflow/models/production"

PRODUCTION_MODEL_PATH = os.path.join(PRODUCTION_DIR, "production_model.pkl")
PRODUCTION_METRICS_PATH = os.path.join(PRODUCTION_DIR, "production_metrics.txt")


def prepare_directories():
    #Tworzy wymagane foldery, jeżeli jeszcze nie istnieją.
    os.makedirs(MODELS_DIR, exist_ok=True)
    os.makedirs(PRODUCTION_DIR, exist_ok=True)


def retrain_model(**kwargs):
    #Wczytuje dane, trenuje nowy model RandomForestClassifier, wykonuje walidację i zapisuje model z wersjonowaniem czasowym.
    prepare_directories()

    df = pd.read_csv(DATA_PATH)

    X = df.drop("target", axis=1)
    y = df["target"]

    X_train, X_valid, y_train, y_valid = train_test_split(
        X,
        y,
        test_size=0.3,
        random_state=42,
        stratify=y
    )

    model = RandomForestClassifier(
        n_estimators=100,
        random_state=42
    )

    model.fit(X_train, y_train)

    y_pred = model.predict(X_valid)
    accuracy = accuracy_score(y_valid, y_pred)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    model_path = os.path.join(MODELS_DIR, f"rf_model_{timestamp}.pkl")
    metrics_path = os.path.join(MODELS_DIR, f"metrics_{timestamp}.txt")

    joblib.dump(model, model_path)

    with open(metrics_path, "w", encoding="utf-8") as f:
        f.write(f"model_path={model_path}\n")
        f.write(f"accuracy={accuracy}\n")
        f.write(f"timestamp={timestamp}\n")

    print(f"New model saved to: {model_path}")
    print(f"New model accuracy: {accuracy}")

    kwargs["ti"].xcom_push(key="new_model_path", value=model_path)
    kwargs["ti"].xcom_push(key="new_accuracy", value=accuracy)

    return model_path


def compare_and_promote_model(**kwargs):
    """
    Porównuje nowy model z modelem produkcyjnym.
    Jeżeli nowy model ma wyższą lub równą accuracy, zostaje podmieniony jako produkcyjny.
    W przeciwnym razie pozostaje tylko w archiwum.
    """
    ti = kwargs["ti"]

    new_model_path = ti.xcom_pull(
        task_ids="retrain_model",
        key="new_model_path"
    )

    new_accuracy = ti.xcom_pull(
        task_ids="retrain_model",
        key="new_accuracy"
    )

    new_accuracy = float(new_accuracy)

    old_accuracy = -1.0

    if os.path.exists(PRODUCTION_METRICS_PATH):
        with open(PRODUCTION_METRICS_PATH, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("accuracy="):
                    old_accuracy = float(line.strip().split("=")[1])

    print(f"Old production accuracy: {old_accuracy}")
    print(f"New model accuracy: {new_accuracy}")

    if new_accuracy >= old_accuracy:
        shutil.copy2(new_model_path, PRODUCTION_MODEL_PATH)

        with open(PRODUCTION_METRICS_PATH, "w", encoding="utf-8") as f:
            f.write(f"model_path={PRODUCTION_MODEL_PATH}\n")
            f.write(f"source_model={new_model_path}\n")
            f.write(f"accuracy={new_accuracy}\n")
            f.write(
                f"promoted_at={datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}\n"
            )

        print("New model promoted to production.")
        decision = "promoted"
    else:
        print("New model was not better. It remains archived only.")
        decision = "archived_only"

    ti.xcom_push(key="decision", value=decision)

    return decision


default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
}


with DAG(
    dag_id="retrain_model_dag",
    description="Dynamiczne re-trenowanie modelu ML z wersjonowaniem i walidacją",
    default_args=default_args,
    start_date=datetime.datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ml", "retraining", "lab08"],
) as dag:

    retrain_task = PythonOperator(
        task_id="retrain_model",
        python_callable=retrain_model,
    )

    compare_and_promote_task = PythonOperator(
        task_id="compare_and_promote_model",
        python_callable=compare_and_promote_model,
    )

    retrain_task >> compare_and_promote_task