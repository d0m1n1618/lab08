Celem ćwiczenia było przygotowanie środowiska Apache Airflow oraz utworzenie procesu automatycznego re-trenowania modelu uczenia maszynowego.

W projekcie przygotowano DAG Airflow, który odpowiada za:

- wczytanie danych z pliku CSV,
- wytrenowanie modelu klasyfikacyjnego,
- obliczenie metryki accuracy,
- zapis modelu z wersjonowaniem czasowym,
- porównanie nowej wersji modelu z modelem produkcyjnym,
- warunkową promocję modelu do środowiska produkcyjnego.

Do klasyfikacji wykorzystano model `RandomForestClassifier` z biblioteki `scikit-learn`.

---

## Wykorzystane technologie

- Python
- Apache Airflow
- Docker
- Docker Compose
- pandas
- scikit-learn
- joblib

## Konfiguracja środowiska

Środowisko zostało uruchomione lokalnie z wykorzystaniem Docker Compose. Przygotowano obraz bazujący na Apache Airflow oraz doinstalowano biblioteki wymagane do trenowania modelu ML.

Plik requirements.txt zawiera:

pandas
scikit-learn
joblib

Plik Dockerfile buduje obraz Airflow z wymaganymi bibliotekami Python.

## Dane wejściowe

Do projektu wykorzystano zbiór danych Breast Cancer Wisconsin dostępny w bibliotece scikit-learn.

Zbiór został wygenerowany za pomocą pliku:

generate_dataset.py

Skrypt pobiera dane za pomocą funkcji:

load_breast_cancer()

Następnie tworzy plik:

data/new_data.csv

Plik zawiera cechy numeryczne oraz kolumnę:

target

Kolumna target jest etykietą klasy, którą przewiduje model.

## DAG Airflow

Główny plik DAG znajduje się w lokalizacji:

dags/retrain_model_dag.py

DAG nosi nazwę:

retrain_model_dag

Składa się z dwóch zadań:

retrain_model
compare_and_promote_model

## Zadania:

retrain_model → compare_and_promote_model
Zadanie retrain_model

Pierwsze zadanie odpowiada za re-trenowanie modelu. W ramach tego kroku wykonywane są następujące operacje:

Wczytanie danych z pliku:
/opt/airflow/data/new_data.csv
Oddzielenie cech od kolumny docelowej:
X = df.drop("target", axis=1)
y = df["target"]
Podział danych na zbiór treningowy i walidacyjny.
Wytrenowanie modelu:
RandomForestClassifier
Obliczenie metryki:
accuracy
Zapis modelu do folderu models z nazwą zawierającą timestamp.

zapisane wersje modeli:

rf_model_20260513_175440.pkl
rf_model_20260513_175741.pkl
rf_model_20260513_180642.pkl

Dla każdej wersji zapisywany jest również plik z metrykami, np.:

metrics_20260513_180642.txt

zawartość pliku metryk:

model_path=/opt/airflow/models/rf_model_20260513_180642.pkl
accuracy=0.935672514619883
timestamp=20260513_180642
Zadanie compare_and_promote_model

Drugie zadanie odpowiada za porównanie nowo wytrenowanego modelu z aktualnym modelem produkcyjnym.

Jeżeli nowy model uzyska accuracy większe lub równe aktualnemu modelowi produkcyjnemu, zostaje skopiowany do folderu:

models/production/

jako:

production_model.pkl

Dodatkowo zapisywany jest plik:

production_metrics.txt

zawartość pliku:

model_path=/opt/airflow/models/production/production_model.pkl
source_model=/opt/airflow/models/rf_model_20260513_180642.pkl
accuracy=0.935672514619883
promoted_at=20260513_180648

Oznacza to, że aktualnym modelem produkcyjnym została wersja:

rf_model_20260513_180642.pkl
Harmonogram

DAG został skonfigurowany z harmonogramem:

schedule_interval="@daily"

Oznacza to, że Airflow może uruchamiać proces re-trenowania raz dziennie.

DAG można także uruchomić ręcznie z poziomu interfejsu Airflow za pomocą przycisku:

Trigger DAG
Wyniki wykonania

DAG został uruchomiony kilkukrotnie z poziomu interfejsu Airflow. Wszystkie zadania zakończyły się statusem success.

W wyniku działania DAG-a utworzono kilka wersji modelu:

rf_model_20260513_175440.pkl
rf_model_20260513_175741.pkl
rf_model_20260513_180642.pkl

Dla każdej wersji zapisano osobny plik metryk:

metrics_20260513_175440.txt
metrics_20260513_175741.txt
metrics_20260513_180642.txt

Uzyskana wartość accuracy wyniosła:

0.935672514619883

Aktualnie wypromowany model produkcyjny znajduje się w folderze:

models/production/

## Wersjonowanie modeli

Wersjonowanie modeli zostało zrealizowane przez dodawanie timestampu do nazwy pliku modelu.

rf_model_20260513_180642.pkl

Dzięki temu każda kolejna wersja modelu jest zapisywana jako oddzielny plik. Pozwala to zachować historię trenowania i w razie potrzeby wrócić do wcześniejszej wersji.

## Mechanizm promocji modelu

Promocja modelu do środowiska produkcyjnego odbywa się warunkowo.

Nowy model zostaje ustawiony jako produkcyjny tylko wtedy, gdy jego accuracy jest większe lub równe accuracy aktualnego modelu produkcyjnego.

Jeżeli nowy model jest gorszy, pozostaje tylko w folderze models jako wersja archiwalna.