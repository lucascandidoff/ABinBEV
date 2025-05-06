# 🏗️ Local Data Pipeline: Breweries Data Processing

This project implements a simple data pipeline using Apache Airflow, Apache Spark (PySpark), and Delta Lake, orchestrated within Docker containers using Docker Compose.

The pipeline processes brewery data through standard data lake layers:

* **Bronze:** Raw, ingested data.
* **Silver:** Cleaned, transformed, and structured data.
* **Gold:** Curated data ready for analysis (dimensional model and aggregated tables).

## 🚀 Technologies Used

* **Apache Airflow:** For workflow orchestration and scheduling.
* **Apache Spark (PySpark):** For data processing and transformation.
* **Delta Lake:** For reliable storage of data lake tables (ACID properties).
* **PostgreSQL:** As the metadata database for Airflow.
* **Docker & Docker Compose:** For containerization and managing services.
* **Python:** For writing DAGs and Spark scripts.

## 📋 Prerequisites

Before you begin, ensure you have the following installed on your machine:

* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Engine and Docker Compose v2+)
* Sufficient disk space (data will be stored locally).
* At least 8GB of RAM is recommended for running Spark processes within Docker, although it might run with less depending on the data size.

## 📁 Project Structure

├── dags/                # Airflow DAG files
│   └── brewery_pipeline_dag.py
├── scripts/             # PySpark transformation scripts
│   ├── bronze_layer.py
│   ├── silver_layer.py
│   └── gold_layer.py    # Your code to generate dimensions, fact, and aggregated table
├── data/                # Local storage for Delta Lake tables (Bronze, Silver, Gold)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── Dockerfile           # Defines the custom Airflow image with Spark/Delta dependencies
├── docker-compose.yaml  # Defines the services (Airflow Webserver, Scheduler, Postgres, CLI)
└── requirements.txt     # Python dependencies for Airflow and Spark scripts

*(Note: The `data` folder will be created and populated when you run the pipeline.)*

## ⚙️ Setup and Running Locally

Follow these steps to get the project up and running:

1.  **Clone the repository:**

    ```bash
    git clone <URL_DO_SEU_REPOSITORIO>
    cd <NOME_DA_PASTA_DO_REPOSITORIO>
    ```

2.  **Build the Docker image:**
    This step builds the custom Airflow image including PySpark, Delta Lake, and other required Python dependencies defined in `requirements.txt`.

    ```bash
    docker compose build
    ```

3.  **Start the Docker containers:**
    This will start the PostgreSQL database, Airflow webserver, scheduler, and a CLI service in detached mode (`-d`).

    ```bash
    docker compose up -d
    ```

4.  **Initialize Airflow Database and Create a User:**
    On the first run, Airflow needs to set up its database and create an administrator user. Wait a moment for the `postgres` and `airflow-webserver` containers to start before running these commands.

    ```bash
    # Initialize the database
    docker compose run airflow-cli airflow db migrate

    # Create an admin user
    # Replace <USERNAME> and <PASSWORD> with your desired credentials
    docker compose run airflow-cli airflow users create \
        --username <USERNAME> \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password <PASSWORD>
    ```

    *(Note: You might see warnings during `db migrate`, which are often normal for the first setup.)*

## ▶️ Running the Data Pipeline

1.  **Access the Airflow UI:**
    Open your web browser and go to:
    ```
    http://localhost:8080
    ```

2.  **Log in:**
    Use the username and password you created in the setup step.

3.  **Find and Unpause the DAG:**
    Look for the DAG named `brewery_pipeline_dag` (or whatever name you gave your DAG file in `dags/`). Toggle the switch next to the DAG name from `Off` to `On`.

4.  **Trigger a DAG Run:**
    Click the "Play" button (Trigger DAG) for the `brewery_pipeline_dag`.

5.  **Monitor the Run:**
    You can monitor the progress of the DAG run in the Airflow UI using the Grid View, Graph View, or by checking the Task Logs.

## 👀 Accessing Data and Logs

* **Delta Lake Data:** The processed data in Bronze, Silver, and Gold layers is stored in the `./data` folder on your local machine. You can inspect the contents (which will include Parquet files and Delta transaction logs) using file explorer.
* **Airflow Task Logs:** Logs for each task run are stored in the `./logs` folder on your local machine, mirrored from inside the container.
* **Container Logs:** You can view the logs of individual Docker containers from your terminal:
    ```bash
    docker compose logs airflow-webserver
    docker compose logs airflow-scheduler
    docker compose logs postgres
    # Or logs for other services if you added them
    ```

## 🛑 Stopping the Project

To stop all running services defined in the `docker-compose.yaml`:

```bash
docker compose down
