# Local Development Guidelines with `airflowctl` and Apache Airflow 3.1.3

> **Goal:** Provide a simple, repeatable way to develop and test Airflow DAGs locally using `airflowctl`, then deploy them to a remote Airflow 3.1.3 environment.

## 1. Concepts & Tools

* **Apache Airflow 3.1.3**

  * Python-based workflow orchestrator.
  * Requires Python 3.10–3.13. ([Apache Airflow][2])
  * Ships with a modern React UI, new REST API behavior, and a split CLI: `airflow` for local instance operations, and `airflowctl` (official) for remote/API-driven operations. ([Apache Airflow][3])

* **This `airflowctl` (project manager CLI)**

  * A separate CLI (same name) that:

    * Creates a standardized Airflow project on your machine (`init`).
    * Manages a dedicated virtualenv and installs a specific Airflow version (`build`).
    * Starts/stops local Airflow components and shows logs (`start`, `stop`, `logs`). ([GitHub][1])
  * Uses a `settings.yaml` file for Airflow/Python versions, and `.env` for environment variables.

> ⚠️ **Name clash note:**
>
> * **Local dev:** this doc uses `airflowctl` = *project manager* CLI (`airflowctl init/build/start/...`). ([GitHub][1])
> * **Remote ops:** the **official** `apache-airflow-ctl` (also invoked as `airflowctl`) talks to Airflow via REST API for remote management. ([PyPI][4])
>   When in doubt, check `airflowctl --help` to see which one you installed.

---

## 2. Prerequisites

* **OS:** Any modern Linux/macOS/WSL.
* **Python:** 3.11 recommended (supported by Airflow 3.1.3). ([Apache Airflow][2])
* **VS Code:**
  * Python extension.

---

## 3. Installing `airflowctl` for Local Dev

In a global tools environment (or a dev-specific one):

```bash
pip install "airflowctl"
```

This installs the CLI that can:

* Initialize a project and write `settings.yaml`.
* Install **Apache Airflow 3.1.3** inside a venv.
* Start the webserver/scheduler and stream logs. ([GitHub][1])

> **Recommendation:** Pin the version of `airflowctl` in a `requirements-dev.txt` to avoid surprises:
>
> ```txt
> airflowctl==0.2.11
> ```

---

## 4. Creating a New Local Airflow Project

From the folder where you keep infra/dev projects:

```bash
airflowctl init etl_orchestrator \
  --airflow-version 3.1.3 \
  --python-version 3.11
```

Or, to immediately build the virtualenv & start Airflow:

```bash
airflowctl init etl_orchestrator \
  --airflow-version 3.1.3 \
  --python-version 3.11 \
  --build-start
```

This creates a directory like: ([GitHub][1])

```text
etl_orchestrator/
  .env
  .gitignore
  dags/
    example_dag_basic.py
  plugins/
  requirements.txt
  settings.yaml
```

Key files:

* `settings.yaml`

  * Central config for local dev:

    * `airflow_version: "3.1.3"`
    * `python_version: "3.11"`
    * `mode` → venv manager settings (e.g. `uv` or `pyenv`).
    * Optional: predefined **connections** and **variables**.
* `.env`

  * Environment variables (including Airflow config overrides).
* `dags/`

  * Your DAG definitions go here.
* `plugins/`

  * Custom operators/hooks/macros.
* `requirements.txt`

  * Extra Python dependencies for your DAGs.

---

## 5. Building the Airflow Environment

If you didn’t use `--build-start`, do this inside the project:

```bash
cd etl_orchestrator
airflowctl build
```

`airflowctl build` will: ([GitHub][1])

* Create the virtualenv (using `uv` or `pyenv` as needed).
* Install **Apache Airflow 3.1.3** and your `requirements.txt`.
* Wire up `AIRFLOW_HOME` and related paths.

> **Tip:** Commit `settings.yaml` and `requirements.txt`.
> Don’t commit the `.venv/` folder or SQLite DB files.

---

## 6. Running Airflow Locally

From the project directory:

### 6.1 Start services

```bash
airflowctl start etl_orchestrator --background
# or, from inside the project:
airflowctl start .
```

This will:

* Activate the virtualenv.
* Start **webserver**, **scheduler**, and **triggerer** for your local instance. ([GitHub][1])

Access the UI at:

* [http://localhost:8080](http://localhost:8080) (default). ([GitHub][1])

### 6.2 View logs

```bash
airflowctl logs etl_orchestrator
```

Filter by component: ([GitHub][1])

```bash
# Scheduler only
airflowctl logs etl_orchestrator -s

# Webserver only
airflowctl logs etl_orchestrator -w

# Scheduler + webserver
airflowctl logs etl_orchestrator -s -w
```

### 6.3 Stop services

```bash
airflowctl stop etl_orchestrator
```

---

## 7. Project Structure & Code Organization

Recommended layout (you can adapt names):

```text
etl_orchestrator/
  dags/
    etl/
      __init__.py
      customers_etl_dag.py
      orders_etl_dag.py
    utils/
      __init__.py
      extract.py
      transform.py
      load.py
  plugins/
    __init__.py
    hooks/
    operators/
  tests/
    test_transform.py
  requirements.txt
  settings.yaml
  .env
```

**Guidelines:**

1. **Keep DAG files thin**

   * DAGs should mainly define:

     * schedule, default_args, dependencies.
   * Heavy business logic goes into `etl/utils/*.py`.

2. **Use importable modules**

   * Treat `etl_orchestrator` (or subdir) as a Python package by adding `__init__.py`.
   * This keeps imports stable:

     ```python
     from dags.etl.utils.transform import normalize_customers
     ```

3. **Align local and remote**

   * Folder structure under `dags/` should match what you will deploy to remote VMs.
   * Avoid hard-coding local-only paths (e.g. `/Users/...`).

---

## 8. Configuring Airflow for Local Dev

`airflowctl` starts you with **SQLite + SequentialExecutor**, which is fine for basic development. ([GitHub][1])

### 8.1 Environment variables via `.env`

You can override any Airflow setting with env vars in `.env`, e.g.:

```bash
# Example: use LocalExecutor instead of SequentialExecutor
AIRFLOW__CORE__EXECUTOR=LocalExecutor
# For Airflow >= 2.6, to skip SQLite compatibility check:
_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK=1

# Example: change database (if running local Postgres)
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
```

Then restart:

```bash
airflowctl stop etl_orchestrator
airflowctl start etl_orchestrator --background
```

(Only do this if you *need* parallelism; SQLite is good enough for many dev cases.) ([GitHub][1])

### 8.2 Managing connections & variables

You have 2 main options:

1. **Declare in `settings.yaml`** (recommended for reproducible dev): ([GitHub][1])

   ```yaml
   connections:
     - conn_id: my_postgres
       conn_type: postgres
       host: localhost
       port: 5432
       login: airflow
       password: airflow
       schema: analytics

   variables:
     - key: S3_BUCKET
       value: my-local-bucket
       description: Default bucket for dev ETL
   ```

   `airflowctl` can bootstrap these into your project environment.

2. **Create via UI or CLI**

   * Use Airflow UI → Admin → Connections / Variables.
   * Or via CLI:

     ````bash
     airflowctl airflow connections add ...
     airflowctl airflow variables set ...
     ``` :contentReference[oaicite:17]{index=17}  
     ````

---

## 9. Typical Local Dev Workflow (VS Code + `airflowctl`)

1. **Create/Update code**

   * Edit DAGs in `dags/` and transformation code in `dags/etl/utils/`.
   * Add any dependencies to `requirements.txt`.

2. **Rebuild dependencies (if needed)**

   ```bash
   airflowctl build   # if you changed requirements/settings
   ```

3. **Start / restart local Airflow**

   ```bash
   airflowctl start etl_orchestrator --background
   ```

4. **Develop and test DAGs**

   * Use the web UI to:

     * Unpause DAGs.
     * Trigger manual runs.
     * Inspect logs, XComs, and task instances.

5. **Run unit tests (outside Airflow)**

   * In VS Code:

     * Use pytest to test `etl/utils/*.py` directly.
   * This keeps DAGs small and tests fast.

6. **Iterate**

   * Change code → refresh web UI → re-trigger DAG.

---

## 10. Using the Underlying `airflow` CLI via `airflowctl`

You can run raw `airflow` commands in the project’s venv without manually activating it:

```bash
# From inside the project:
airflowctl airflow dags list
airflowctl airflow info
airflowctl airflow version
```

This is equivalent to:

```bash
source .venv/bin/activate
source .env
airflow dags list
```

… but is handled automatically by `airflowctl`. ([GitHub][1])

Useful commands:

* `airflowctl airflow dags list`
* `airflowctl airflow dags show my_dag_id`
* `airflowctl airflow tasks test my_dag_id task_id 2025-01-01`
* `airflowctl airflow config get-value core executor`

---

## 11. Working with Remote Airflow (VM Deployment)

**Pattern:**

1. **Local dev** with `airflowctl`:

   * Develop DAGs + ETL code.
   * Verify they run in local Airflow 3.1.3.

2. **Version control**

   * Commit only:

     * `dags/`, `plugins/`, `requirements.txt`, `settings.yaml`, `tests/`, `.env.example`.
   * Ignore:

     * `.venv/`, `logs/`, SQLite DB files, and other local artifacts.

3. **Remote environment (on VMs)**

   * Install **Apache Airflow 3.1.3** using the official pip + constraints method. ([Apache Airflow][5])
   * Set `dags_folder` to your code directory (from Git).
   * Use **systemd/Docker** or other orchestration to run `airflow webserver`, `airflow scheduler`, etc.

4. **Deploy**

   * CI/CD (or simple script) pulls the Git repo onto the VM(s).
   * After deploy, reload or restart scheduler/webserver.

5. **Remote operations (optional)**

   * Use the **official** `apache-airflow-ctl` CLI (also named `airflowctl`) to interact with the remote instance over REST:

     * Trigger DAG runs.
     * Inspect dagruns and tasks.
     * Pause/unpause DAGs. ([PyPI][4])

> The key idea: **`airflowctl` (local project manager) for dev; `airflow` + `apache-airflow-ctl` for production/remote.**

---

## 12. Versioning & Upgrades

* **Pin Airflow in `settings.yaml`**:

  ```yaml
  airflow_version: "3.1.3"
  python_version: "3.11"
  ```

* Follow the **Airflow 3 upgrade guide** when moving across 3.x versions (3.1.3 → 3.2, etc.), especially if you share a metadata DB between environments. ([Apache Airflow][6])

---

## 13. Recommended VS Code Settings (Optional)

In `.vscode/settings.json`:

```json
{
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
  "python.testing.pytestEnabled": true,
  "python.testing.pytestArgs": [
    "tests"
  ],
  "python.formatting.provider": "black",
  "editor.formatOnSave": true
}
```

You can add a debug configuration that:

* Activates the venv.
* Loads `.env`.
* Runs a specific ETL function or a small harness script to test transformations outside of Airflow.

---

[1]: https://github.com/kaxil/airflowctl "GitHub - kaxil/airflowctl: A CLI tool to streamline getting started with Apache Airflow™ and managing multiple Airflow projects"
[2]: https://airflow.apache.org/docs/apache-airflow/stable/start.html?utm_source=chatgpt.com "Quick Start — Airflow 3.1.3 Documentation - Apache Airflow"
[3]: https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html?utm_source=chatgpt.com "Release Notes — Airflow 3.1.3 Documentation"
[4]: https://pypi.org/project/apache-airflow-ctl/?utm_source=chatgpt.com "apache-airflow-ctl"
[5]: https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html?utm_source=chatgpt.com "Installation of Airflow® — Airflow 3.1.3 Documentation"
[6]: https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html?utm_source=chatgpt.com "Upgrading to Airflow 3 — Airflow 3.1.3 Documentation"
