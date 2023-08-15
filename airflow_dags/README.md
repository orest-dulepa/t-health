# TH Airflow DAGs repo

## Table of contents
- [TH Airflow DAGs repo](#th-airflow-dags-repo)
  - [Table of contents](#table-of-contents)
  - [General info](#general-info)
  - [Technologies](#technologies)
  - [Project Structure](#project-structure)
  - [DAG Structure](#dag-structure)
    - [setup](#setup)
    - [main](#main)
      - [extract](#extract)
      - [transform](#transform)
      - [load](#load)
      - [notify](#notify)
    - [cleanup](#cleanup)
  - [Local Setup](#local-setup)
    - [Prerequisites](#prerequisites)
  - [Production Setup](#production-setup)
  - [Dev Notes](#dev-notes)
    - [Table naming](#table-naming)
    - [Column naming](#column-naming)
  - [DAGs List](#dags-list)

## General info
Just a place for keeping and managing all data pipelines using Apache Airflow ecosystem.

## Technologies
Project is created with:
* Apache Airflow
* Python
* SQL
* boto3
	
## Project Structure
In progress (todo: add info about how to run sql from files also)

* `.aws`
  * `deployemnt`
* `.github`
  * `docs`
  * `workflows`
* `config`
* `dags`
* `plugins`
* `airflow.sh`
* `docker-compose.yaml`
* `Dockerfile`
* `requirements.txt`

## DAG Structure

### setup
Setup section
```yaml

```
### main
Main section
```yaml

```
* extract
* transform
* load
* notify

#### extract
Extract section

```yaml

```
#### transform
Transform section

```yaml

```
#### load
Load section

```yaml

```
#### notify
Notification section

```yaml

```
### cleanup
Cleanup section

```yaml

```
## Local Setup
How to set up and run it manually

### Prerequisites
To run this project locally:
1. [Install docker and docker compose](https://docs.docker.com/engine/install/) 


```

```

## Production Setup
In progress

## Dev Notes
In progress

### Table naming
The idea of naming the external tables based on the next pattern - `"ref_<organization name>_<team/sub-part of organization>_<data>"`

For instance:
* **Organization**: "_cms_" - "Centers for Medicare Services"
* **Team**: "_nppes_" - "National Plan and Provider Enumeration System"
* **Data**: "_npi_" - "National Provider ID"

### Column naming

* Must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_)
* Must begin with a letter or underscore
* Must be less than the maximum length of 59 characters.

So, long story short - the validation should convert columns names to lowercase, truncate to 59 characters long, and replace any invalid characters with "_"

## DAGs List
* `nppes_data_dag`
