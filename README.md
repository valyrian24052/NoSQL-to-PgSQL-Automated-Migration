# MongoDB to PostgreSQL Data Migration

This project is designed to migrate data from a MongoDB database to a PostgreSQL database. The script fetches the schema from MongoDB, processes the data, and inserts it into PostgreSQL. The process can be configured to run one-time or incrementally at specified intervals.
## Directory Structure
![image](https://github.com/user-attachments/assets/89c0fb6a-5af9-4a8c-8625-87ab8426e195)

## Prerequisites

- Python 3.x
- MongoDB
- PostgreSQL

## Installation

1. Clone the repository to your local machine:
    ```bash
    https://github.com/valyrian24052/NoSQL-to-PgSQL-Automated-Migration.git
    cd mongo-to-postgres-migration
    ```

2. Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

Before running the script, update the `input.py` file with your MongoDB and PostgreSQL connection strings.

### `input.py`

```python
# input.py

# MongoDB connection string
Mongoconnstr = "input mongo conn string here"

# PostgreSQL connection string
pgconnstr = 'input postgre server conn string here'

"""
Change the type to -->incremental<-- to update the data recurring at some time,
and update the time difference in minutes to set the time period.
"""
type = 'one-time'
timediff = 60
