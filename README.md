# MongoDB to PostgreSQL Data Migration

This project is designed to migrate data from a MongoDB database to a PostgreSQL database. The script fetches the schema from MongoDB, processes the data, and inserts it into PostgreSQL. The process can be configured to run one-time or incrementally at specified intervals.

## Prerequisites

- Python 3.x
- MongoDB
- PostgreSQL

## Installation

1. Clone the repository to your local machine:
    ```bash
    git clone https://github.com/your-repo/mongo-to-postgres-migration.git
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