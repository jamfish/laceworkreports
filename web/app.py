import typing
from typing import Dict as typing_dict

import logging
import os
from pathlib import Path

import jinja2
import pandas as pd
import sqlalchemy_utils
from flask import Flask
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy_utils.functions import create_database, database_exists

logging.basicConfig(level=logging.INFO)

# sqlite database
db_path = Path("./database/database.db")
db_connection = f"sqlite:///{db_path.absolute()}?check_same_thread=False"

# jinja templates
template_folder = Path("./templates")
app = Flask(__name__, template_folder=template_folder.absolute())
fileloader = jinja2.FileSystemLoader(searchpath=template_folder.absolute())
env = jinja2.Environment(
    loader=fileloader, extensions=["jinja2.ext.do"], autoescape=True
)


def sqlite_queries(
    queries: typing_dict[typing.Any, typing.Any],
    db_table: typing.Any,
    db_connection: typing.Any,
    custom_columns: typing.Any = None,
) -> typing_dict[typing.Any, typing.Any]:

    logging.info("Generating query results")
    engine = create_engine(db_connection, echo=False)
    conn = engine.connect()

    results = {}
    for query in queries.keys():
        logging.debug(f"Executing query: {query}")
        sql_query = queries[query]
        if db_table is not None:
            sql_query = sql_query.replace(":db_table", db_table)
        else:
            sql_query = sql_query.replace(":db_table", "")

        if custom_columns is not None:
            sql_query = sql_query.replace(":custom_columns", custom_columns)
        else:
            sql_query = sql_query.replace(":custom_columns", "")

        df = pd.read_sql_query(
            sql=sql_query,
            con=conn,
        )
        results[query] = df.to_dict(orient="records")

    logging.info("Queries complete")
    return results


def get_inventory():
    results = sqlite_queries(
        queries={
            "report": """
                        SELECT 
                            csp,
                            json_extract(cloudDetails,'$.accountID') AS accountID,
                            json_extract(cloudDetails,'$.accountAlias') AS acountAlias,
                            startTime,
                            endTime,
                            :custom_columns
                            resourceId,
                            resourceRegion,
                            resourceType,
                            resourceTags,
                            service,
                            status,
                            resourceConfig
                        FROM
                            :db_table AS dm
                        LIMIT 10000
                        """,
        },
        db_table="inventory",
        custom_columns=None,
        db_connection=db_connection,
    )
    logging.info(results)
    # results = [{"id": 1, "test": "value"}]

    return [
        {
            "name": "inventory_coverage",
            "summary": {
                "reportTitle": "Inventory Coverage",
                "description": "The Inventory Coverage Report displays a list of all Lacework Inventory.",
                "rows": len(results["report"]),
            },
            "report": results["report"],
        }
    ]


@app.route("/", methods=["GET"])
def home():
    template = env.get_template("inventory.html.j2")

    return template.render(datasets=get_inventory())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
