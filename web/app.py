import typing
from typing import Dict as typing_dict

import json
import logging
import os
from pathlib import Path

import jinja2
import pandas as pd
import sqlalchemy_utils
from flask import Flask, jsonify, request
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


def get_inventory(
    offset=0, limit=10, sort=None, order=None, filter=None, download=None
):
    # if we have offset=0 and limit=0 return all (download)
    if download == "csv":
        limit_filter = ""
    else:
        limit_filter = f"LIMIT {limit} OFFSET {offset}"

    if sort is not None:
        sort_filter = f"ORDER BY {sort} {order}"
    else:
        sort_filter = ""

    if filter is not None:
        filter = json.loads(filter)
        filters = []
        for field in filter.keys():
            filters.append(f"{field} LIKE '%{filter[field]}%'")
        ors = " OR ".join(filters)
        filter_filter = f"WHERE {ors}"
    else:
        filter_filter = ""

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
                        :filter
                        :sort
                        :limit
                        """.replace(
                ":sort", sort_filter
            )
            .replace(":limit", limit_filter)
            .replace(":filter", filter_filter),
            "total_filtered": """
                        SELECT 
                            count(*) as count
                        FROM
                            :db_table AS dm
                        :filter
                        """.replace(
                ":filter", filter_filter
            ),
            "total": """
                        SELECT 
                            count(*) as count
                        FROM
                            :db_table AS dm
                        """,
        },
        db_table="inventory",
        custom_columns=None,
        db_connection=db_connection,
    )
    # logging.info(results)
    # results = [{"id": 1, "test": "value"}]
    if download == "csv":
        return results["report"]

    return [
        {
            "name": "inventory_coverage",
            "summary": {
                "reportTitle": "Inventory Coverage",
                "description": "The Inventory Coverage Report displays a list of all Lacework Inventory.",
                "rows": results["total"][0]["count"],
                "rows_filtered": results["total_filtered"][0]["count"],
            },
            "report": results["report"],
        }
    ]


@app.route("/", methods=["GET"])
def home():
    template = env.get_template("inventory.html.j2")

    return template.render(datasets=get_inventory())


@app.route("/inventory", methods=["GET"])
def inventory():
    template = env.get_template("inventory2.html.j2")

    return template.render(datasets=get_inventory(limit=100))


@app.route("/data/inventory", methods=["GET", "POST"])
def data_inventory():
    logging.info(f"request: {request.json}")
    offset = request.json.get("offset", 0)
    limit = request.json.get("limit", 0)
    sort = request.json.get("sort", None)
    order = request.json.get("order", None)
    filter = request.json.get("filter", None)
    download = request.json.get("download", None)
    result = get_inventory(
        offset=offset,
        limit=limit,
        sort=sort,
        order=order,
        filter=filter,
        download=download,
    )

    if download == "csv":
        return pd.DataFrame(result).to_csv(index=False)

    return jsonify(result)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
