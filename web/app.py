from flask import Flask
from pathlib import Path
import jinja2
import pandas as pd
import sqlalchemy_utils
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy_utils.functions import create_database, database_exists
from typing import Dict as typing_dict
import typing
import os
import logging

logging.basicConfig(level=logging.INFO)

# sqlite database
db_path = Path("./databse/database.db")
db_connection = f"sqlite:///{db_path.absolute()}?check_same_thread=False"

# jinja templates
template_folder = Path('./templates')
app = Flask(__name__, template_folder=template_folder.absolute())
fileloader = jinja2.FileSystemLoader(
    searchpath=template_folder.absolute()
)
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
        if custom_columns is not None:
            sql_query = sql_query.replace(":custom_columns", custom_columns)

        df = pd.read_sql_query(
            sql=sql_query,
            con=conn,
        )
        results[query] = df.to_dict(orient="records")

    logging.info("Queries complete")
    return results

def get_inventory():
    sqlite_queries(
        queries=InventoryQueries,
        db_table="inventory",
        custom_columns=tag_column_query,
        db_connection=db_connection,
    )
        
    # results = [
    #         {
    #             'id': 1,
    #             'test': 'value'
    #         }
    #     ]

    return [{
        'name': 'inventory_coverage',
        'summary': {
            'reportTitle': 'Inventory Coverage',
            'description': 'The Inventory Coverage Report displays a list of all Lacework Inventory.',
            'rows': 1
        },
        'report': results
    }]
    
              
            


@app.route('/', methods=['GET'])
def home():
    template = env.get_template('inventory.html.j2')
    
    return template.render(
        datasets=get_inventory()
    )


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)