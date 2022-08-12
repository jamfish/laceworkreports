"""
Example script showing how to use the LaceworkClient class.
"""

import csv
import json
import logging
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from laceworksdk import LaceworkClient

logging.basicConfig(level=logging.INFO)

load_dotenv()

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class ReferenceLookup:
    def __init__(self, key, field, dict, multivalue=False):
        self.key = key
        self.field = field
        self.dict = dict
        self.multivalue = multivalue

    def lookup(self, value, default=None):
        # only return the first matching result or default value
        dict = list(filter(lambda x: x[self.key] == value, self.dict))

        rows = []
        for row in dict:
            # return the entire row
            if self.field is None:
                rows.append(row)
            else:
                for i in self.field.split("."):
                    if i in row:
                        row = row[i]
                    else:
                        row = default
                    rows.append(row)

        # return all multiple values
        if self.multivalue:
            return rows

        # return first value only
        else:
            return rows.pop() if len(rows) > 0 else default


class DataHandler:
    def __init__(self, format, file_path="export.csv"):
        if format not in ["csv", "dict"]:
            raise Exception(
                f"Unsupported export format, expected csv or dict found: {format}"
            )

        self.format = format
        self.file_path = file_path

    def __open(self):
        if self.format == "csv":
            self.header = False
            self.fp = open(self.file_path, "w")
            self.writer = csv.writer(self.fp, quoting=csv.QUOTE_ALL)
            self.dataset = csv.reader(self.fp)
        else:
            self.dataset = []

    def __close(self):
        if self.format == "csv":
            self.fp.close()

    def insert(self, row):
        if self.format == "csv":
            if not self.header:
                self.writer.writerow(row.keys())
                self.header = True

            self.writer.writerow(row.values())
        elif self.format == "dict":
            self.dataset.append(row)

    def get(self):
        return self.dataset

    def __enter__(self):
        self.__open()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.__close()


def query(
    client,
    type,
    object=None,
    start_time=None,
    end_time=None,
    filters=None,
    returns=None,
    dataset=None,
):

    if start_time is None:
        start_time = datetime.now(timezone.utc) + timedelta(days=-1)
    if end_time is None:
        end_time = datetime.now(timezone.utc)
    if filters is None:
        filters = []

    # build query string
    q = {
        "timeFilter": {
            "startTime": start_time.strftime(ISO_FORMAT),
            "endTime": end_time.strftime(ISO_FORMAT),
        },
        "filters": filters,
        "returns": returns,
    }

    if dataset is not None:
        q["dataset"] = dataset

    # create reference to search object
    if object is not None:
        obj = getattr(getattr(client, f"{type}"), f"{object}")
    else:
        obj = getattr(client, f"{type}")

    # return query result reference
    return obj.search(json=q)


def lookup(key, dict, default=None):
    for i in key.split("."):
        if i in dict:
            dict = dict[i]
        else:
            return default

    return dict


def flatten_json(y):
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + "_")
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + "_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def map_fields(data, field_map=None):
    if field_map is None:
        # flatten json
        data = flatten_json(data)
        field_map = {}
        for key in data.keys():
            field_map[key] = key

    result = {}
    for field in field_map.keys():
        # for reference field find the matching local key and lookup the field value
        if isinstance(field_map[field], ReferenceLookup):
            result[field] = field_map[field].lookup(lookup(field_map[field].key, data))
        else:
            result[field] = lookup(field_map[field], data)

    return result


def export(format, results, field_map=None, file_path="export.csv"):
    with DataHandler(format, file_path=file_path) as h:
        # process results
        for result in results:
            for data in result["data"]:
                # create the data row
                try:
                    row = map_fields(data=data, field_map=field_map)
                except Exception as e:
                    logging.error(f"Failed to map fields for data: {data}")
                    raise Exception(e)

                h.insert(row)

        # return
        return h.get()


if __name__ == "__main__":

    client = LaceworkClient()

    # scenario 1 - export a list of machines from inventory
    export(
        "csv",
        query(
            client=client,
            type="inventory",
            object=None,
            filters=[
                {"field": "resourceType", "expression": "eq", "value": "ec2:instance"}
            ],
            dataset="AwsCompliance",
        ),
        # field_map={
        #     "start_time": "startTime",
        #     "end_time": "endTime",
        #     "mid": "mid",
        #     "tags": "machineTags",
        #     "hostname": "hostname",
        #     "public_ip": "machineTags.ExternalIp",
        # },
        file_path="export_machines.csv",
    )
