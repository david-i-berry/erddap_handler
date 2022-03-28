from datetime import datetime, timedelta, timezone
from flask import request, make_response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from functools import reduce
import os.path, os
import yaml
import json
import requests
from isodate import parse_duration
from datetime import datetime, timezone
import pandas as pd
import io
from erddap_handler import app
import logging

LOGGER = logging.getLogger(__name__)

WIS2NODE_DATA = os.environ.get("WIS2BOX_DATADIR")
INCOMING = f"{WIS2NODE_DATA}{os.sep}incoming"
DISCOVERY = f"{WIS2NODE_DATA}{os.sep}metadata{os.sep}discovery"
MCF_INDEX = f"{DISCOVERY}{os.sep}mcf_index.yaml"

limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"]
)

def get_mcf_file(topic_hierarchy, mcf_index):
    # check topic hierarchy in mcf_index dictionary
    try:
        mcf_file = reduce( lambda dict_, key_: dict_[key_],
                       topic_hierarchy.split("."),
                       mcf_index["index"])
    except KeyError as err:
        print("key not found")
        return False
    else:
        return mcf_file


@app.route('/')
def index():
    return 'Hello World!'


@app.route('/erddap/', methods=["GET"])
@limiter.limit("1 per minute")
def erddap():
    # get time called
    currenttime = datetime.now(timezone.utc)
    resulttime = currenttime.strftime("%Y-%m-%dT%H%M%SZ")
    # get topic hierarchy
    th = request.args.get("th")
    # load mcf index
    with open(MCF_INDEX) as fh:
        mcf_index = yaml.full_load(fh)
    # validate topic hierarchy and get mcf file name
    mcf_file = get_mcf_file(th, mcf_index)
    if not mcf_file:
        response = make_response("Error processing topic hierarchy", 400)
        return response
    # set output path for data based on th
    output_dir = th.replace(".",os.sep)
    output_dir = f"{INCOMING}{os.sep}{output_dir}"
    # remove the following when configured outside of app
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    errors = list()
    result = list()
    for f in mcf_file:
        with open(f"{DISCOVERY}{os.sep}{f}") as fh:
            mcf_metadata = yaml.full_load(fh)

        url = mcf_metadata["identification"]["url"]
        fmt = ".csv"  # fmt = ".geoJson"
        query = "?"
        period = mcf_metadata["identification"]["extents"]["temporal"][0]["resolution"]  # noqa
        mintime = currenttime - parse_duration(period)
        mintime = mintime.strftime("%Y-%m-%dT%H:%M:%SZ")
        filter = f"&time>={mintime}"
        url = f"{url}{fmt}{query}{filter}"
        id_field = mcf_metadata["wis2box"]["station_id"]
        if fmt == ".geoJson":
            try:
                data = json.loads(requests.get(url).text)
            except Exception as e:
                errors.append(e)
            try:
                with open(f"{output_dir}{os.sep}test.json","w") as fh:
                    fh.write(json.dumps(data))
            except Exception as e:
                errors.append(e)
        elif fmt == ".csv":
            # get data as string
            try:
                data_string = requests.get(url).content
            except Exception as e:
                errors.append(e)
            # now convert to stream like object
            try:
                fh = io.StringIO(data_string.decode("utf-8"))
            except Exception as e:
                errors.append(e)
            # now read using pandas
            try:
                data = pd.read_csv(fh)
                data = data.iloc[1:,:]  # drop row containing units
                try:
                    ids = data[id_field].unique()
                except:
                    print(data)
                # we want one id per file, split
                for id in ids:
                    subset = data[ data[id_field] == id ]
                    outfile = f"{output_dir}{os.sep}{id}_{resulttime}.csv"
                    subset.to_csv(outfile, index=False)
            except Exception as e:
                errors.append(e)
        else:
            response = make_response("Bad fmt requested", 400)
            return response
    if not errors:
        response = make_response("Success", 200)
    else:
        print(errors)
        response = make_response("Error, see logs", 400)
    return response
