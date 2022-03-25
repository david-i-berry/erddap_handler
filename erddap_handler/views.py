from datetime import datetime, timedelta, timezone
from flask import request, make_response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from functools import reduce
import os.path
import yaml
import json
import requests
from isodate import parse_duration
from datetime import datetime, timezone
import pandas as pd
import io
from erddap_handler import app

MCF_INDEX = "/local/data/metadata/discovery/mcf_index.yaml"
WIS2NODE_DATA = "/local/data/"

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

    with open( f"{WIS2NODE_DATA}{os.sep}metadata{os.sep}discovery{os.sep}{mcf_file}") as fh:  # NOQA
        mcf_metadata = yaml.full_load(fh)
    url = mcf_metadata["identification"]["url"]
    fmt = ".csv"  # fmt = ".geoJson"
    query = "?"
    period = mcf_metadata["identification"]["extents"]["temporal"][0]["resolution"]  # noqa
    currenttime = datetime.now(timezone.utc)
    mintime = currenttime - parse_duration(period)
    mintime = mintime.strftime("%Y-%m-%dT%H:%M:%SZ")
    filter = f"&time>={mintime}"
    url = f"{url}{fmt}{query}{filter}"
    if fmt == ".geoJson":
        data = json.loads(requests.get(url).text)
        with open("/local/data/test.json","w") as fh:
            fh.write(json.dumps(data))
        response = make_response("Success - json written to disk", 200)
    elif fmt == ".csv":
        # get data as string
        data_string = requests.get(url).content
        # now convert to stream like object
        fh = io.StringIO(data_string.decode("utf-8"))
        # now read using pandas
        data = pd.read_csv(fh)
        data.to_csv( "/local/data/test.csv", index=False)
        response = make_response("Success - csv written to disk", 200)
    else:
        response = make_response("Bad fmt requested", 400)
    return response
