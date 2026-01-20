import importlib.resources
import tomllib

from ecmwf.datastores import Remote

from openhexa.toolbox.era5.models import Variable


def get_name(remote: Remote) -> str:
    """Create file name from remote request.

    Returns:
        File name with format: {year}{month}_{request_id}.{ext}

    """
    request = remote.request
    data_format = request["data_format"]
    download_format = request["download_format"]
    year = request["year"]
    month = request["month"]
    ext = "zip" if download_format == "zip" else data_format
    return f"{year}{month}_{remote.request_id}.{ext}"


def get_variables() -> dict[str, Variable]:
    """Load ERA5-Land variables metadata.

    Returns:
        A dictionary mapping variable names to their metadata.

    """
    with importlib.resources.files("openhexa.toolbox.era5").joinpath("data/variables.toml").open("rb") as f:
        return tomllib.load(f)
