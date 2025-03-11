<div align="center">
   <img alt="OpenHEXA Logo" src="https://raw.githubusercontent.com/BLSQ/openhexa-app/main/hexa/static/img/logo/logo_with_text_grey.svg" height="80">
</div>
<p align="center">
    <em>Open-source Data integration platform</em>
</p>
<p align="center">
   <a href="https://github.com/BLSQ/openhexa-app/actions/workflows/test.yml">
      <img alt="Test Suite" src="https://github.com/BLSQ/openhexa-toolbox/actions/workflows/ci.yml/badge.svg">
   </a>
</p>

OpenHEXA Toolbox
================

OpenHEXA is an open-source data integration platform developed by [Bluesquare](https://bluesquarehub.com).

The Toolbox is an utility library to acquire and process data from various data sources. It is installed by default 
on OpenHEXA, and can be used both in notebooks and in data pipelines.

See the [Using the OpenHEXA Toolbox](https://github.com/BLSQ/openhexa/wiki/Using-the-OpenHEXA-Toolbox) section of the 
wiki for usage instructions.

## Installation

```sh
pip install openhexa.toolbox
```

## Modules

[**openhexa.toolbox.dhis2**](docs/dhis2.md) - Acquire and process data from DHIS2 instances <br>
[**openhexa.toolbox.iaso**](docs/iaso.md) - Acquire and process data from IASO instances <br>
[**openhexa.toolbox.kobo**](docs/kobo.md) - Acquire and process submissions from KoboToolbox instances <br>
[**openhexa.toolbox.hexa**](docs/hexa.md) - Interact with OpenHEXA API <br>
[**openhexa.toolbox.ERA**](openhexa/toolbox/era5/README.md) - Acquire and process data from ERA5


## Release
 
This project uses [release-please](https://github.com/googleapis/release-please) to manage releases using conventional commits.

To release a new version:

1. You need to have a least a commit with a conventional commit message (`feat|fix`) since the last release.
2. release-please will create a new release PR on GitHub.
3. Once the PR is merged, release-please will create a new release on GitHub.
4. A GitHub action will build the package on github release creation and upload it to PyPI and Anaconda.


