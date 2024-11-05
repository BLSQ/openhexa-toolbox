# Changelog

## [1.4.1](https://github.com/BLSQ/openhexa-toolbox/compare/v1.4.0...v1.4.1) (2024-11-05)


### Bug Fixes

* **era5:** missing rasterio submodule import ([0e47337](https://github.com/BLSQ/openhexa-toolbox/commit/0e47337059fe504afe7fbc728f2bd27b264c76e5))

## [1.4.0](https://github.com/BLSQ/openhexa-toolbox/compare/v1.3.0...v1.4.0) (2024-11-05)


### Features

* **era5:** support sum aggregation for accumulated variables ([f75aa3a](https://github.com/BLSQ/openhexa-toolbox/commit/f75aa3abfb0ddcc1030b28a3630baef203cc8d27))

## [1.3.0](https://github.com/BLSQ/openhexa-toolbox/compare/v1.2.0...v1.3.0) (2024-11-05)


### Features

* **era5:** add weekly and monthly aggregation ([f1292a1](https://github.com/BLSQ/openhexa-toolbox/commit/f1292a12b5e6e14e6b98f3b674e68cd353f4db06))

## [1.2.0](https://github.com/BLSQ/openhexa-toolbox/compare/v1.1.4...v1.2.0) (2024-10-23)


### Features

* add ERA5 download and aggregation ([#61](https://github.com/BLSQ/openhexa-toolbox/issues/61)) ([48cb5ce](https://github.com/BLSQ/openhexa-toolbox/commit/48cb5ce66fab7f4ddd740c1e3e8ccbb682b22a90))
* **DHSI2:** add put method ([#63](https://github.com/BLSQ/openhexa-toolbox/issues/63)) ([c79aac3](https://github.com/BLSQ/openhexa-toolbox/commit/c79aac3e7b1b820e4e82282bb6e8d904770723b1))
* support both urgUnits and orgunits from IASO ([a319874](https://github.com/BLSQ/openhexa-toolbox/commit/a31987475c8eec7b05de7aedecf514ae7884cc83))


### Bug Fixes

* changed default field for orgunits ([a281f2f](https://github.com/BLSQ/openhexa-toolbox/commit/a281f2f5e2bb56c5e0b8f481258a77758565a258))
* tests for iaso orgunits ([042ffdf](https://github.com/BLSQ/openhexa-toolbox/commit/042ffdf4c02fbbe653a6e36aeb967ef4fa9a870b))

## [1.1.4](https://github.com/BLSQ/openhexa-toolbox/compare/v1.1.3...v1.1.4) (2024-09-26)


### Bug Fixes

* **kobo:** do not try to cast null values ([74b49e1](https://github.com/BLSQ/openhexa-toolbox/commit/74b49e14d66a88da4264879f46f7a93d4353c112))
* **kobo:** support string calculate fields ([49a6e63](https://github.com/BLSQ/openhexa-toolbox/commit/49a6e63703a59571d4507b218d479a70d63b53cc))

## [1.1.3](https://github.com/BLSQ/openhexa-toolbox/compare/v1.1.2...v1.1.3) (2024-09-26)


### Bug Fixes

* compatibility with polars &gt; 0.20.30 ([738fe8d](https://github.com/BLSQ/openhexa-toolbox/commit/738fe8d4e9b08917bc3354718bd335afac8c12a0))

## [1.1.2](https://github.com/BLSQ/openhexa-toolbox/compare/v1.1.1...v1.1.2) (2024-09-23)


### Bug Fixes

* **dhis2:** do not raise authentication errors for status codes 406 ([d0af883](https://github.com/BLSQ/openhexa-toolbox/commit/d0af883d9b710073cbb9aa8c73698017fc6d6e97))

## [1.1.1](https://github.com/BLSQ/openhexa-toolbox/compare/v1.1.0...v1.1.1) (2024-09-23)


### Bug Fixes

* **dhis2:** do not raise authentication errors for old versions ([3e0eb93](https://github.com/BLSQ/openhexa-toolbox/commit/3e0eb93321dbab65c973847220298f0dad484424))

## [1.1.0](https://github.com/BLSQ/openhexa-toolbox/compare/v1.0.2...v1.1.0) (2024-08-27)


### Features

* **openHEXA:** add openHEXA client ([#49](https://github.com/BLSQ/openhexa-toolbox/issues/49)) ([5d23150](https://github.com/BLSQ/openhexa-toolbox/commit/5d23150ee7acdb51968c0191a2cf5ac5fa1d749d))

## [1.0.2](https://github.com/BLSQ/openhexa-toolbox/compare/v1.0.1...v1.0.2) (2024-08-13)


### Bug Fixes

* **dhis2:** fix lastUpdated and lastUpdatedDuration params ([883de3b](https://github.com/BLSQ/openhexa-toolbox/commit/883de3ba0b22905887e376e96747e7d3f760328d))

## [1.0.1](https://github.com/BLSQ/openhexa-toolbox/compare/1.0.0...v1.0.1) (2024-08-13)


### Bug Fixes

* **dhis2:** allow single lastUpdated param ([#50](https://github.com/BLSQ/openhexa-toolbox/issues/50)) ([df64006](https://github.com/BLSQ/openhexa-toolbox/commit/df6400664c6097e04dbaa968cebb4fdedb41c487))

## [1.0.0](https://github.com/BLSQ/openhexa-toolbox/compare/0.2.6...v1.0.0) (2024-07-26)

### Features

* **dhis2:** add caching for all GET requests ([#46](https://github.com/BLSQ/openhexa-toolbox/issues/46)) ([710fafb](https://github.com/BLSQ/openhexa-toolbox/commit/710fafb01a6adc641da40a83e2e8f294dd284607))


### Bug Fixes

* **IASOClient:** remove columns ([#43](https://github.com/BLSQ/openhexa-toolbox/issues/43)) ([8ce0a6d](https://github.com/BLSQ/openhexa-toolbox/commit/8ce0a6d4cd282c163f69cc08df0c3721c7b74eec))

## [0.2.7](https://github.com/BLSQ/openhexa-toolbox/compare/0.2.6...0.2.7) (2024-07-19)


### Bug Fixes

* **IASOClient:** remove columns ([#43](https://github.com/BLSQ/openhexa-toolbox/issues/43)) ([8ce0a6d](https://github.com/BLSQ/openhexa-toolbox/commit/8ce0a6d4cd282c163f69cc08df0c3721c7b74eec))


### Miscellaneous

* **deps:** update actions/checkout action to v4 ([#34](https://github.com/BLSQ/openhexa-toolbox/issues/34)) ([9d328d7](https://github.com/BLSQ/openhexa-toolbox/commit/9d328d747e85d47965cd4ec44e270893429d755c))
* **deps:** update dependency dev/black to v24 ([#37](https://github.com/BLSQ/openhexa-toolbox/issues/37)) ([503f3b3](https://github.com/BLSQ/openhexa-toolbox/commit/503f3b3fb656e26c597b91968d0a08f08b988b7b))
* **deps:** update dependency dev/build to v1 ([#38](https://github.com/BLSQ/openhexa-toolbox/issues/38)) ([0227c6c](https://github.com/BLSQ/openhexa-toolbox/commit/0227c6c3d8e9902e07f6023c630aeaf324a7e93e))
* **deps:** update dependency dev/pytest to v8 ([#39](https://github.com/BLSQ/openhexa-toolbox/issues/39)) ([fd5edeb](https://github.com/BLSQ/openhexa-toolbox/commit/fd5edebe4186762ff6ea05124c9ec18ceb4299ed))
* **deps:** update dependency dev/pytest-cov to ~=4.1.0 ([#32](https://github.com/BLSQ/openhexa-toolbox/issues/32)) ([5807a2f](https://github.com/BLSQ/openhexa-toolbox/commit/5807a2fec97b0a83efe837eb271528de4ae911c4))
* **deps:** update dependency dev/pytest-cov to v5 ([#40](https://github.com/BLSQ/openhexa-toolbox/issues/40)) ([822814b](https://github.com/BLSQ/openhexa-toolbox/commit/822814b966e2786968a2984873b514d298a25bc0))
* **deps:** update dependency dev/ruff to ~=0.5.2 ([#33](https://github.com/BLSQ/openhexa-toolbox/issues/33)) ([609d63c](https://github.com/BLSQ/openhexa-toolbox/commit/609d63c9666ddb1db42f22dd53efcb7eb0718790))

## [0.2.6](https://github.com/BLSQ/openhexa-toolbox/compare/0.2.5...0.2.6) (2024-07-18)


### Bug Fixes

* **IASOClient:** set default page to 1 ([#27](https://github.com/BLSQ/openhexa-toolbox/issues/27)) ([2601aca](https://github.com/BLSQ/openhexa-toolbox/commit/2601aca97d3d495d3d8439def36457387ed041a2))


### Miscellaneous

* Configure Renovate ([#26](https://github.com/BLSQ/openhexa-toolbox/issues/26)) ([c1f1ee6](https://github.com/BLSQ/openhexa-toolbox/commit/c1f1ee624ffdf273d9fb1bf972361c1dc93f5fc4))
* **deps:** update dependency dev/black to ~=23.12.1 ([#29](https://github.com/BLSQ/openhexa-toolbox/issues/29)) ([786e7d8](https://github.com/BLSQ/openhexa-toolbox/commit/786e7d8f7c615fa868daa11aa80a099eeef2baa4))
* **deps:** update dependency dev/pytest to ~=7.4.4 ([#30](https://github.com/BLSQ/openhexa-toolbox/issues/30)) ([8eed033](https://github.com/BLSQ/openhexa-toolbox/commit/8eed033fa6e92f4a298e906ffbbedd6ecade9b9b))

## [0.2.5](https://github.com/BLSQ/openhexa-toolbox/compare/0.2.4...0.2.5) (2024-06-19)


### Bug Fixes

* **deps:** Remove unused 'stringcase' dependency ([1d2a9db](https://github.com/BLSQ/openhexa-toolbox/commit/1d2a9db9b05ee7089dc21011ae99efb282b770f7))

## [0.2.4](https://github.com/BLSQ/openhexa-toolbox/compare/0.2.3...0.2.4) (2024-06-13)


### Features

* **IASOClient:** implement IASO client ([a46e67e](https://github.com/BLSQ/openhexa-toolbox/commit/a46e67ed27072f3597e95a5ed0d029d278e5e071))

## [0.2.3](https://github.com/BLSQ/openhexa-toolbox/compare/0.2.2...0.2.3) (2024-05-27)


### Features

* **dhis2:** support Period objects in get requests ([6b0860d](https://github.com/BLSQ/openhexa-toolbox/commit/6b0860da09ef129111e0d06ffe405f2de74a30eb))


### Bug Fixes

* **dhis2:** compatibility with python&lt;=3.10 ([cfc0ff4](https://github.com/BLSQ/openhexa-toolbox/commit/cfc0ff45cff6f6a4e8ab6e4a86253ed03db8ecb0))
* **dhis2:** fix bad type ([0a15d0c](https://github.com/BLSQ/openhexa-toolbox/commit/0a15d0c92e383c4d6903c6a2ca44814feaff6d72))
* **dhis2:** fix typing annotation ([ee0c5db](https://github.com/BLSQ/openhexa-toolbox/commit/ee0c5dbbc0a60b8ae67d868481fa707b2cbd70b5))
* **dhis2:** ignore chunks without data values ([fd0623e](https://github.com/BLSQ/openhexa-toolbox/commit/fd0623ed73d11ffe4c9e0d9ef2fcb4b8cea0eb04))
* **dhis2:** support null periods parameter ([e36e280](https://github.com/BLSQ/openhexa-toolbox/commit/e36e2800f2e5d74aebadbe3440b2ed7a98c74ac8))

## [0.2.2](https://github.com/BLSQ/openhexa-toolbox/compare/0.2.1...0.2.2) (2024-05-13)


### Bug Fixes

* **dhis2:** invalid params argument ([070379b](https://github.com/BLSQ/openhexa-toolbox/commit/070379bd9ea9c0a4638e87be323ea79ab967bf15))

## [0.2.1](https://github.com/BLSQ/openhexa-toolbox/compare/0.2.0...0.2.1) (2024-05-10)


### Features

* **dhis2:** add default expire time to cache ([b8bbeca](https://github.com/BLSQ/openhexa-toolbox/commit/b8bbecad80b164fee32f56e1a9bfc1a488838042))
* **dhis2:** support query filters for metadata ([46b17b9](https://github.com/BLSQ/openhexa-toolbox/commit/46b17b904f17182168ccf43bf111bc22be6a4538))


### Miscellaneous

* fix line length ([440844d](https://github.com/BLSQ/openhexa-toolbox/commit/440844dd2ca7902738926c240e2f10b555821730))

## [0.2.0](https://github.com/BLSQ/openhexa-toolbox/compare/0.1.8...0.2.0) (2024-02-22)


### ⚠ BREAKING CHANGES

* add support for kobotoolbox

### Features

* add support for kobotoolbox ([f0af8b3](https://github.com/BLSQ/openhexa-toolbox/commit/f0af8b3935ee34ee0b084fc44565988444328af4))

## [0.1.8](https://github.com/BLSQ/openhexa-toolbox/compare/0.1.7...0.1.8) (2024-02-08)


### Bug Fixes

* **dhis2:** do not raise an error if push is successfull ([01cf5cd](https://github.com/BLSQ/openhexa-toolbox/commit/01cf5cda9ff993303918be9732cedb3170929f0e))

## [0.1.7](https://github.com/BLSQ/openhexa-toolbox/compare/0.1.6...0.1.7) (2023-08-22)


### Features

* **dhis2:** allow skipping validation of data values ([d2a6b37](https://github.com/BLSQ/openhexa-toolbox/commit/d2a6b37521a71ccd953de8276e633db0ac1a7cec))

## [0.1.6](https://github.com/BLSQ/openhexa-toolbox/compare/0.1.5...0.1.6) (2023-08-22)


### Features

* **dhis2:** push data values ([c4861af](https://github.com/BLSQ/openhexa-toolbox/commit/c4861af8fc1f4d9a764ff15bc32d897d59880736))
* **dhis2:** support identifiableObjects endpoint ([46a508c](https://github.com/BLSQ/openhexa-toolbox/commit/46a508c5be321cca0620b25445b3fab025ffaf4a))
* **dhis2:** support post requests ([41c1702](https://github.com/BLSQ/openhexa-toolbox/commit/41c17027909bd275aad6877ef361e3390c10c93a))

## [0.1.5](https://github.com/BLSQ/openhexa-toolbox/compare/0.1.4...0.1.5) (2023-08-08)


### Bug Fixes

* **dhis2:** fix support for no cache dir ([da92d59](https://github.com/BLSQ/openhexa-toolbox/commit/da92d59b61012ecad855946ce250c337194c8020))


### Miscellaneous

* **main:** switch pytest to importlib ([4461a88](https://github.com/BLSQ/openhexa-toolbox/commit/4461a88fa4cc612a8649c1524294511b30a20821))
* **tests:** add tests for dhis2 periods ([e2589a1](https://github.com/BLSQ/openhexa-toolbox/commit/e2589a1334f8b27ad20cb82090b8581fe197558d))
* **tests:** simplify period tests ([140879d](https://github.com/BLSQ/openhexa-toolbox/commit/140879d0ea80199457941cd43a9e8aced4590697))

## [0.1.4](https://github.com/BLSQ/openhexa-toolbox/compare/0.1.3...0.1.4) (2023-08-02)


### Features

* **dhis2:** support comparison between periods ([a1224bf](https://github.com/BLSQ/openhexa-toolbox/commit/a1224bf8c63deef4a856e184fcf12c99996e3c12))


### Bug Fixes

* **dhis2:** dhis2 version number is a string ([ab1e603](https://github.com/BLSQ/openhexa-toolbox/commit/ab1e603ccf51b221db4dabf9960621da35522e7b))
* **dhis2:** duplicated rows in analytics response ([45ec0ec](https://github.com/BLSQ/openhexa-toolbox/commit/45ec0ec825826f357db440ba1c2c5df75b872af0))
* **dhis2:** fix year format validation ([d7892cc](https://github.com/BLSQ/openhexa-toolbox/commit/d7892ccd3d25a25204fc45957fa5405229d27fc9))
* **dhis2:** support no cache ([4299c94](https://github.com/BLSQ/openhexa-toolbox/commit/4299c9481a308c37a4b01cd153114bdc3df80971))
* **dhis2:** week range calculation ([4842d35](https://github.com/BLSQ/openhexa-toolbox/commit/4842d355c539ae54b2bdaf9af665b933c9c73ac7))


### Miscellaneous

* remove unused function ([e360b16](https://github.com/BLSQ/openhexa-toolbox/commit/e360b16b1b060c042ade5c8c4384140ec4fcd970))

## [0.1.3](https://github.com/BLSQ/openhexa-toolbox/compare/v0.1.2...0.1.3) (2023-07-25)


### Miscellaneous

* Add pre-commit, ci & release-please workflows ([28d51d3](https://github.com/BLSQ/openhexa-toolbox/commit/28d51d33ff5f9155431e72bac2bcb03bfc59146f))
