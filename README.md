[![Sensu Bonsai Asset](https://img.shields.io/badge/Bonsai-Download%20Me-brightgreen.svg?colorB=89C967&logo=sensu)](https://bonsai.sensu.io/assets/alasconnect/sensu-airflow-check)
![Go Test](https://github.com/alasconnect/sensu-airflow-check/workflows/Go%20Test/badge.svg)
![goreleaser](https://github.com/alasconnect/sensu-airflow-check/workflows/goreleaser/badge.svg)

# sensu-airflow-check

## Table of Contents
- [Overview](#overview)
- [Files](#files)
- [Usage examples](#usage-examples)
- [Configuration](#configuration)
  - [Asset registration](#asset-registration)
  - [Check definition](#check-definition)
- [Installation from source](#installation-from-source)
- [Additional notes](#additional-notes)
- [Contributing](#contributing)

## Overview

The sensu-airflow-check is a [Sensu Check][6] that provides monitoring for airflow and DAGs.

## Checks

This collection contains the following checks:

 - airflow-check - for checking the health of the airflow metadatabase and scheduler.
 - airflow-dag-check - for checking DAG runs for all or a specific list of DAGs

## Usage examples

```
airflow-check --url http://localhost:8080/ --username admin --password admin
```

```
# Will check all loaded DAGs
airflow-dag-check --url http://localhost:8080/ --username admin --password admin
```

```
# Will check specific DAGs and will fail if one of the DAGs does not exist
airflow-dag-check --url http://localhost:8080/ --username admin --password admin --dag my_dag_id --dag my_other_dag_id
```

## Configuration

The airflow API must be configured.
Typically, this means setting the following line in the `[api]` section:
```
auth_backend = airflow.api.auth.backend.basic_auth
```

### Asset registration

[Sensu Assets][10] are the best way to make use of this plugin. If you're not using an asset, please
consider doing so! If you're using sensuctl 5.13 with Sensu Backend 5.13 or later, you can use the
following command to add the asset:

```
sensuctl asset add alasconnect/sensu-airflow-check
```

If you're using an earlier version of sensuctl, you can find the asset on the [Bonsai Asset Index][https://bonsai.sensu.io/assets/alasconnect/sensu-airflow-check].

### Check definition

#### airflow-check

```yml
---
type: CheckConfig
api_version: core/v2
metadata:
  name: airflow-check
  namespace: default
spec:
  command: airflow-check --url {url} --username {username} --password {password}
  subscriptions:
  - system
  runtime_assets:
  - alasconnect/sensu-airflow-check
```

#### airflow-dag-check

```yml
---
type: CheckConfig
api_version: core/v2
metadata:
  name: airflow-dag-check
  namespace: default
spec:
  command: airflow-dag-check --url {url} --username {username} --password {password} --dag {dag_id_1} --dag {dag_id_2}
  subscriptions:
  - system
  runtime_assets:
  - alasconnect/sensu-airflow-check
```

## Installation from source

The preferred way of installing and deploying this plugin is to use it as an Asset. If you would
like to compile and install the plugin from source or contribute to it, download the latest version
or create an executable script from this source.

From the local path of the sensu-airflow-check repository:

```
go build -o bin/airflow-check ./cmd/airflow-check
go build -o bin/airflow-dag-check ./cmd/airflow-dag-check
```

## Additional notes

## Contributing

For more information about contributing to this plugin, see [Contributing][1].

[1]: https://github.com/sensu/sensu-go/blob/master/CONTRIBUTING.md
[2]: https://github.com/sensu-community/sensu-plugin-sdk
[3]: https://github.com/sensu-plugins/community/blob/master/PLUGIN_STYLEGUIDE.md
[4]: https://github.com/sensu-community/check-plugin-template/blob/master/.github/workflows/release.yml
[5]: https://github.com/sensu-community/check-plugin-template/actions
[6]: https://docs.sensu.io/sensu-go/latest/reference/checks/
[7]: https://github.com/sensu-community/check-plugin-template/blob/master/main.go
[8]: https://bonsai.sensu.io/
[9]: https://github.com/sensu-community/sensu-plugin-tool
[10]: https://docs.sensu.io/sensu-go/latest/reference/assets/
