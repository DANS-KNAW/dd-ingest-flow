Development
===========
This page contains information for developers about how to contribute to this project.

Set-up
------
This project can be used in combination with  [dans-dev-tools]{:target=_blank}. Before you can start it as a service
some dependencies must first be started:

### HSQL database

Open a separate terminal tab:

```commandline
start-hsqldb-server.sh
```

### dd-dtap

The service needs a Dataverse instance to talk to. For this you can use [dd-dtap]{:target=_blank} (only accessible to DANS developers):

```commandline
start-preprovisioned-box -s
```

After start-up:

```commandline
vagrant ssh
curl -X PUT -d s3kretKey http://localhost:8080/api/admin/settings/:BlockedApiKey
curl -X PUT -d unblock-key http://localhost:8080/api/admin/settings/:BlockedApiPolicy
```

### dd-ingest-flow + dd-validate-dans-bag/application

Open both projects in separate terminal tabs do the following for each:

```commandline
start-env.sh
```

Configure the correct API key in `etc/config.yml`.

Now you can start the service:

```commandline
start-service.sh
```

## Prepare and start an ingest

Once the dependencies and services are started you can ingest a single deposit by moving
(not copy) a deposit into `data/auto-ingest/inbox` or whatever directory is configured in  

    dd-ingest-flow/etc/config.yml ingestFlow:autoIngest:inbox

Note that a migration bag has more data than valid for this process. 
The validator will inform you about what to remove and how to fix the checksums.

The [dans-datastation-tools]{:target=_blank} project has commands to copy/move your data into an `ingest_area` (autoIngest/import/migration) require a user group `deposits`.
When running locally you don't have such a group, so you can't use these commands.
Make sure to have the following structure.

```
dd-ingest-flow
├── data
│   ├── auto-ingest
│   │   ├── inbox
│   │   │   └── <SOME-DIR>
│   │   │       ├── <UUID>
│   │   │       ├── bag
│   │   │       │   └── *
│   │   │       └── deposit.properties
│   │   └── out
│   └── tmp
```

Alternatively you can prepare batches in one of the other ingest areas and start as follows.

Configure the `ingest_flow` section and `dataverse` section of `.dans-datastation-tools.yml` which is  a copy of `src/datastation/example-dans-datastation-tools.yml`.

* `service_baseurl` should refer to `localhost`
* The `ingest_areas` should refer to the same folders as the `ingestFlow` section of `dd-ingest-flow/etc/config.yml`.
  Replace the default `/var/opt/dans.knaw.nl/tmp` in the latter with `data`.
* Set the `apiKey`
* To repeat a test you'll need the `dv-dataset-destroy` script which needs `safety_latch: OFF`, the default is `ON`.

Assuming `dans-datastation-tools` and `dd-ingest-flow` are in the same directory:

```commandline
cd ~/git/service/data-station/dans-datastation-tools
poetry run ingest-flow start-migration -s ../dd-ingest-flow/data/migration/inbox/<SOME-DIR>/<UUID>
```


[dans-dev-tools]: https://github.com/DANS-KNAW/dans-dev-tools#dans-dev-tools

[dans-datastation-tools]: https://github.com/DANS-KNAW/dans-datastation-tools#dans-datastation-tools

[dd-dtap]: https://github.com/DANS-KNAW/dd-dtap