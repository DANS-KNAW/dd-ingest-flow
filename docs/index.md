dd-ingest-flow
==============
Ingests DANS deposit directories into Dataverse.

SYNOPSIS
--------

    # Start server
    dd-ingest-flow { server | check }

    # Client
    ingest-flow-* 

For `ingest-flow-*` commands see [dans-datastation-tools]{:target=_blank}.

DESCRIPTION
-----------

### Summary

The `dd-ingest-flow` service imports [deposit directories]{:target=_blank} into Dataverse. If successful, this will result in a new dataset in
Dataverse or a new version of an existing dataset. The input deposit directories must be located in a directory on local disk storage known as
an [ingest area](#ingest-areas).

### Ingest areas

An ingest area is a directory on local disk storage that is used by the service to receive deposits. It contains the following subdirectories:

* `inbox` - the directory under which all input deposits must be located
* `outbox` - a directory where the processed deposit are moved to (if successful to a subdirectory `processed`, otherwise to one of `rejected` or `failed`)

The service supports three ingest areas:

* import - for bulk import of deposits, triggered by a data manager;
* migration - for bulk import of datasets migrated from [EASY]{:target=_blank};
* auto-ingest - for continuous import of deposits offered through deposit service, such as [dd-sword2]{:target=_blank};

### Processing of a deposit

#### Order of deposit processing

A deposit directory represents one dataset version. The version history of a datasets is represented by a sequence of deposit directories. When enqueuing
deposits the program will first order them by the timestamp in the `Created` element in the contained bag's `bag-info.txt` file.

#### Processing steps

The processing of a deposit consists of the following steps:

1. Check that the deposit is a valid [deposit directory]{:target=_blank}.
2. Check that the bag in the deposit is a valid v1 [DANS bag]{:target=_blank}.
3. Map the dataset level metadata to the metadata fields expected in the target Dataverse.
4. If:
    * deposit represents first version of a dataset: create a new dataset draft.
    * deposit represents an update to an existing dataset: [draft a new version](#update-deposit).
5. Publish the new dataset-version.

#### Update-deposit

When receiving a deposit that specifies a new version for an existing dataset (an update-deposit) the assumption is that the bag contains the metadata and file
data that must be in the new version. This means:

* The metadata specified completely overwrites the metadata in the latest version. So, even if the client needs to change only one word, it must send all the
  existing metadata with only that particular word changed. Any metadata left out will be deleted in the new version.
* The files will replace the files in the latest version. So the files that are in the deposit are the ones that will be in the new version. If a file is to be
  deleted from the new version, it should simply be left out in the deposit. If a file is to remain unchanged in the new version, an exact copy of the current
  file must be sent.

!!! note "File path is key"

    The local file path (in Dataverse terms: directoryLabel + name) is used as the key to determine what file in the latest published version, if any, is targetting. 
    For example, to replace a published file with label `foo.txt` and directoryLabel `my/special/folder`, the bag must contain the new version at 
    `data/my/special/folder/foo.txt`. (Note that the directoryLabel is the path relative to the bag's `data/` folder.)

### Mapping to Dataverse dataset

The mapping rules are documented in the spreadsheet [DD Ingest Flow Mapping Rules]{:target=_blank}. Access to the Google spreadsheet is granted on 
request to customers of DANS.

The spreadsheet includes rules for:

* dataset level metadata
* dataset terms
* file level metadata and attributes (including setting an embargo)

ARGUMENTS
---------

### Server

```text
positional arguments:
{server,check}         available commands

named arguments:
-h, --help             show this help message and exit
-v, --version          show the application version and exit
```

### Client

The service has a RESTful API. In [dans-datastation-tools]{:target=_blank} commands to manage the service are available. These commands have names starting with
`ingest-flow-`.

INSTALLATION AND CONFIGURATION
------------------------------
Currently this project is built as an RPM package for RHEL7/CentOS7 and later. The RPM will install the binaries to
`/opt/dans.knaw.nl/dd-ingest-flow` and the configuration files to `/etc/opt/dans.knaw.nl/dd-ingest-flow`. The configuration options are documented by comments
in the default configuration file `config.yml`.

To install the module on systems that do not support RPM, you can copy and unarchive the tarball to the target host. You will have to take care of placing the
files in the correct locations for your system yourself. For instructions on building the tarball, see next section.

BUILDING FROM SOURCE
--------------------
Prerequisites:

* Java 11 or higher
* Maven 3.3.3 or higher
* RPM

Steps:

```shell 
git clone https://github.com/DANS-KNAW/dd-ingest-flow.git
cd dd-ingest-flow
mvn clean install
```

If the `rpm` executable is found at `/usr/local/bin/rpm`, the build profile that includes the RPM packaging will be activated. If `rpm` is available, but at a
different path, then activate it by using Maven's `-P` switch: `mvn -Pprm install`.

Alternatively, to build the tarball execute:

```bash
mvn clean install assembly:single
```

[DANS bag]: {{ dans_bagit_profile }}

[DANS BagIt Profile v1]: {{ dans_bagit_profile }}

[deposit directories]: {{ deposit_directory }}

[deposit directory]: {{ deposit_directory }}

[dans-datastation-tools]: https://dans-knaw.github.io/dans-datastation-tools/

[dd-sword2]: https://dans-knaw.github.io/dd-sword2/

[EASY]: https://easy.dans.knaw.nl

[DD Ingest Flow Mapping Rules]: https://docs.google.com/spreadsheets/d/1G5YHSDg3a91nI9NgRjbz11iRFU9qgnNkde6K84j1NWI/edit#gid=107937978 
