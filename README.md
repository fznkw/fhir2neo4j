fhir2neo4j
==========
This script populates a Neo4j database with resources of a FHIR server.

Currently, the following resource types are supported according to the [HL7 standard](http://hl7.org/fhir/):\
[Condition](https://hl7.org/fhir/condition.html),
[DiagnosticReport](https://hl7.org/fhir/diagnosticreport.html),
[Encounter](https://hl7.org/fhir/encounter.html), 
[Observation](https://hl7.org/fhir/observation.html),
[Organization](https://hl7.org/fhir/organization.html),
[Patient](https://hl7.org/fhir/patient.html),
[Procedure](https://hl7.org/fhir/procedure.html).

Where necessary, the requirements of the German Medical Information Initiative ([MII]) were further taken into account. These MII specific FHIR profiles could be found [here](https://simplifier.net/organization/koordinationsstellemii).

To support more resource types, additional model files can be easily added. Just look at one of the existing model files, e.g. `model_patient.py`.

Requirements
------------

Python 3.9 and the modules [fhirclient], [neo4j] and [rich] are needed:

    pip install fhirclient neo4j rich

This script uses the [Neo4j APOC core library](https://neo4j.com/docs/apoc/current/), which must be manually enabled for the database. For details, see the [Neo4j documentation](https://neo4j.com/docs/apoc/5/installation/).
    
Usage
-----

```
fhir2neo4j.py [-d] [-r RES [RES ...]] [--resolve] [-fhir URL[:PORT]] [--chunksize N] [--limit N] [--novalidation] [--help] [--log LEVEL] [--parallel] [-db DATABASE] URL[:PORT] USER PW

main commands (can be used in combination):
  -d, --delete      delete database content (without warning!)
  -r RES [RES ...]  list of FHIR resource(s) which are to transform (in the given order)
  --resolve         searches the database for unresolved references and tries to resolve them

arguments specifying connection to Neo4j database:
  URL[:PORT]        URL and port of the Neo4j database. Example: "neo4j://100.10.20.3:7687"
  USER              Neo4j database username
  PW                Neo4j database password
  -db DATABASE      name of the Neo4j database to use (default: "neo4j")

arguments specifying connection to FHIR server:
  -fhir URL[:PORT]  URL and port of the FHIR server
  --chunksize N     FHIR servers send resources in a series of pages; ask server for N resources per page (default: 250)
  --limit N         limit number of FHIR resources to receive for each resource
  --novalidation    turn validation of fhir resources off (default: on)

optional arguments:
  --help            show help message and exit
  --log LEVEL       set log level; possible values: "info", "warning", "error" (default: "error")
  --parallel        use parallel processing to increase speed of transformation (default: off)
```
    
Example usage
-------------

Delete database content, transform resources "Organization" and "Patient" of FHIR server at URL to a local Neo4j database and try to resolve unresolved references:

    python fhir2neo4j.py -d -r Organization Patient --resolve -fhir URL neo4j://localhost:7687 NEO4JUSER NEO4JPW

Additional notes
----------------

**Basic auth**

- If the FHIR server is protected by basic auth, a URL of the form `http(s)://USER:PW@SERVER` can be used

**Parallel processing**

- By default, parallel processing is not enabled
- To use parallel processing and thus speedup the transformation, add `--parallel` to the command line

**Unresolved references**

Some resource elements refer to other resources.
These references can be by URL (literal reference) or indirectly via an identifier object (logical reference).

When referenced via a logical reference, `fhir2neo4j` creates a placeholder entry in the database during transformation to prevent performance reducing database lookups.
To resolve these logical references and their placeholders, run `fhir2neo4j` with the `--resolve` command.
See the [FHIR documentation](https://hl7.org/fhir/references.html) for details about reference types.

[fhirclient]: https://github.com/smart-on-fhir/client-py
[neo4j]: https://neo4j.com/docs/api/python-driver/current/
[rich]: https://github.com/Textualize/rich
[MII]: https://www.medizininformatik-initiative.de
