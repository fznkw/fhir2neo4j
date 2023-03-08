# -*- coding: UTF-8 -*-

"""This file contains a Neo4j model for the FHIR resource 'Encounter'.

This model is based on the official FHIR profile 'Encounter' of HL7, and the FHIR profile
'Kontakt mit einer Gesundheitseinrichtung' of the German Medical Informatics Initiative which can be found here:
https://hl7.org/fhir/encounter.html
https://simplifier.net/medizininformatikinitiative-modulfall/kontaktgesundheitseinrichtung
"""

from fhirclient.models.encounter import Encounter
import neo4j
import model_common as common
import queries

# set up logging
import logging
log = logging.getLogger("fhir2neo4j")

__author__ = "Felix Zinkewitz"
__version__ = "0.1"
__date__ = "2023-02-14"


def initialize_database(neo4j_driver: neo4j.Driver, database: str) -> list[neo4j.ResultSummary]:
    """Do whatever is necessary to prepare the database for the model.
    Actually, only constraints need to be set for now.

    If parallel processing is used, constraints are important, because
    'When performing MERGE on a single-node pattern when the node
    does not yet exist (and there is no unique constraint), there is nothing to lock on to avoid race
    conditions, so concurrent transactions may result in duplicate nodes.'
    (https://neo4j.com/developer/kb/understanding-how-merge-works/)

    In addition, when a constraint is created, an index for the node property is also automatically created,
    which has a very positive effect on performance.

    Create a constraint for each node label that could result out of the transformation.
    In particular, all possible merged nodes and referenced resource types must be considered, as well as created backboneelement and coding nodes.

    Args:
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        list[neo4j.ResultSummary]: a list with Neo4j ResultSummary objects
    """

    results = list()

    # call database initializing function of the common modul
    results.extend(common.initialize_database(neo4j_driver, database))

    # add constraints
    results.append(queries.create_constraint_unique_node_properties("Account", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ActEncounterClass", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("ActPriority", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("AdmitSource", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Appointment", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("AufnahmegrundDritteStelle", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("AufnahmegrundErsteUndZweiteStelle", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("AufnahmegrundVierteStelle", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Condition", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("DiagnosisRole", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Diet", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("DischargeDisposition", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Encounter", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("EncounterDiagnosis", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("EncounterLocation", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("EncounterParticipant", "temp_id", neo4j_driver, database))  # note: 'temp_id' not 'fhir_id'
    results.append(queries.create_constraint_unique_node_properties("EncounterReason", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("EncounterType", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("EpisodeOfCare", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Group", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ImmunizationRecommendation", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Location", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("LocationType", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Observation", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Organization", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ParticipantType", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("Patient", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Practitioner", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("PractitionerRole", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("Procedure", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ReAdmissionIndicator", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("RelatedPerson", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ServiceRequest", "fhir_id", neo4j_driver, database))
    results.append(queries.create_constraint_unique_node_properties("ServiceType", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("SpecialArrangement", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label
    results.append(queries.create_constraint_unique_node_properties("SpecialCourtesy", ["code", "system", "version"], neo4j_driver, database))  # coding node with additional label

    return results


def process_resource(
    encounter: Encounter,
    database_deleted: bool,
    node_merges: list,
    node_relationship_merges: list,
    neo4j_driver: neo4j.Driver,
    database: str
) -> None:
    """Transforms the resource and adds node and relationship merges to the corresponding lists ({node_merges}, {node_relationship_merges}).

    Args:
        encounter (Encounter): a fhirclient Encounter object
        database_deleted (bool): True if the database is freshly deleted
        node_merges (list): the list to which node merges could be added
        node_relationship_merges (list): the list to which node and relationship merges could be added
        neo4j_driver (neo4j.Driver): Neo4j driver object
        database (str): name of the database to pass to the driver

    Returns:
        None
    """

    properties = dict()  # dict in which the node properties are gathered
    label = "Encounter"  # label of the resulting node

    # an encounter needs at least Encounter.id
    if encounter.id is None:
        log.warning("Could not process Encounter resource: missing Encounter.id.")
        return

    """ Encounter.id
    Cardinality: 0..1
    Type: string
    """
    common.append_properties(encounter.id, "fhir_id", properties)

    identifying_properties = {"fhir_id": properties["fhir_id"]}

    """ Encounter.identifier
    Cardinality: 0..*
    Type: list of Identifier datatypes
    """
    common.process_identifiers(encounter.identifier, "IDENTIFIED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.status
    Cardinality: 1..1
    Type: code (string)
    """
    common.append_properties(encounter.status, "status", properties)

    """ Encounter.statusHistory
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    if encounter.statusHistory is not None:
        # loop over statusHistories
        for n, status_history in enumerate(encounter.statusHistory):
            key_name = f"status_history" if n == 0 else f"status_history{n+1}"

            """ Encounter.statusHistory.status
            Cardinality: 1..1
            Type: code (string)
            """
            common.append_properties(status_history.status, key_name, properties)

            """ Encounter.statusHistory.period
            Cardinality: 1..1
            Type: Period datatype
            """
            common.append_period(status_history.period, key_name, properties)

    """ Encounter.class
    Cardinality: 1..1
    Type: CodableConcept datatype
    """
    common.process_codings(encounter.class_fhir, "ActEncounterClass", "HAS_CLASS", {}, label, identifying_properties, node_relationship_merges)

    """ Encounter.classHistory
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    if encounter.classHistory is not None:
        # loop over classHistories
        for n, class_history in enumerate(encounter.classHistory):
            """ Encounter.classHistory.class
            Cardinality: 1..1
            Type: CodableConcept datatype
            """
            """ Encounter.classHistory.period
            Cardinality: 1..1
            Type: Period datatype
            """
            class_history_properties = dict()
            common.append_period(class_history.period, "period", class_history_properties)
            common.process_codings(class_history.class_fhir, "ActEncounterClass", "HAS_CLASS_HISTORY", class_history_properties, label, identifying_properties, node_relationship_merges)

    """ Encounter.type
    Cardinality: 0..*
    Type: list of CodableConcept datatypes
    """
    common.process_codableconcepts(encounter.type, "EncounterType", "type", "HAS_TYPE", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.serviceType
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(encounter.serviceType, "ServiceType", "service_type", "HAS_SERVICE_TYPE", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.priority
    Cardinality: 0..1
    Type: CodableConcept datatype
    """
    common.process_codableconcepts(encounter.priority, "ActPriority", "priority", "HAS_PRIORITY", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.subject
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(encounter.subject, ["Patient", "Group"], "subject", "HAS_SUBJECT", label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.episodeOfCare
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(encounter.episodeOfCare, "EpisodeOfCare", "episode_of_care", "PART_OF", label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.basedOn
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(encounter.basedOn, "ServiceRequest", "based_on", "BASED_ON", label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.participant
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    participant_label = "EncounterParticipant"
    for participant, participant_identifying_properties, participant_properties in common.process_backboneelements(encounter.participant, "HAS_PARTICIPANT", participant_label, label, identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Encounter.participant.type
        Cardinality: 0..*
        Type: list of CodableConcept datatypes
        """
        common.process_codableconcepts(participant.type, "ParticipantType", "type", "HAS_TYPE", {}, participant_label, participant_identifying_properties, node_merges, node_relationship_merges)

        """ Encounter.participant.period
        Cardinality: 0..1
        Type: Period datatype
        """
        common.append_period(participant.period, "period", participant_properties)

        """ Encounter.participant.individual
        Cardinality: 0..1
        Type: Reference datatype
        """
        common.process_references(participant.individual, ["Practitioner", "PractitionerRole", "RelatedPerson"], "individual", "HAS_INDIVIDUAL", participant_label, participant_identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.appointment
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(encounter.appointment, "Appointment", "appointment", "SCHEDULED_BY", label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.period
    Cardinality: 0..1
    Type: Period datatype
    """
    common.append_period(encounter.period, "period", properties)

    """ Encounter.length
    Cardinality: 0..1
    Type: Duration (Quantity) datatype
    """
    common.append_quantities(encounter.length, "length", properties)

    """ Encounter.reasonCode
    Cardinality: 0..*
    Type: list of CodableConcept datatypes
    """
    common.process_codableconcepts(encounter.reasonCode, "EncounterReason", "reason", "HAS_REASON", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.reasonReference
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(encounter.reasonReference, ["Condition", "Procedure", "Observation", "ImmunizationRecommendation"], "reason_reference", "HAS_REASON_REFERENCE", label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.diagnosis
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    diagnosis_label = "EncounterDiagnosis"
    for diagnosis, diagnosis_identifying_properties, diagnosis_properties in common.process_backboneelements(encounter.diagnosis, "HAS_DIAGNOSIS", diagnosis_label, label, identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Encounter.diagnosis.condition
        Cardinality: 1..1
        Type: Reference datatype
        """
        common.process_references(diagnosis.condition, ["Condition", "Procedure"], "condition", "HAS_CONDITION", diagnosis_label, diagnosis_identifying_properties, node_merges, node_relationship_merges)

        """ Encounter.diagnosis.use
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(diagnosis.use, "DiagnosisRole", "use", "HAS_USE", {}, diagnosis_label, diagnosis_identifying_properties, node_merges, node_relationship_merges)

        """ Encounter.diagnosis.rank
        Cardinality: 0..1
        Type: integer
        """
        common.append_properties(diagnosis.rank, "rank", diagnosis_properties)

    """ Encounter.account
    Cardinality: 0..*
    Type: list of Reference datatypes
    """
    common.process_references(encounter.account, "Account", "account", "HAS_ACCOUNT", label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.hospitalization
    Cardinality: 0..1
    Type: BackboneElement
    """
    if encounter.hospitalization is not None:
        """ Encounter.hospitalization.preAdmissionIdentifier
        Cardinality: 0..1
        Type: Identifier datatype
        """
        common.process_identifiers(encounter.hospitalization.preAdmissionIdentifier, "HAS_HOSPITALIZATION_PRE_ADMISSION_IDENTIFIER", label, identifying_properties, node_merges, node_relationship_merges)

        """ Encounter.hospitalization.origin
        Cardinality: 0..1
        Type: Reference datatype
        """
        common.process_references(encounter.hospitalization.origin, ["Location", "Organization"], "hospitalization_origin", "HAS_HOSPITALIZATION_ORIGIN", label, identifying_properties, node_merges, node_relationship_merges)

        """ Encounter.hospitalization.admitSource
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(encounter.hospitalization.admitSource, "AdmitSource", "hospitalization_admit_source", "HOSPITALIZATION_ADMITTED_FROM", {}, label, identifying_properties, node_merges, node_relationship_merges)

        """ Encounter.hospitalization.reAdmission
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(encounter.hospitalization.reAdmission, "ReAdmissionIndicator", "hospitalization_re_admission", "HAS_HOSPITALIZATION_RE_ADMISSION_TYPE", {}, label, identifying_properties, node_merges, node_relationship_merges)

        """ Encounter.hospitalization.dietPreference
        Cardinality: 0..*
        Type: list of CodableConcept datatypes
        """
        common.process_codableconcepts(encounter.hospitalization.dietPreference, "Diet", "hospitalization_diet_preference", "HAS_HOSPITALIZATION_DIET_PREFERENCE", {}, label, identifying_properties, node_merges, node_relationship_merges)

        """ Encounter.hospitalization.specialCourtesy
        Cardinality: 0..*
        Type: list of CodableConcept datatypes
        """
        common.process_codableconcepts(encounter.hospitalization.specialCourtesy, "SpecialCourtesy", "hospitalization_special_courtesy", "HAS_HOSPITALIZATION_SPECIAL_COURTESY", {}, label, identifying_properties, node_merges, node_relationship_merges)

        """ Encounter.hospitalization.specialArrangement
        Cardinality: 0..*
        Type: list of CodableConcept datatypes
        """
        common.process_codableconcepts(encounter.hospitalization.specialArrangement, "SpecialArrangement", "hospitalization_special_arrangement", "HAS_HOSPITALIZATION_SPECIAL_ARRANGEMENT", {}, label, identifying_properties, node_merges, node_relationship_merges)

        """ Encounter.hospitalization.destination
        Cardinality: 0..1
        Type: Reference datatype
        """
        common.process_references(encounter.hospitalization.destination, ["Location", "Organization"], "hospitalization_destination", "HAS_HOSPITALIZATION_DESTINATION", label, identifying_properties, node_merges, node_relationship_merges)

        """ Encounter.hospitalization.dischargeDisposition
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(encounter.hospitalization.dischargeDisposition, "DischargeDisposition", "hospitalization_discharge_disposition", "HAS_HOSPITALIZATION_DISCHARGE_DISPOSITION", {}, label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.location
    Cardinality: 0..*
    Type: list of BackboneElements
    """
    location_label = "EncounterLocation"
    for location, location_identifying_properties, location_properties in common.process_backboneelements(encounter.location, "HAS_LOCATION", location_label, label, identifying_properties, database_deleted, node_relationship_merges, neo4j_driver, database):
        # {process_backboneelements} takes care of everything except processing of backbone element specific items
        """ Encounter.location.location
        Cardinality: 1..1
        Type: Reference datatype
        """
        common.process_references(location.location, ["Location"], "location", "HAS_LOCATION", location_label, location_identifying_properties, node_merges, node_relationship_merges)

        """ Encounter.location.status
        Cardinality: 0..1
        Type: code (string)
        """
        common.append_properties(location.status, "status", location_properties)

        """ Encounter.location.physicalType
        Cardinality: 0..1
        Type: CodableConcept datatype
        """
        common.process_codableconcepts(location.physicalType, "LocationType", "physical_type", "HAS_TYPE", {}, location_label, location_identifying_properties, node_merges, node_relationship_merges)

        """ Encounter.location.period
        Cardinality: 0..1
        Type: Period datatype
        """
        common.append_period(location.period, "period", location_properties)

    """ Encounter.serviceProvider
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(encounter.serviceProvider, "Organization", "service_provider", "HAS_SERVICE_PROVIDER", label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.partOf
    Cardinality: 0..1
    Type: Reference datatype
    """
    common.process_references(encounter.partOf, "Encounter", "part_of", "PART_OF", label, identifying_properties, node_merges, node_relationship_merges)

    """ Encounter.extension
    Cardinality: 0..*
    Type: list of Extension datatypes
    """
    # the German Medical Informatics Initiative defines an extension 'Aufnahmegrund' which itself contains up to three extensions:
    # 'ErsteUndZweiteStelle', 'DritteStelle' and 'VierteStelle' which each contains a Coding object
    """ Encounter.extension:Aufnahmegrund
    Cardinality: 0..1
    Type: Extension
    """
    for extension in common.process_extensions(encounter.extension, "http://fhir.de/StructureDefinition/Aufnahmegrund"):
        for subextension in common.process_extensions(extension, None):
            if subextension.url == "ErsteUndZweiteStelle":
                """ Encounter.extension:Aufnahmegrund:ErsteUndZweiteStelle
                Cardinality: 0..1
                Type: Extension
                """
                """ Encounter.extension:Aufnahmegrund:ErsteUndZweiteStelle.valueCoding
                Cardinality: 0..1
                Type: Coding datatype
                """
                common.process_codings(subextension.valueCoding, "AufnahmegrundErsteUndZweiteStelle", "HAS_AUFNAHMEGRUND_ERSTE_UND_ZWEITE_STELLE", {}, label, identifying_properties, node_relationship_merges)
            elif subextension.url == "DritteStelle":
                """ Encounter.extension:Aufnahmegrund:DritteStelle
                Cardinality: 0..1
                Type: Extension
                """
                """ Encounter.extension:Aufnahmegrund:DritteStelle.valueCoding
                Cardinality: 0..1
                Type: Coding datatype
                """
                common.process_codings(subextension.valueCoding, "AufnahmegrundDritteStelle", "HAS_AUFNAHMEGRUND_DRITTE_STELLE", {}, label, identifying_properties, node_relationship_merges)
            elif subextension.url == "VierteStelle":
                """ Encounter.extension:Aufnahmegrund:VierteStelle
                Cardinality: 0..1
                Type: Extension
                """
                """ Encounter.extension:Aufnahmegrund:VierteStelle.valueCoding
                Cardinality: 0..1
                Type: Coding datatype
                """
                common.process_codings(subextension.valueCoding, "AufnahmegrundVierteStelle", "HAS_AUFNAHMEGRUND_VIERTE_STELLE", {}, label, identifying_properties, node_relationship_merges)

    # merge encounter node with all properties
    common.append_node_merge(label, identifying_properties, properties, node_merges)
