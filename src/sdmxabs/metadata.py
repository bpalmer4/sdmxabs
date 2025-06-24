"""Extract metadata from the ABS SDMX API."""

from typing import Any, Unpack
from sdmxabs.download_cache import acqure_url, GetFileKwargs


URL_STEM = "https://data.api.abs.gov.au/rest"
# /{structureType}/{agencyId}/{structureId}/{structureVersion}
# ? references={reference value}& detail={level of detail}

agency_ids = ("ABS",)
STRUCTUERS = (
    "datastructure"
    "dataflow"
    "codelist"
    "conceptscheme"
    "categoryscheme"
    "contentconstraint"
    "actualconstraint"
    "agencyscheme"
    "categorisation"
    "hierarchicalcodelist"
)
stucture_ids = ("all",)
details = (
    "allstubs",  # All artefacts will be returned as stubs.
    "referencestubs",  # The referenced artefacts will be returned as stubs.
    "referencepartial",  # The referenced item schemes should only include
    # items used by the artefact to be returned. For example,
    # a concept scheme would only contain the concepts used in
    # a DSD, and its isPartial flag would be set to true. As
    # another example, if a dataflow is constrained, then the
    # codelists returned should only contain the subset of codes
    # allowed by that constraint.
    "allcompletestubs",  # All artefacts should be returned as complete stubs,
    # containing identification information, the artefacts' name,
    # description, annotations and isFinal information.
    "referencecompletestubs",  # The referenced artefacts should be returned as complete
    # stubs, containing identification information, the artefacts'
    # name, description, annotations and isFinal information.
    "full",  # All available information for all artefacts will be returned. This is the default.)
)


def abs_catalogue(**kwargs: Unpack[GetFileKwargs]) -> Any:
    """Get the toplevel metadata from the ABS SDMX API."""

    kwargs["modality"] = kwargs.get("modality", "prefer_cache")
    return acqure_url(f"{URL_STEM}/dataflow/ABS/all/", **kwargs)
