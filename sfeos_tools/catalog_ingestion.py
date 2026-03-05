"""Catalog ingestion module for creating STAC catalogs from SKOS/RDF-XML files."""

import re

import requests
from rdflib import Graph, Namespace
from rdflib.namespace import RDF, SKOS

DCT = Namespace("http://purl.org/dc/terms/")


def slugify(text: str) -> str:
    """Convert a label like 'Atmospheric Temperature' to 'atmospheric-temperature'."""
    text = text.lower()
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


def _create_catalog(subject, g, uri_to_stac_id, stac_api_url, headers, auth, verify_ssl):
    """Helper function to create a catalog from a SKOS concept."""
    stac_id = uri_to_stac_id[subject]
    title = str(g.value(subject, SKOS.prefLabel) or stac_id)
    
    # Combine definition and modified date into the description
    definition = g.value(subject, SKOS.definition)
    modified = g.value(subject, DCT.modified)
    
    desc = str(definition) if definition else f"ESA Earth Topic: {title}."
    if modified:
        desc += f" (Last modified: {modified})"
    
    stac_links = []
    
    # 1. Capture external vocabulary links
    match_types = {
        SKOS.exactMatch: "SKOS Exact Match",
        SKOS.closeMatch: "SKOS Close Match",
        SKOS.broadMatch: "SKOS Broad Match",
        SKOS.narrowMatch: "SKOS Narrow Match",
    }
    for skos_prop, link_title in match_types.items():
        for match_uri in g.objects(subject, skos_prop):
            stac_links.append(
                {"rel": "related", "href": str(match_uri), "title": link_title}
            )
    
    # 2. Capture internal horizontal links (skos:related)
    for related_subj in g.objects(subject, SKOS.related):
        related_stac_id = uri_to_stac_id.get(related_subj)
        if related_stac_id:
            stac_links.append(
                {
                    "rel": "related",
                    "href": f"{stac_api_url}/catalogs/{related_stac_id}",
                    "title": f"Related Concept: {related_stac_id.replace('-', ' ').title()}",
                }
            )
    
    # POST /catalogs
    catalog_payload = {
        "type": "Catalog",
        "id": stac_id,
        "title": title,
        "description": desc,
        "stac_version": "1.0.0",
        "links": stac_links,
    }
    
    res = requests.post(
        f"{stac_api_url}/catalogs", json=catalog_payload, headers=headers, auth=auth, verify=verify_ssl
    )
    if res.status_code in [201, 200, 409]:
        print(f"Created: {stac_id} ({len(stac_links)} metadata links)")
    else:
        print(f"Failed to create {stac_id}: {res.text}")


def ingest_from_xml(
    xml_file: str, stac_api_url: str, user: str = None, password: str = None, use_ssl: bool = None
) -> None:
    """Ingest SKOS/RDF-XML file to create STAC catalogs and sub-catalogs.

    Args:
        xml_file: Path to the RDF/XML file
        stac_api_url: Base URL of the STAC API
        user: Optional username for basic authentication
        password: Optional password for basic authentication
        use_ssl: Optional SSL verification flag
    """
    headers = {"Content-Type": "application/json"}
    
    # Prepare authentication if provided
    auth = None
    if user and password:
        auth = (user, password)
    
    # Prepare SSL verification settings
    verify_ssl = True
    if use_ssl is False:
        verify_ssl = False

    print(f"Parsing RDF/XML file: {xml_file}")
    g = Graph()
    g.parse(xml_file, format="xml")

    # Pass 0: Pre-compute all STAC IDs and build hierarchy
    print("Pre-computing STAC IDs and hierarchy...")
    uri_to_stac_id = {}
    hierarchy_map = {}  # Maps child_uri -> parent_uri
    
    for subject in g.subjects(RDF.type, SKOS.Concept):
        pref_label = g.value(subject, SKOS.prefLabel)
        title = str(pref_label) if pref_label else "Unnamed Concept"
        uri_to_stac_id[subject] = slugify(title)
        
        # Build hierarchy from skos:broader relationships
        broader = g.value(subject, SKOS.broader)
        if broader:
            hierarchy_map[subject] = broader

    # Pass 1: Create root-level catalogs (those without parents)
    print("\nCreating root catalogs...")
    root_catalogs = set()
    for subject in g.subjects(RDF.type, SKOS.Concept):
        if subject not in hierarchy_map:
            root_catalogs.add(subject)
    
    for subject in root_catalogs:
        _create_catalog(subject, g, uri_to_stac_id, stac_api_url, headers, auth, verify_ssl)

    # Pass 2: Create sub-catalogs under their parents
    print("\nCreating sub-catalogs...")
    for child_uri, parent_uri in hierarchy_map.items():
        parent_id = uri_to_stac_id.get(parent_uri)
        child_id = uri_to_stac_id.get(child_uri)
        
        if parent_id and child_id:
            # Create the catalog under its parent
            title = str(g.value(child_uri, SKOS.prefLabel) or child_id)
            definition = g.value(child_uri, SKOS.definition)
            modified = g.value(child_uri, DCT.modified)
            
            desc = str(definition) if definition else f"ESA Earth Topic: {title}."
            if modified:
                desc += f" (Last modified: {modified})"
            
            stac_links = []
            
            # Add semantic links
            match_types = {
                SKOS.exactMatch: "SKOS Exact Match",
                SKOS.closeMatch: "SKOS Close Match",
                SKOS.broadMatch: "SKOS Broad Match",
                SKOS.narrowMatch: "SKOS Narrow Match",
            }
            for skos_prop, link_title in match_types.items():
                for match_uri in g.objects(child_uri, skos_prop):
                    stac_links.append(
                        {"rel": "related", "href": str(match_uri), "title": link_title}
                    )
            
            # Add related links
            for related_subj in g.objects(child_uri, SKOS.related):
                related_stac_id = uri_to_stac_id.get(related_subj)
                if related_stac_id:
                    stac_links.append(
                        {
                            "rel": "related",
                            "href": f"{stac_api_url}/catalogs/{related_stac_id}",
                            "title": f"Related Concept: {related_stac_id.replace('-', ' ').title()}",
                        }
                    )
            
            catalog_payload = {
                "type": "Catalog",
                "id": child_id,
                "title": title,
                "description": desc,
                "stac_version": "1.0.0",
                "links": stac_links,
            }
            
            res = requests.post(
                f"{stac_api_url}/catalogs/{parent_id}/catalogs",
                json=catalog_payload,
                headers=headers,
                auth=auth,
                verify=verify_ssl
            )
            if res.status_code in [201, 200, 409]:
                print(f"Created sub-catalog: {parent_id} -> {child_id}")
            else:
                print(f"Failed to create sub-catalog {child_id} under {parent_id}: {res.text}")

    print("\nStructural hierarchy established.")

    print("\nIngestion complete!")
