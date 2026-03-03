import os
import re
import requests
from rdflib import Graph, Namespace
from rdflib.namespace import RDF, SKOS

# Configuration
STAC_API_URL = "http://localhost:8080"  # Update with your SFEOS URL
HEADERS = {"Content-Type": "application/json"}

# Additional namespaces used in the ESA file
DCT = Namespace("http://purl.org/dc/terms/")

def slugify(text: str) -> str:
    """Converts a label like 'Atmospheric Temperature' to 'atmospheric-temperature'."""
    text = text.lower()
    return re.sub(r'[^a-z0-9]+', '-', text).strip('-')

def ingest_thesaurus():
    xml_data = os.getenv("ESA_THESAURUS_XML")
    if not xml_data:
        print("Error: ESA_THESAURUS_XML environment variable not set.")
        return

    print("Parsing RDF/XML graph...")
    g = Graph()
    g.parse(data=xml_data, format="xml")

    # Pass 0: Pre-compute all STAC IDs so we can resolve internal cross-links
    print("Pre-computing STAC IDs...")
    uri_to_stac_id = {}
    for subject in g.subjects(RDF.type, SKOS.Concept):
        pref_label = g.value(subject, SKOS.prefLabel)
        title = str(pref_label) if pref_label else "Unnamed Concept"
        uri_to_stac_id[subject] = slugify(title)

    hierarchy_links = []

    # Pass 1: Create Catalogs with ALL semantic metadata and links
    print("\nCreating Catalogs with rich metadata...")
    for subject in g.subjects(RDF.type, SKOS.Concept):
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
            SKOS.narrowMatch: "SKOS Narrow Match"
        }
        for skos_prop, link_title in match_types.items():
            for match_uri in g.objects(subject, skos_prop):
                stac_links.append({
                    "rel": "related",
                    "href": str(match_uri),
                    "title": link_title
                })

        # 2. Capture internal horizontal links (skos:related)
        for related_subj in g.objects(subject, SKOS.related):
            related_stac_id = uri_to_stac_id.get(related_subj)
            if related_stac_id:
                # We point directly to where this catalog will live in our API
                stac_links.append({
                    "rel": "related",
                    "href": f"{STAC_API_URL}/catalogs/{related_stac_id}",
                    "title": f"Related Concept: {related_stac_id.replace('-', ' ').title()}"
                })

        # POST /catalogs
        catalog_payload = {
            "type": "Catalog",
            "id": stac_id,
            "title": title,
            "description": desc,
            "stac_version": "1.0.0",
            "links": stac_links
        }
        
        res = requests.post(f"{STAC_API_URL}/catalogs", json=catalog_payload, headers=HEADERS)
        if res.status_code in [201, 200, 409]:
            print(f"Created: {stac_id} ({len(stac_links)} metadata links)")
        else:
            print(f"Failed to create {stac_id}: {res.text}")

        # Queue hierarchical links for Pass 2
        for child_subject in g.objects(subject, SKOS.narrower):
            hierarchy_links.append((subject, child_subject))

    # Pass 2: Establish the Recursive Hierarchy (Link by Reference)
    print("\nEstablishing structural hierarchy (parent/child links)...")
    for parent_subj, child_subj in hierarchy_links:
        parent_id = uri_to_stac_id.get(parent_subj)
        child_id = uri_to_stac_id.get(child_subj)

        if parent_id and child_id:
            link_payload = {"id": child_id}
            link_url = f"{STAC_API_URL}/catalogs/{parent_id}/catalogs"
            
            res = requests.post(link_url, json=link_payload, headers=HEADERS)
            if res.status_code in [200, 201]:
                print(f"Tree Linked: {parent_id} -> {child_id}")
            else:
                print(f"Failed to link {parent_id} -> {child_id}: {res.text}")

    print("\nIngestion complete!")

if __name__ == "__main__":
    ingest_thesaurus()