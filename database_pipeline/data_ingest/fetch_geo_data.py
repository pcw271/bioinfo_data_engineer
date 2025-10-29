"""
fetch_geo_data.py
-----------------
Fetches sample metadata from NCBI GEO (Gene Expression Omnibus)
and saves it as a CSV file for later MySQL import.
"""

import pandas as pd
import requests
from xml.etree import ElementTree as ET
import os


def fetch_geo_metadata(geo_id: str, output_dir: str = "data_ingest"):
    """
    Download and parse GEO Series metadata.
    Example: geo_id="GSE158052"
    """
    url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={geo_id}&targ=self&form=xml&view=quick"
    print(f"Fetching metadata from GEO: {url}")

    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch GEO data for {geo_id}")

    root = ET.fromstring(response.text)

    # Extract sample-level metadata
    records = []
    for sample in root.findall(".//Sample"):
        sample_id = sample.attrib.get("iid", "")
        title = sample.findtext("Title", "")
        organism = sample.findtext("Channel/Organism", "")
        source = sample.findtext("Channel/Source", "")
        molecule = sample.findtext("Channel/Molecule", "")
        label = sample.findtext("Channel/Label", "")
        records.append({
            "geo_id": geo_id,
            "sample_id": sample_id,
            "title": title,
            "organism": organism,
            "source": source,
            "molecule": molecule,
            "label": label
        })

    df = pd.DataFrame(records)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{geo_id}_metadata.csv")
    df.to_csv(output_path, index=False)
    print(f"✅ Saved metadata for {geo_id} → {output_path}")

    return df


if __name__ == "__main__":
    # Example GEO dataset (you can change this to any accession)
    geo_id = "GSE158052"
    fetch_geo_metadata(geo_id)

