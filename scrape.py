#!/usr/bin/env python3

import asyncio
import json
from typing import Dict, List, Tuple, Iterable

import httpx
import mercantile
import polyline

BASE_URL = "https://kubra.io"
MIN_ZOOM = 7
MAX_ZOOM = 14

async def fetch_json(client: httpx.AsyncClient, url: str) -> Dict:
    response = await client.get(url)
    response.raise_for_status()
    return response.json()

async def get_state(client: httpx.AsyncClient, instance_id: str, view_id: str) -> Dict:
    state_url = f"{BASE_URL}/stormcenter/api/v1/stormcenters/{instance_id}/views/{view_id}/currentState?preview=false"
    return await fetch_json(client, state_url)

async def get_cluster_url_template(client: httpx.AsyncClient, state: Dict, instance_id: str, view_id: str) -> str:
    deployment_id = state["stormcenterDeploymentId"]
    config_url = f"{BASE_URL}/stormcenter/api/v1/stormcenters/{instance_id}/views/{view_id}/configuration/{deployment_id}?preview=false"
    
    config = await fetch_json(client, config_url)
    interval_data = config["config"]["layers"]["data"]["interval_generation_data"]
    layer = next(layer for layer in interval_data if layer["type"].startswith("CLUSTER_LAYER"))
    
    data_path = state["data"]["cluster_interval_generation_data"]
    return f"{BASE_URL}/{data_path}/public/{layer['id']}/{{quadkey}}.json"

async def get_expected_outages(client: httpx.AsyncClient, state: Dict) -> int:
    data_path = state["data"]["interval_generation_data"]
    data_url = f"{BASE_URL}/{data_path}/public/summary-1/data.json"
    data = await fetch_json(client, data_url)
    return data["summaryFileData"]["totals"][0]["total_outages"]

def get_bounding_box(points: List[Tuple[float, float]]) -> List[float]:
    x_coordinates, y_coordinates = zip(*points)
    return [
        min(y_coordinates),
        min(x_coordinates),
        max(y_coordinates),
        max(x_coordinates),
    ]

async def get_service_area_quadkeys(client: httpx.AsyncClient, state: Dict) -> List[str]:
    regions_key, regions = next(iter(state["datastatic"].items()))
    service_areas_url = f"{BASE_URL}/{regions}/{regions_key}/serviceareas.json"
    
    res = await fetch_json(client, service_areas_url)
    areas = res["file_data"][0]["geom"]["a"]
    
    points = [point for geom in areas for point in polyline.decode(geom)]
    bbox = get_bounding_box(points)
    
    return [mercantile.quadkey(t) for t in mercantile.tiles(*bbox, zooms=[MIN_ZOOM])]

async def descend(client: httpx.AsyncClient, quadkeys: List[str], cluster_url_template: str) -> Iterable[Dict]:
    async def process_quadkey(quadkey: str) -> List[Dict]:
        url = cluster_url_template.format(qkh=quadkey[-3:][::-1], quadkey=quadkey)
        try:
            data = await fetch_json(client, url)
        except httpx.HTTPStatusError as e:
            if e.response.status_code != 404:
                raise
            return []

        if not any(outage["desc"]["cluster"] for outage in data["file_data"]) or len(quadkey) == MAX_ZOOM:
            return [{"source": url, **outage} for outage in data["file_data"]]
        else:
            tasks = [process_quadkey(quadkey + str(i)) for i in (0, 1, 2, 3)]
            results = await asyncio.gather(*tasks)
            return [item for sublist in results for item in sublist]

    tasks = [process_quadkey(quadkey) for quadkey in quadkeys]
    results = await asyncio.gather(*tasks)
    return [item for sublist in results for item in sublist]

async def scrape_outages(instance_id: str, view_id: str) -> Iterable[Dict]:
    async with httpx.AsyncClient() as client:
        state = await get_state(client, instance_id, view_id)
        cluster_url_template = await get_cluster_url_template(client, state, instance_id, view_id)
        quadkeys = await get_service_area_quadkeys(client, state)
        return await descend(client, quadkeys, cluster_url_template)

def to_geojson(outages: Iterable[Dict]) -> Dict:
    features = []
    for outage in outages:
        geom = outage.pop("geom")
        feature = {"type": "Feature", "properties": outage}
        if area := geom.get("a"):
            rings = [[point[::-1] for point in polyline.decode(polyline_ring)] for polyline_ring in area]
            feature["geometry"] = {"type": "Polygon", "coordinates": rings}
        else:
            point = polyline.decode(geom["p"][0])[0][::-1]
            feature["geometry"] = {"type": "Point", "coordinates": point}
        features.append(feature)
    
    return {"type": "FeatureCollection", "features": features}

async def main(instance_id: str, view_id: str, raw: bool = False) -> None:
    outages = await scrape_outages(instance_id, view_id)
    if raw:
        print(json.dumps(outages))
    else:
        print(json.dumps(to_geojson(outages), sort_keys=True, indent=2))

if __name__ == "__main__":
    import sys
    asyncio.run(main(sys.argv[1], sys.argv[2], "--raw" in sys.argv))
