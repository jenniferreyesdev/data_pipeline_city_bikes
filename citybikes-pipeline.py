import dlt
from typing import Dict, List, Generator, Any, Optional
import requests
from datetime import datetime
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from dotenv import load_dotenv
import os
import sys
import subprocess
from pathlib import Path

#load environment variables and configurations
load_dotenv()

ENVIRONMENT = os.getenv('ENVIRONMENT', 'production')
CITY_NAME = os.getenv('CITY_NAME', 'Chicago')
DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), 'citybikes_dbt')

#Configuration dictionary
config = {
    'development': {
        'dev_mode': True,
        'pipeline_name': 'citybikes_data_dev',
        'dataset_name': 'citybikes_dev'
    },
    'staging': {
        'dev_mode': True,
        'pipeline_name': 'citybikes_data_staging',
        'dataset_name': 'citybikes_staging'
    },
    'production': {
        'dev_mode': False,
        'pipeline_name': 'citybikes_data',
        'dataset_name': 'citybikes'
    }
}

env_config = config.get(ENVIRONMENT, config['production'])

def find_network_by_city(city_name: str) -> Optional[str]:
    #Retrieve the bike network ID based on CITY_NAME

    url = "http://api.citybik.es/v2/networks"
    response = requests.get(url)
    response.raise_for_status()
    networks = response.json()["networks"]
    
    city_lower = city_name.lower()
    for network in networks:
        location = network.get("location", {})
        if (city_lower in location.get("city", "").lower() or 
            city_lower in network.get("name", "").lower()):
            return network.get("id")
    
    raise ValueError(f"No bike-sharing network found for {city_name}")

def get_network_details(network_id: str) -> Dict:
    #Construct dynamic URL and fetch data for specific network_id

    url = f"http://api.citybik.es/v2/networks/{network_id}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["network"]

@dlt.resource(primary_key="id", write_disposition="replace")
def get_network_info(city_name: str) -> Generator[Dict, None, None]:
    #Retrieves information about the city's bike network
    
    try:
        network_id = find_network_by_city(city_name)
        network_data = get_network_details(network_id)
        network_data['id'] = network_id
        print(f"Network data fetched successfully: {network_data['id']}")
        yield network_data
    except Exception as e:
        raise RuntimeError(f"Error fetching network info: {str(e)}")

@dlt.resource(primary_key="station_id", write_disposition="replace")
def get_station_data(city_name: str) -> Generator[Dict, None, None]:
    #Retrieves detailed station data for the network
    try:
        network_id = find_network_by_city(city_name)
        network_data = get_network_details(network_id)
        print("Network ID ", network_id, " Network Data", network_data.keys())
        if not network_data or 'stations' not in network_data:
            raise ValueError("Invalid network data received")
            
        for station in network_data['stations']:
            station_info = {
                "station_id": station.get("id"),
                "name": station.get("name"),
                "latitude": station.get("latitude"),
                "longitude": station.get("longitude"),
                "free_bikes": station.get("free_bikes"),
                "empty_slots": station.get("empty_slots"),
                "timestamp": station.get("timestamp"),
                "address": station.get("extra", {}).get("address")
            }
            yield station_info
            
    except Exception as e:
        raise RuntimeError(f"Error processing station data: {str(e)}")

@dlt.resource(
    primary_key="analysis_id",
    write_disposition="replace",
    table_name="coverage_analysis",
    columns={
        "analysis_id": {
            "data_type": "text",
            "nullable": False
        },
        "total_stations": {
            "data_type": "bigint",  
            "nullable": False
        },
        "station_density": {
            "data_type": "double",
            "nullable": False
        },
        "service_area_sqmi": {
            "data_type": "double",
            "nullable": False
        },
        "coverage_gaps": {
            "data_type": "json",  
            "nullable": True
        },
        "recommendations": {
            "data_type": "json",  
            "nullable": True
        }
    }
)
def get_coverage_analysis(station_data) -> Generator[Dict, None, None]:
    #Analyzes station coverage and recommends new locations
    print("get_coverage_analysis running")
    try:
        if not station_data:
            raise ValueError("No station data provided")
            
        if not isinstance(station_data, list):
            station_data = list(station_data)
            
        stations_df = pd.DataFrame(station_data)
        # print(stations_df.head(5))
        if stations_df.empty:
            raise ValueError("No station data available for analysis")
        
        grid_size = 0.01
        lat_range = np.arange(
            float(stations_df['latitude'].min()) - grid_size,
            float(stations_df['latitude'].max()) + grid_size,
            grid_size
        )
        lon_range = np.arange(
            float(stations_df['longitude'].min()) - grid_size,
            float(stations_df['longitude'].max()) + grid_size,
            grid_size
        )
        print("Analyzing coverage gaps...")
        coverage_gaps = []
        for lat in lat_range:
            for lon in lon_range:
                min_distance = float('inf')
                for _, station in stations_df.iterrows():
                    distance = geodesic(
                        (float(lat), float(lon)),
                        (float(station["latitude"]), float(station["longitude"]))
                    ).miles
                    min_distance = min(min_distance, distance)
                
                if min_distance > 0.5:
                    geolocator = Nominatim(user_agent="citybikes_analysis")
                    try:
                        location = geolocator.reverse((float(lat), float(lon)))
                        address = location.address if location else "Unknown location"
                    except:
                        address = f"Location at ({float(lat):.4f}, {float(lon):.4f})"
                    
                    coverage_gaps.append({
                        "location": address,
                        "coordinates": (float(lat), float(lon)),
                        "nearest_station_distance": float(round(min_distance, 2))
                    })
        
        total_stations = int(len(stations_df))
        lat_span = float(stations_df['latitude'].max() - stations_df['latitude'].min())
        lon_span = float(stations_df['longitude'].max() - stations_df['longitude'].min())
        #Convert lat and lon to miles
        approx_area_sqmi = float(lat_span * lon_span * 69 * 69)
        station_density = float(total_stations / approx_area_sqmi if approx_area_sqmi > 0 else 0)
        
        coverage_gaps.sort(key=lambda x: x['nearest_station_distance'], reverse=True)
        
        recommendations_list = [
            f"{gap['location']} (Distance to nearest station: {gap['nearest_station_distance']} miles)"
            for gap in coverage_gaps[:5]
        ]
        
        yield {
            "analysis_id": "1",
            "total_stations": total_stations,
            "station_density": float(round(station_density, 2)),
            "service_area_sqmi": float(round(approx_area_sqmi, 2)),
            "coverage_gaps": coverage_gaps[:10],
            "recommendations": recommendations_list
        }
    except Exception as e:
        raise RuntimeError(f"Error in coverage analysis: {str(e)}")

@dlt.source
def citybikes_source(city_name: str):
    #DLT source: CityBikes API data
    #Creates three data streams: network_info, station_data, & coverage_analysis
   
    network_info = get_network_info(city_name).with_name("network_info")
    print("network_info")
    print(network_info)
    station_data = get_station_data(city_name).with_name("station_data")
    print("station_data")
    print(station_data)
    coverage_analysis = get_coverage_analysis.with_name("coverage_analysis")
    print("coverage_analysis")
    print(coverage_analysis)
    
    coverage_analysis = coverage_analysis.bind(station_data=station_data)
    
    return [network_info, station_data, coverage_analysis]

def run_dbt_transformations():
    #Run dbt transformations
    try:
        print("Running dbt transformations...")
        
        os.environ['DBT_PROFILES_DIR'] = os.path.join(os.path.dirname(__file__), '.dbt')
        
        os.chdir(DBT_PROJECT_DIR)
        
        subprocess.run(["dbt", "deps"], check=True)
        subprocess.run(["dbt", "run"], check=True)
        
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"dbt transformation failed: {str(e)}")
    except Exception as e:
        raise RuntimeError(f"Error during dbt transformation: {str(e)}")
        
if __name__ == "__main__":
    try:
        print(f"Starting pipeline in {ENVIRONMENT} environment...")
        
        #Initialize dlt pipeline
        pipeline = dlt.pipeline(
            pipeline_name=env_config['pipeline_name'],
            destination='postgres',
            dataset_name=env_config['dataset_name'],
            dev_mode=env_config['dev_mode']
        )
        
        load_info = pipeline.run(citybikes_source(CITY_NAME))
        print("Initial data load completed:", load_info)
        
        #Run dbt transformations
        run_dbt_transformations()
        
        print("Pipeline execution completed successfully")
        
    except Exception as e:
        print(f"Error running pipeline: {str(e)}")
        raise