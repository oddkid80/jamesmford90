import requests
import pandas as pd
import json

api_addresses = {
    "films": "https://swapi.dev/api/films/",
    "people": "https://swapi.dev/api/people/",
    "planets": "https://swapi.dev/api/planets/",
    "species": "https://swapi.dev/api/species/",
    "starships": "https://swapi.dev/api/starships/",
    "vehicles": "https://swapi.dev/api/vehicles/"
}

def pull_swapi_api(api_url):
    list = []
    while api_url:
        api_request = requests.get(api_url)
        request_data = api_request.json()
        for el in request_data['results']:
            list.append(el)
        api_url = request_data['next']   
    df = pd.DataFrame(list)
    return df

def pull_people(people,planets,ships):
    """
        Takes swapi people, planets, and ships dataframes and creates a list of people
            with their ships and planets.
    """
    list_of_people = []
    for person in people.to_dict(orient='records'):
        #pulling ships        
        list_of_ships = ships.loc[ships['url'].isin(person['starships'])]
        list_of_ships = list_of_ships['name'].tolist()
        #pulling planets
        list_of_planets = []
        for planet in planets.to_dict(orient='records'):
            if person['url'] in planet['residents']:
                list_of_planets.append(planet['name'])
        
        person_record = {'name':person['name'],'ships':list_of_ships or None,'planets':list_of_planets or None}
        list_of_people.append(person_record)
    
    return list_of_people

def pull_species_across_planets(species,people,planets):
    """
        Takes swapi species, people, and planets, and returns list of species that exist across
            more than one planet.
    """
    species_across_planets = []
    for species in species.to_dict(orient='records'):
        planet_list=[]
        for person in people.to_dict(orient='records'):
            if species['url'] in person['species']:
                for planet in planets.to_dict(orient='records'):
                    if person['url'] in planet['residents']:
                        if planet['url'] not in planet_list:
                            planet_list.append(planet['url'])
        if len(planet_list) > 1:
            species_across_planets.append(species['name'])
            
    return species_across_planets

if __name__ == '__main__':
    
    people = pull_swapi_api(api_addresses['people'])
    planets = pull_swapi_api(api_addresses['planets'])
    ships = pull_swapi_api(api_addresses['starships'])
    species = pull_swapi_api(api_addresses['species'])
    
    list_of_people = pull_people(people,planets,ships)
    list_of_species_across_planets = pull_species_across_planets(species,people,planets)