import requests
import sqlite3
import os
import logging

# Database directory
db_directory = os.path.abspath(os.path.join('data_pipeline'))

# Check if directory exists, create if not
if not os.path.exists(db_directory):
    os.makedirs(db_directory)

# Database directory and path
db_path = os.path.join(db_directory, 'starwars.db')

# Function to get all data from a URL with pagination
def fetch_all_data(url):
    results = []
    while url:
        response = requests.get(url)
        data = response.json()
        results.extend(data['results'])
        url = data['next']  # move to the next page, if exists
    return results

# Fetching all data with pagination for each category
planet_url = "https://swapi.dev/api/planets/"
planets = fetch_all_data(planet_url)

people_url = "https://swapi.dev/api/people/"
people = fetch_all_data(people_url)

films_url = "https://swapi.dev/api/films/"
films = fetch_all_data(films_url)

starships_url = "https://swapi.dev/api/starships/"
starships = fetch_all_data(starships_url)

species_url = "https://swapi.dev/api/species/"
species = fetch_all_data(species_url)

vehicles_url = "https://swapi.dev/api/vehicles/"
vehicles = fetch_all_data(vehicles_url)

# Connect to SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create tables if they do not exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS planets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        diameter TEXT,
        rotation_period TEXT,
        orbital_period TEXT,
        gravity TEXT,
        population TEXT,
        climate TEXT,
        terrain TEXT,
        surface_water TEXT,
        url TEXT,
        created TEXT,
        edited TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS characters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        birth_year TEXT,
        eye_color TEXT,
        gender TEXT,
        hair_color TEXT,
        height TEXT,
        mass TEXT,
        skin_color TEXT,
        homeworld TEXT,
        films TEXT,
        species TEXT,
        starships TEXT,
        vehicles TEXT,
        url TEXT,
        created TEXT,
        edited TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS films (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        episode_id INTEGER,
        opening_crawl TEXT,
        director TEXT,
        producer TEXT,
        release_date TEXT,
        species TEXT,
        starships TEXT,
        vehicles TEXT,
        characters TEXT,
        planets TEXT,
        url TEXT,
        created TEXT,
        edited TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS starships (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        model TEXT,
        starship_class TEXT,
        manufacturer TEXT,
        cost_in_credits TEXT,
        length TEXT,
        crew TEXT,
        passengers TEXT,
        max_atmosphering_speed TEXT,
        hyperdrive_rating TEXT,
        MGLT TEXT,
        cargo_capacity TEXT,
        consumables TEXT,
        films TEXT,
        pilots TEXT,
        url TEXT,
        created TEXT,
        edited TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS vehicles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        model TEXT,
        vehicle_class TEXT,
        manufacturer TEXT,
        length TEXT,
        cost_in_credits TEXT,
        crew TEXT,
        passengers TEXT,
        max_atmosphering_speed TEXT,
        cargo_capacity TEXT,
        consumables TEXT,
        films TEXT,
        pilots TEXT,
        url TEXT,
        created TEXT,
        edited TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS species (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        classification TEXT,
        designation TEXT,
        average_height TEXT,
        average_lifespan TEXT,
        eye_colors TEXT,
        hair_colors TEXT,
        skin_colors TEXT,
        language TEXT,
        homeworld TEXT,
        people TEXT,
        films TEXT,
        url TEXT,
        created TEXT,
        edited TEXT
    )
''')

# Function to handle missing data
def get_value(d, key, default=None):
    return d.get(key, default)

# Insert data into tables
logging.basicConfig(level=logging.DEBUG)

def insert_planets(planets):
    for planet in planets:
        climate = get_value(planet, 'climate')
        logging.debug(f'Inserting planet: {get_value(planet, "name")}, Climate: {climate}')
        cursor.execute('''
            INSERT INTO planets (name, diameter, rotation_period, orbital_period, gravity, population, climate, terrain, surface_water, url, created, edited)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (get_value(planet, 'name'), get_value(planet, 'diameter'), get_value(planet, 'rotation_period'), get_value(planet, 'orbital_period'), get_value(planet, 'gravity'), get_value(planet, 'population'), climate, get_value(planet, 'terrain'), get_value(planet, 'surface_water'), get_value(planet, 'url'), get_value(planet, 'created'), get_value(planet, 'edited')))
    conn.commit()


def insert_characters(people):
    for person in people:
        # Prepare a tuple with default values as None for missing data
        values = (
            get_value(person, 'name'),
            get_value(person, 'birth_year'),
            get_value(person, 'eye_color'),
            get_value(person, 'gender'),
            get_value(person, 'hair_color'),
            get_value(person, 'height'),
            get_value(person, 'mass'),
            get_value(person, 'skin_color'),
            get_value(person, 'homeworld'),
            ','.join(get_value(person, 'films', [])),
            ','.join(get_value(person, 'species', [])),
            ','.join(get_value(person, 'starships', [])),
            ','.join(get_value(person, 'vehicles', [])),
            get_value(person, 'url'),
            get_value(person, 'created'),
            get_value(person, 'edited')
        )

        # Insert data into the table
        cursor.execute('''
            INSERT INTO characters (name, birth_year, eye_color, gender, hair_color, height, mass, skin_color, homeworld, films, species, starships, vehicles, url, created, edited)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', values)

    conn.commit()



def insert_films(films):
    for film in films:
        cursor.execute('''
            INSERT INTO films (title, episode_id, opening_crawl, director, producer, release_date, species, starships, vehicles, characters, planets, url, created, edited)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (get_value(film, 'title'), get_value(film, 'episode_id'), get_value(film, 'opening_crawl'), get_value(film, 'director'), get_value(film, 'producer'), get_value(film, 'release_date'), ','.join(get_value(film, 'species', [])), ','.join(get_value(film, 'starships', [])), ','.join(get_value(film, 'vehicles', [])), ','.join(get_value(film, 'characters', [])), ','.join(get_value(film, 'planets', [])), get_value(film, 'url'), get_value(film, 'created'), get_value(film, 'edited')))
    conn.commit()

def insert_starships(starships):
    for starship in starships:
        cursor.execute('''
            INSERT INTO starships (name, model, starship_class, manufacturer, cost_in_credits, length, crew, passengers, max_atmosphering_speed, hyperdrive_rating, MGLT, cargo_capacity, consumables, films, pilots, url, created, edited)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (get_value(starship, 'name'), get_value(starship, 'model'), get_value(starship, 'starship_class'), get_value(starship, 'manufacturer'), get_value(starship, 'cost_in_credits'), get_value(starship, 'length'), get_value(starship, 'crew'), get_value(starship, 'passengers'), get_value(starship, 'max_atmosphering_speed'), get_value(starship, 'hyperdrive_rating'), get_value(starship, 'MGLT'), get_value(starship, 'cargo_capacity'), get_value(starship, 'consumables'), ','.join(get_value(starship, 'films', [])), ','.join(get_value(starship, 'pilots', [])), get_value(starship, 'url'), get_value(starship, 'created'), get_value(starship, 'edited')))
    conn.commit()

def insert_species(species):
    for specie in species:
        cursor.execute('''
            INSERT INTO species (name, classification, designation, average_height, average_lifespan, eye_colors, hair_colors, skin_colors, language, homeworld, people, films, url, created, edited)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (get_value(specie, 'name'), get_value(specie, 'classification'), get_value(specie, 'designation'), get_value(specie, 'average_height'), get_value(specie, 'average_lifespan'), get_value(specie, 'eye_colors'), get_value(specie, 'hair_colors'), get_value(specie, 'skin_colors'), get_value(specie, 'language'), get_value(specie, 'homeworld'), ','.join(get_value(specie, 'people', [])), ','.join(get_value(specie, 'films', [])), get_value(specie, 'url'), get_value(specie, 'created'), get_value(specie, 'edited')))
    conn.commit()

def insert_vehicles(vehicles):
    for vehicle in vehicles:
        cursor.execute('''
            INSERT INTO vehicles (name, model, vehicle_class, manufacturer, length, cost_in_credits, crew, passengers, max_atmosphering_speed, cargo_capacity, consumables, films, pilots, url, created, edited)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (get_value(vehicle, 'name'), get_value(vehicle, 'model'), get_value(vehicle, 'vehicle_class'), get_value(vehicle, 'manufacturer'), get_value(vehicle, 'length'), get_value(vehicle, 'cost_in_credits'), get_value(vehicle, 'crew'), get_value(vehicle, 'passengers'), get_value(vehicle, 'max_atmosphering_speed'), get_value(vehicle, 'cargo_capacity'), get_value(vehicle, 'consumables'), ','.join(get_value(vehicle, 'films', [])), ','.join(get_value(vehicle, 'pilots', [])), get_value(vehicle, 'url'), get_value(vehicle, 'created'), get_value(vehicle, 'edited')))
    conn.commit()

insert_planets(planets)
insert_characters(people)
insert_films(films)
insert_starships(starships)
insert_species(species)
insert_vehicles(vehicles)

# Close the connection
conn.close()

print("Data entered successfully!")