import requests
import sqlite3
import os

#database directory
db_directory = os.path.abspath(os.path.join('data_pipeline'))


#check if directory exists, create if not
if not os.path.exists(db_directory):
    os.makedirs(db_directory)

#database directory and path
db_path = os.path.join(db_directory, 'starwars.db')

#function to get data from a URL
def fetch_data(url):
    response = requests.get(url)
    return response.json()

#planet data
planet_url = "https://swapi.dev/api/planets/"
planets = fetch_data(planet_url)

#character data
people_url = "https://swapi.dev/api/people/"
people = fetch_data(people_url)

#ship data
starships_url = "https://swapi.dev/api/starships/"
starships = fetch_data(starships_url)

#connect to SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

#create tables if they do not exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS planets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        climate TEXT,
        terrain TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS characters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        height TEXT,
        mass TEXT,
        hair_color TEXT,
        skin_color TEXT,
        eye_color TEXT,
        birth_year TEXT,
        gender TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS starships (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        model TEXT,
        manufacturer TEXT,
        max_atmosphering_speed TEXT
    )
''')

#insert data into tables
def insert_planets(planets):
    for planet in planets['results']:
        cursor.execute('''
            INSERT INTO planets (name, climate, terrain)
            VALUES (?, ?, ?)
        ''', (planet['name'], planet['climate'], planet['terrain']))
    conn.commit()

def insert_characters(people):
    for person in people['results']:
        cursor.execute('''
            INSERT INTO characters (name, height, mass, hair_color, skin_color, eye_color, birth_year, gender)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (person['name'], person['height'], person['mass'], person['hair_color'], person['skin_color'], person['eye_color'], person['birth_year'], person['gender']))
    conn.commit()

def insert_starships(starships):
    for starship in starships['results']:
        cursor.execute('''
            INSERT INTO starships (name, model, manufacturer, max_atmosphering_speed)
            VALUES (?, ?, ?, ?)
        ''', (starship['name'], starship['model'], starship['manufacturer'], starship['max_atmosphering_speed']))
    conn.commit()

insert_planets(planets)
insert_characters(people)
insert_starships(starships)

#close the connection
conn.close()

print("Data entered successfully!")
