import os
import sqlite3
import logging
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

def query_database(query, params=()):
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data_pipeline', 'starwars.db'))

    if not os.path.exists(db_path):
        return f"Error: Database file not found in {db_path}", 500

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
    except sqlite3.Error as e:
        return f"Database error: {e}", 500
    finally:
        conn.close()
    
    return results

@app.route('/')
def home():
    return render_template_string('''
        <h1>Welcome to the Star Wars API</h1>
        <ul>
            <li><a href="/fastest_ships">Fastest ships</a></li>
            <li><a href="/appears_most">Characters that appear most</a></li>
            <li><a href="/hottest_planet">Hottest planets</a></li>
        </ul>
    ''')

@app.route('/hottest_planet', methods=['GET'])
def hottest_planet():
    query = '''
        SELECT
            name,
            climate
        FROM
            planets
        WHERE
            surface_water IS NOT NULL AND
            surface_water != '' AND
            surface_water != 'unknown'
        ORDER BY
            CASE
                WHEN climate LIKE '%Superheated%' THEN 1
                WHEN climate LIKE '%Hot%' THEN 2
                WHEN climate LIKE '%Tropical%' THEN 3
                WHEN climate LIKE '%Arid%' THEN 4
                WHEN climate LIKE '%Temperate%' THEN 5
                WHEN climate LIKE '%Murky%' THEN 6
                WHEN climate LIKE '%Frozen%' THEN 7
                WHEN climate LIKE '%Frigid%' THEN 8
                WHEN climate LIKE '%Polluted%' THEN 9
                WHEN climate LIKE '%Artificial Temperate%' THEN 10
                ELSE 11
            END,
            CAST(surface_water AS INTEGER) DESC
        LIMIT 3;
    '''
    planets = query_database(query)

    # Format the response
    formatted_response = "Top 3 hottest planets in the Star Wars universe:\n\n"
    for i, planet in enumerate(planets, start=1):
        formatted_response += f"{i}° name: {planet[0]}, climate: {planet[1]}\n"

    return formatted_response, 200, {'Content-Type': 'text/plain; charset=utf-8'}

@app.route('/appears_most', methods=['GET'])
def appears_most():
    query = '''
        SELECT
            name,
            (LENGTH(films) - LENGTH(REPLACE(films, ',', '')) + 1) AS film_count
        FROM
            characters
        WHERE
            films IS NOT NULL AND films <> ''
        ORDER BY
            film_count DESC
        LIMIT 5;
    '''
    characters = query_database(query)

    # Format the response
    formatted_response = "Top 5 characters that appear most in Star Wars:\n\n"
    for i, character in enumerate(characters, start=1):
        formatted_response += f"{i}° name: {character[0]}, appearances: {character[1]}\n"

    return formatted_response, 200, {'Content-Type': 'text/plain; charset=utf-8'}

@app.route('/fastest_ships', methods=['GET'])
def fastest_ships():
    query = '''
        SELECT name, max_atmosphering_speed
        FROM starships
        WHERE max_atmosphering_speed != 'n/a' AND max_atmosphering_speed != ''
        ORDER BY CAST(max_atmosphering_speed AS INTEGER) DESC
        LIMIT 3;
    '''
    ships = query_database(query)

    # Format the response
    formatted_response = "Top 3 fastest ships in the Star Wars universe:\n\n"
    for i, ship in enumerate(ships, start=1):
        formatted_response += f"{i}° name: {ship[0]}, speed: {ship[1]} km/h\n"

    return formatted_response, 200, {'Content-Type': 'text/plain; charset=utf-8'}

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)