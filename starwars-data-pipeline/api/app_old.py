import os
import sqlite3
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

def query_database(query, params=()):
    # Ajuste para o caminho relativo a partir do diretório atual
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
        SELECT name, climate
        FROM planets
        ORDER BY climate DESC
        LIMIT 3
    '''
    planets = query_database(query)
    return jsonify(planets)

@app.route('/appears_most', methods=['GET'])
def appears_most():
    query = '''
        SELECT name, COUNT(*) as appearances
        FROM characters
        GROUP BY name
        ORDER BY appearances DESC
        LIMIT 5
    '''
    characters = query_database(query)
    return jsonify(characters)

@app.route('/fastest_ships', methods=['GET'])
def fastest_ships():
    query = '''
        SELECT name, max_atmosphering_speed
        FROM starships
        ORDER BY max_atmosphering_speed DESC
        LIMIT 3
    '''
    ships = query_database(query)
    return jsonify(ships)

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
