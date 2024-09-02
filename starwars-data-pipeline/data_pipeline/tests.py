import unittest
import sqlite3
import os

class TestDatabase(unittest.TestCase):
    def setUp(self):
        # Caminho para o banco de dados existente
        self.db_path = os.path.join('data_pipeline', 'starwars.db')
        
        # Verificar se o banco de dados existe
        if not os.path.exists(self.db_path):
            self.fail(f"Banco de dados não encontrado: {self.db_path}")
        
        # Conectar ao banco de dados existente
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def test_planets_insertion(self):
        self.cursor.execute('SELECT COUNT(*) FROM planets')
        count = self.cursor.fetchone()[0]
        self.assertGreater(count, 0, "Nenhum planeta foi inserido")

    def test_characters_insertion(self):
        self.cursor.execute('SELECT COUNT(*) FROM characters')
        count = self.cursor.fetchone()[0]
        self.assertGreater(count, 0, "Nenhum personagem foi inserido")

    def test_starships_insertion(self):
        self.cursor.execute('SELECT COUNT(*) FROM starships')
        count = self.cursor.fetchone()[0]
        self.assertGreater(count, 0, "Nenhuma nave foi inserida")

    def tearDown(self):
        self.conn.close()

if __name__ == '__main__':
    unittest.main()
