from flask import Flask, jsonify, render_template
import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import json
import pandas as pd
from io import StringIO
from scrapedtables import get_tables_data

app = Flask(__name__)

db_file = 'instance/database.db'
connection = sqlite3.connect(db_file, timeout=10)
cursor = connection.cursor()

json_file_path = os.path.join('data', 'nsi-municipalities-updated-corrected.json') # nsi-municipalities-updated.json
def load_json():
    with open(json_file_path, 'r', encoding='utf-8') as file:
        array = json.load(file)
    return array

municipalities = load_json()

def put_municipalities_in_table(slug_to_check, slug_to_add, name_cyrillic_to_add, type_cyrillic_to_add, province_cyrillic_to_add):
    query = 'SELECT 1 FROM municipalities WHERE slug = ? LIMIT 1'
    cursor.execute(query, (slug_to_check,))
    result = cursor.fetchone()
    if result:
        # print(f"municipality with slug '{slug_to_check}' exists.")
        return True
    else:
        # print(f"municipality with slug '{slug_to_check}' does not exist.")
        cursor.execute('''
        INSERT INTO municipalities (slug, name_cyrillic, type_cyrillic, province_cyrillic) 
        VALUES (?, ?, ?, ?)
        ''', (slug_to_add, name_cyrillic_to_add, type_cyrillic_to_add, province_cyrillic_to_add))
        # return False

for municipality in municipalities:
    put_municipalities_in_table(municipality['slug'],
                                municipality['slug'],
                                municipality['name_cyrillic'],
                                municipality['type_cyrillic'],
                                municipality['province_cyrillic'])

# Insert scraped data in DB table
def put_municipality_scraped_data_in_table(data):

    with open('data/merged_data.json', 'r', encoding='utf-8') as file:
        data_from_json = json.load(file)

    # Extract keys from the first object in the list
    all_keys = list(data_from_json[0].keys())
    columns = all_keys[4:] # We do not need the first 4 columns here
    
    values = [data[column] for column in columns] + [data['name_cyrillic']]
    
    sql = f'''
        UPDATE municipalities 
        SET {", ".join([f"{column} = ?" for column in columns])}
        WHERE name_cyrillic = ?
    '''
    
    cursor.execute(sql, values)

scraped_data = get_tables_data()

for row in scraped_data:
    put_municipality_scraped_data_in_table(row)


#Extract from db
final_select_query = 'SELECT * FROM municipalities'
cursor.execute(final_select_query)
table_db = cursor.fetchall()

# Get the column names from the cursor description
column_names = [description[0] for description in cursor.description]

municipalities_db_list = [
    dict(zip(column_names, row))
    for row in table_db
]

connection.commit()
connection.close()

#Routes #Homepage
@app.route('/') 
def index():
    municipality_render = []
    for municipality in municipalities_db_list:
        municipality_object = {"slug": municipality['slug'],
                               "name_cyrillic": municipality['name_cyrillic'],
                               "type_cyrillic": municipality['type_cyrillic'],
                               "province_cyrillic": municipality['province_cyrillic']}
        municipality_render.append(municipality_object)
    return render_template('index.html', 
                           municipality_render=municipality_render)

#Routes #Municiplaity pages
@app.route('/obshtina/<municipality_url>')
def municipality_urls(municipality_url):
    for municipality in municipalities_db_list:
        if municipality['slug'] == municipality_url:
            return render_template('municipality.html', municipality=municipality)    

if __name__ == '__main__':
    app.run(debug=True)