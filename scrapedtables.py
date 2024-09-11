from flask import Flask, jsonify, render_template
import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import json
import pandas as pd
from io import StringIO
import pprint

def get_tables_data():
    urls = {
        "population":"https://nsi.bg/bg/content/2975/%D0%BD%D0%B0%D1%81%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D0%B5-%D0%BF%D0%BE-%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D0%B8-%D0%BE%D0%B1%D1%89%D0%B8%D0%BD%D0%B8-%D0%BC%D0%B5%D1%81%D1%82%D0%BE%D0%B6%D0%B8%D0%B2%D0%B5%D0%B5%D0%BD%D0%B5-%D0%B8-%D0%BF%D0%BE%D0%BB",
        "deaths":"https://nsi.bg/bg/content/3006/%D1%83%D0%BC%D0%B8%D1%80%D0%B0%D0%BD%D0%B8%D1%8F-%D0%BF%D0%BE-%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D0%B8-%D0%BE%D0%B1%D1%89%D0%B8%D0%BD%D0%B8-%D0%B8-%D0%BF%D0%BE%D0%BB",
        "born":"https://nsi.bg/bg/content/2961/%D0%B6%D0%B8%D0%B2%D0%BE%D1%80%D0%BE%D0%B4%D0%B5%D0%BD%D0%B8-%D0%BF%D0%BE-%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D0%B8-%D0%BE%D0%B1%D1%89%D0%B8%D0%BD%D0%B8-%D0%B8-%D0%BF%D0%BE%D0%BB",
        "relocation":"https://nsi.bg/bg/content/3060/%D0%BC%D0%B5%D1%85%D0%B0%D0%BD%D0%B8%D1%87%D0%BD%D0%BE-%D0%B4%D0%B2%D0%B8%D0%B6%D0%B5%D0%BD%D0%B8%D0%B5-%D0%BD%D0%B0-%D0%BD%D0%B0%D1%81%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D0%B5%D1%82%D0%BE-%D0%BF%D0%BE-%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D0%B8-%D0%BE%D0%B1%D1%89%D0%B8%D0%BD%D0%B8-%D0%B8-%D0%BF%D0%BE%D0%BB",
        "kinder_gardens":"https://nsi.bg/bg/content/3430/%D0%B4%D0%B5%D1%82%D1%81%D0%BA%D0%B8-%D0%B3%D1%80%D0%B0%D0%B4%D0%B8%D0%BD%D0%B8-%D0%B4%D0%B5%D1%86%D0%B0-%D0%BF%D0%B5%D0%B4%D0%B0%D0%B3%D0%BE%D0%B3%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B8-%D0%BF%D0%B5%D1%80%D1%81%D0%BE%D0%BD%D0%B0%D0%BB-%D0%BC%D0%B5%D1%81%D1%82%D0%B0-%D0%B8-%D0%B3%D1%80%D1%83%D0%BF%D0%B8-%D0%B2-%D0%B4%D0%B5%D1%82%D1%81%D0%BA%D0%B8%D1%82%D0%B5-%D0%B3%D1%80%D0%B0%D0%B4%D0%B8%D0%BD%D0%B8-%D0%BF%D0%BE-%D1%81%D1%82%D0%B0%D1%82%D0%B8%D1%81%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B8-%D0%B7%D0%BE%D0%BD%D0%B8-%D1%81%D1%82%D0%B0%D1%82%D0%B8%D1%81%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B8-%D1%80%D0%B0%D0%B9%D0%BE%D0%BD%D0%B8-%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D0%B8-%D0%B8-%D0%BE%D0%B1%D1%89%D0%B8%D0%BD%D0%B8",
        "schools":"https://nsi.bg/bg/content/3508/%D1%83%D1%87%D0%B8%D0%BB%D0%B8%D1%89%D0%B0-%D0%BF%D0%B0%D1%80%D0%B0%D0%BB%D0%B5%D0%BB%D0%BA%D0%B8-%D1%83%D1%87%D0%B8%D1%82%D0%B5%D0%BB%D0%B8-%D1%83%D1%87%D0%B0%D1%89%D0%B8-%D0%B8-%D0%B7%D0%B0%D0%B2%D1%8A%D1%80%D1%88%D0%B8%D0%BB%D0%B8-%D0%B2-%D0%BE%D0%B1%D1%89%D0%BE%D0%BE%D0%B1%D1%80%D0%B0%D0%B7%D0%BE%D0%B2%D0%B0%D1%82%D0%B5%D0%BB%D0%BD%D0%B8-%D0%B8-%D1%81%D0%BF%D0%B5%D1%86%D0%B8%D0%B0%D0%BB%D0%BD%D0%B8-%D1%83%D1%87%D0%B8%D0%BB%D0%B8%D1%89%D0%B0-%D0%BF%D0%BE-%D1%81%D1%82%D0%B0%D1%82%D0%B8%D1%81%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B8-%D0%B7%D0%BE%D0%BD%D0%B8-%D1%81%D1%82%D0%B0%D1%82%D0%B8%D1%81%D1%82%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B8-%D1%80%D0%B0%D0%B9%D0%BE%D0%BD%D0%B8-%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D0%B8-%D0%B8-%D0%BE%D0%B1%D1%89%D0%B8%D0%BD%D0%B8"
    }

    final_table = []

    for url_key, url_value in urls.items():

        if url_key == 'schools':
            response = requests.get(url_value)
            response.encoding = 'utf-8'  # Ensure the correct encoding is used
            soup = BeautifulSoup(response.text, 'html.parser')
            # Find the table and extract data
            table_html = soup.find('tbody')
            rows = table_html.find_all('tr')[1:] # Skip the 1 header row
            table = []

            for row in rows:
                columns = row.find_all('td')
                object = {
                    'name_cyrillic': columns[0].get_text(strip=True),   # Municipality (община)
                    'school_count': columns[1].get_text(strip=True).replace('\xa0', ''),  # Total Population (общо)
                    'school_teachers': columns[2].get_text(strip=True).replace('\xa0', ''),   # Male Population (мъже)
                    'school_14_classes': columns[3].get_text(strip=True).replace('\xa0', ''),  # Female Population (жени)
                    'school_14_students': columns[4].get_text(strip=True).replace('\xa0', ''),  # Total Population (общо)
                    'school_14_female_students': columns[5].get_text(strip=True).replace('\xa0', ''),   # Male Population (мъже)
                    'school_512_classes': columns[6].get_text(strip=True).replace('\xa0', ''),
                    'school_512_students': columns[7].get_text(strip=True).replace('\xa0', ''),
                    'school_512_female_students': columns[8].get_text(strip=True).replace('\xa0', ''),
                    'school_512_mid_graduated': columns[9].get_text(strip=True).replace('\xa0', '')
                }
                if object['name_cyrillic'] and not object['name_cyrillic'].isdigit():
                    table.append(object)
            final_table.append(table)

        if url_key == 'kinder_gardens':
            response = requests.get(url_value)
            response.encoding = 'utf-8'  # Ensure the correct encoding is used
            soup = BeautifulSoup(response.text, 'html.parser')
            # Find the table and extract data
            table_html = soup.find('tbody')
            rows = table_html.find_all('tr')[1:] # Skip the 1 header row
            table = []

            for row in rows:
                columns = row.find_all('td')
                object = {
                    'name_cyrillic': columns[0].get_text(strip=True),   # Municipality (община)
                    'kd_count': columns[1].get_text(strip=True).replace('\xa0', ''),  # Total Population (общо)
                    'kd_total_children': columns[2].get_text(strip=True).replace('\xa0', ''),   # Male Population (мъже)
                    'kd_female_children': columns[3].get_text(strip=True).replace('\xa0', ''),  # Female Population (жени)
                    'kd_total_personnel': columns[4].get_text(strip=True).replace('\xa0', ''),  # Total Population (общо)
                    'kd_children_personnel': columns[5].get_text(strip=True).replace('\xa0', ''),   # Male Population (мъже)
                    'kd_groups_count': columns[6].get_text(strip=True).replace('\xa0', ''),  # Female Population (жени)
                }
                if object['name_cyrillic'] and not object['name_cyrillic'].isdigit():
                    table.append(object)
            final_table.append(table)

        if url_key == 'relocation':
            response = requests.get(url_value)
            response.encoding = 'utf-8'  # Ensure the correct encoding is used
            soup = BeautifulSoup(response.text, 'html.parser')
            # Find the table and extract data
            table_html = soup.find('tbody')
            rows = table_html.find_all('tr')[1:] # Skip the 1 header row
            table = []

            for row in rows:
                columns = row.find_all('td')
                object = {
                    'name_cyrillic': columns[0].get_text(strip=True),   # Municipality (община)
                    'total_relocation_in': columns[1].get_text(strip=True).replace('\xa0', ''),  # Total Population (общо)
                    'male_relocation_in': columns[2].get_text(strip=True).replace('\xa0', ''),   # Male Population (мъже)
                    'female_relocation_in': columns[3].get_text(strip=True).replace('\xa0', ''),  # Female Population (жени)
                    'total_relocation_out': columns[4].get_text(strip=True).replace('\xa0', ''),  # Total Population (общо)
                    'male_relocation_out': columns[5].get_text(strip=True).replace('\xa0', ''),   # Male Population (мъже)
                    'female_relocation_out': columns[6].get_text(strip=True).replace('\xa0', ''),  # Female Population (жени)
                    'total_relocation_growth': columns[7].get_text(strip=True).replace('\xa0', ''),  # Total Population (общо)
                    'male_relocation_growth': columns[8].get_text(strip=True).replace('\xa0', ''),   # Male Population (мъже)
                    'female_relocation_growth': columns[9].get_text(strip=True).replace('\xa0', ''),  # Female Population (жени)
                }
                if object['name_cyrillic'] and not object['name_cyrillic'].isdigit():
                    table.append(object)
            final_table.append(table)

        if url_key == 'population':
            response = requests.get(url_value)
            response.encoding = 'utf-8'  # Ensure the correct encoding is used
            soup = BeautifulSoup(response.text, 'html.parser')
            # Find the table and extract data
            table_html = soup.find('tbody')
            rows = table_html.find_all('tr')[1:] # Skip the 1 header row
            table = []

            for row in rows:
                columns = row.find_all('td')
                object = {
                    'name_cyrillic': columns[0].get_text(strip=True),   # Municipality (община)
                    'total_population': columns[1].get_text(strip=True).replace('\xa0', ''),  # Total Population (общо)
                    'male_population': columns[2].get_text(strip=True).replace('\xa0', ''),   # Male Population (мъже)
                    'female_population': columns[3].get_text(strip=True).replace('\xa0', ''),  # Female Population (жени)
                    'total_population_cities': columns[4].get_text(strip=True).replace('\xa0', ''),  # Total Population (общо)
                    'male_population_cities': columns[5].get_text(strip=True).replace('\xa0', ''),   # Male Population (мъже)
                    'female_population_cities': columns[6].get_text(strip=True).replace('\xa0', ''),  # Female Population (жени)
                    'total_population_villages': columns[7].get_text(strip=True).replace('\xa0', ''),  # Total Population (общо)
                    'male_population_villages': columns[8].get_text(strip=True).replace('\xa0', ''),   # Male Population (мъже)
                    'female_population_villages': columns[9].get_text(strip=True).replace('\xa0', ''),  # Female Population (жени)
                }
                if object['name_cyrillic'] and not object['name_cyrillic'].isdigit():
                    table.append(object)
            final_table.append(table)
        
        if url_key == 'deaths':
            response = requests.get(url_value)
            response.encoding = 'utf-8'  # Ensure the correct encoding is used
            soup = BeautifulSoup(response.text, 'html.parser')
            # Find the table and extract data
            table_html = soup.find('tbody')
            rows = table_html.find_all('tr')[1:] # Skip the 1 header row
            table = []

            for row in rows:
                columns = row.find_all('td')
                object = {
                    'name_cyrillic': columns[0].get_text(strip=True),   # Municipality (община)
                    'deaths_all': columns[1].get_text(strip=True).replace('\xa0', ''),  # Total Deaths (общо)
                    'deaths_males': columns[2].get_text(strip=True).replace('\xa0', ''),   # Male Deaths (мъже)
                    'deaths_females': columns[3].get_text(strip=True).replace('\xa0', '')  # Female Deaths (жени)
                }
                if object['name_cyrillic'] and not object['name_cyrillic'].isdigit():
                    table.append(object)
            final_table.append(table)
        
        if url_key == 'born':
            response = requests.get(url_value)
            response.encoding = 'utf-8'  # Ensure the correct encoding is used
            soup = BeautifulSoup(response.text, 'html.parser')
            # Find the table and extract data
            table_html = soup.find('tbody')
            rows = table_html.find_all('tr')[1:] # Skip the 1 header row
            table = []

            for row in rows:
                columns = row.find_all('td')
                object = {
                    'name_cyrillic': columns[0].get_text(strip=True),   # Municipality (община)
                    'born_all': columns[1].get_text(strip=True).replace('\xa0', ''),  # Total Born (общо)
                    'born_males': columns[2].get_text(strip=True).replace('\xa0', ''),   # Male Born (мъже)
                    'born_females': columns[3].get_text(strip=True).replace('\xa0', '')  # Female Born (жени)
                }
                if object['name_cyrillic'] and not object['name_cyrillic'].isdigit():
                    table.append(object)
            final_table.append(table)
    
    # Convert each list within final_table to a DataFrame
    df_list = [pd.DataFrame(data) for data in final_table]

    # Merge all DataFrames on the 'municipality' column
    df_combined = df_list[0]
    for df in df_list[1:]:
        df_combined = pd.merge(df_combined, df, on="name_cyrillic", how="outer")

    # Convert the combined DataFrame back to a list of dictionaries (optional)
    combined_data = df_combined.to_dict(orient='records')

    # Output as JSON (optional)
    json_data = json.dumps(combined_data, ensure_ascii=False, indent=4)

    existing_file_path = 'data/nsi-municipalities-updated.json'

    # Load the existing data from the JSON file
    with open(existing_file_path, 'r', encoding='utf-8') as json_file:
        existing_data = json.load(json_file)

    # Create a lookup dictionary for the combined data
    combined_lookup = {entry["name_cyrillic"]: entry for entry in combined_data}

    # Merge the data
    for item in existing_data:
        name = item.get("name_cyrillic")
        if name in combined_lookup:
            # Update the existing item with the missing data from combined data
            item.update(combined_lookup[name])

    # Write the merged data back to a JSON file
    output_file_path = 'data/merged_data.json'
    with open(output_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(existing_data, json_file, ensure_ascii=False, indent=4)

    # print(combined_data)
    # combined_data_keys = []
        # pprint.pprint(combined_data[0].keys())
    return combined_data

