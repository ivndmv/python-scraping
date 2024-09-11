from flask import Flask, render_template
import requests
from bs4 import BeautifulSoup

app = Flask(__name__)

# Step 2: Scrape the table data from the provided URL
URL = "https://nsi.bg/bg/content/2975/%D0%BD%D0%B0%D1%81%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D0%B5-%D0%BF%D0%BE-%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D0%B8-%D0%BE%D0%B1%D1%89%D0%B8%D0%BD%D0%B8-%D0%BC%D0%B5%D1%81%D1%82%D0%BE%D0%B6%D0%B8%D0%B2%D0%B5%D0%B5%D0%BD%D0%B5-%D0%B8-%D0%BF%D0%BE%D0%BB"
response = requests.get(URL)
response.encoding = 'utf-8'  # Ensure the correct encoding is used
soup = BeautifulSoup(response.text, 'html.parser')

# Find the table and extract data
table = soup.find('table')
cities_data = []

for row in table.find_all('tr')[1:]:  # Skip the header row
    columns = row.find_all('td')
    if len(columns) > 1:  # Ensure it's not an empty row
        city_data = {
            'region': columns[0].get_text(strip=True),         # Region (област)
            'municipality': columns[1].get_text(strip=True),   # Municipality (община)
            'city': columns[2].get_text(strip=True),           # City/Town (населено място)
            'total_population': columns[3].get_text(strip=True),  # Total Population (общо)
            'male_population': columns[4].get_text(strip=True),   # Male Population (мъже)
            'female_population': columns[5].get_text(strip=True)  # Female Population (жени)
        }
        # Exclude rows that don't represent a city/town (e.g., aggregated data like "Общо за страната")
        if city_data['city'] and not city_data['city'].isdigit():
            cities_data.append(city_data)

# Step 3: Create routes for each city/town
@app.route('/')
def index():
    return render_template('index.html', cities=cities_data)

@app.route('/city/<city_name>')
def city_page(city_name):
    city_info = next((city for city in cities_data if city['city'] == city_name), None)
    if city_info:
        return render_template('city.html', city=city_info)
    else:
        return "City not found", 404

if __name__ == '__main__':
    app.run(debug=True)