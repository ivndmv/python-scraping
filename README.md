<h1>Municipalities-scraped-from-NSI</h1>

<p>
  <img src="https://img.shields.io/badge/version-In_Development-orange.svg" alt="Version">
</p>

<h2>Description</h2>
<p>
  Municipalities-scraped-from-NSI is an in-development Python application built with Flask. The app scrapes tables from <a href="https://www.nsi.bg/">nsi.bg</a> and dynamically generates web pages for each municipality based on the extracted data. The generated pages include relevant statistics and information pulled from the scraped tables, providing up-to-date data for each municipality.
</p>

<h2>Features</h2>
<ul>
  <li><strong>Data Scraping</strong>: Automatically scrapes statistical tables from nsi.bg.</li>
  <li><strong>Dynamic Page Generation</strong>: Generates web pages for all municipalities using the scraped data.</li>
  <li><strong>Flask Integration</strong>: Uses Flask to create a web application that serves the dynamically generated pages.</li>
</ul>

<h2>Installation</h2>
<ol>
  <li>Clone the repository: <code>git clone https://github.com/ivndmv/python-scraping.git</code></li>
  <li>Navigate to the project directory: <code>cd python-scraping</code></li>
  <li>Create and activate a virtual environment: <code>python -m venv venv && source venv/bin/activate</code></li>
  <li>Install the required dependencies: <code>pip install -r requirements.txt</code></li>
</ol>

<h2>Usage</h2>
<ol>
  <li>Run the Flask development server: <code>flask run</code></li>
  <li>Access the web application in your browser at <code>http://127.0.0.1:5000</code></li>
  <li>The application will scrape data from nsi.bg and generate municipality pages dynamically.</li>
</ol>

<h2>License</h2>
<p>
  This project is licensed under the MIT License - see the <a href="LICENSE">LICENSE</a> file for details.
</p>
