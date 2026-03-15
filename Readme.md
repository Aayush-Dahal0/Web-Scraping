# Google Maps Business Leads Scraper (Nepal)

This project is a Python-based Google Maps scraper that collects detailed business information from Google Maps search results. It extracts data such as business name, address, phone number, website, ratings, and social media links.

The script searches for specific business categories across various cities, visits business websites to find contact details (email, Facebook, Instagram), and saves all extracted data into a formatted Excel spreadsheet.

This tool is useful for:
- Lead generation
- Market research
- Sales prospecting
- Competitor analysis
- Building business directories

## Features

- **Automated Searching:** Searches Google Maps using predefined combinations of keywords and cities.
- **Dynamic Scrolling:** Scrolls dynamically to load and capture all available business listings.
- **Core Data Extraction:** Opens each business profile to extract Name, Address, Phone, Website, Rating, and Reviews.
- **Deep Scraping:** Visits the business's website (if available) to scrape email addresses and social media links (Facebook, Instagram).
- **Deduplication:** Removes duplicate entries that may appear in multiple search combinations.
- **Excel Export:** Saves and formats the results into an easy-to-read Excel file (`nepal_business_leads.xlsx` or similar), complete with bold headers, text wrapping, and frozen panes.

## Technologies Used

- **Python**
- **Selenium:** For browser automation and handling dynamic content.
- **BeautifulSoup:** For parsing HTML and extracting data from external websites.
- **Requests:** For fetching website content.
- **Pandas:** For data manipulation and deduplication.
- **OpenPyXL:** For advanced Excel formatting.
- **WebDriver Manager:** To automatically handle browser driver setup.

## Configuration
1. Install Dependencies: pip install -r requirements.txt
2. Before running the script, you must configure the search parameters to match your targets. Open the Python script (e.g., `scraper.py`) and locate the following lists. 

**Change these keywords and cities as per your requirement:**

```python
keywords = [
    "education consultancy",
    "IT training institute",
    "digital marketing agency"
]

cities = [
    "kathmandu",
    "lalitpur",
    "bhaktapur",
    "pokhara"
]
```

The script will search for every combination (e.g., "education consultancy kathmandu", "education consultancy lalitpur", etc.).
3. Run the script: python scraper.py

## How It Works

1. **Query Generation:** The script iterates over all combinations of the provided keywords and cities.
2. **Loading Google Maps:** Uses Selenium to securely open the search results for each query.
3. **Scrolling for Results:** Executes JavaScript to scroll down the results panel until all businesses are visible.
4. **Data Extraction:** Extracts basic details straight from the Google Maps sidebar elements.
5. **Contact Scraping:** Visits any linked custom websites and uses regular expressions (Regex) to find hidden emails and traces social media profiles.
6. **Data Processing:** All gathered records are compiled into a Pandas DataFrame; duplicates (based on Business Name) are dropped.
7. **Finalization:** The cleaned data is written to an Excel file with styling adjustments for readability.

## Output Example

The resulting Excel file will contain structured data similar to:

| Category | City | Business Name | Address | Phone | Website | Email | Facebook |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| IT training institute | Kathmandu | XYZ Institute | Thamel, Kathmandu | 9800000000 | xyz.com | info@xyz.com | facebook.com/xyz |