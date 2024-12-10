from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from webdriver_manager.microsoft import EdgeChromiumDriverManager
import pandas as pd

# Function to set up the Selenium WebDriver
def setup_driver(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
    service = Service(EdgeChromiumDriverManager().install())
    return webdriver.Edge(service=service, options=options)

# Function to scrape the table using Selenium
def scrape_table_with_selenium(driver):
    # Locate the <pre> element
    pre_element = driver.find_element(By.CSS_SELECTOR, "div#listing pre")
    rows = pre_element.text.strip().split("\n")[2:]  # Skip header and separator lines

    # Locate all <a> tags for URLs
    a_tags = driver.find_elements(By.XPATH, "//pre//a")

    if len(rows) != len(a_tags):
        print("Warning: Number of rows and <a> tags do not match!")

    data = []
    for i, row in enumerate(rows):
        # Correctly split the row to ensure the timestamp and size remain intact
        parts = row.split(maxsplit=3)  # Split into at most 4 parts
        if len(parts) < 4:
            continue

        # Extract metadata
        last_modified = parts[0]  # Full timestamp
        size = f"{parts[1]} {parts[2]}"  # Combine size value and unit
        key = parts[3]

        # Extract corresponding URL, ensuring alignment with rows
        full_url = a_tags[i].get_attribute("href") if i < len(a_tags) else None

        # Skip specific unwanted URL
        if full_url == "https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html?prefix=data/":
            continue

        # Log the extracted data
        print(f"Row {i}: Last Modified: {last_modified}, Size: {size}, Key: {key}, URL: {full_url}")

        # Append the extracted data
        data.append({
            "Last Modified": last_modified,
            "Size": size,
            "Key": key,
            "URL": full_url
        })

    # Convert to DataFrame
    return pd.DataFrame(data)

# Main function to handle navigation and scraping
def main():
    url = "https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html"
    headless_mode = True  # Set to False for debugging

    # Initialize Selenium WebDriver
    driver = setup_driver(headless=headless_mode)

    try:
        print(f"Opening URL: {url}")
        driver.get(url)

        # Wait for the <a> tags to load
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
        )

        # Extract all links on the first page
        links = driver.find_elements(By.TAG_NAME, "a")
        urls = [link.get_attribute("href") for link in links if link.get_attribute("href") is not None]

        # Skip the specific unwanted link
        urls = [url for url in urls if url != "https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html?prefix=data/"]

        if not urls:
            print("No URLs found on the page.")
            return

        # Navigate to the last link in the list
        last_url = urls[-1]
        print(f"Navigating to the last link: {last_url}")
        driver.get(last_url)

        # Wait for the new page to load
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div#listing pre"))
        )

        # Scrape the table
        data_df = scrape_table_with_selenium(driver)

        if not data_df.empty:
            print("Scraped Data:")
            print(data_df)

            # Save the scraped data to a CSV file
            # data_df.to_csv("discogs_data_corrected.csv", index=False)
            # print("Data saved to discogs_data_corrected.csv")
        else:
            print("No data found or an error occurred while scraping.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.quit()

# Run the script
if __name__ == "__main__":
    main()
