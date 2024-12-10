import tkinter as tk
from tkinter import ttk, messagebox
from threading import Thread
import requests
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from webdriver_manager.microsoft import EdgeChromiumDriverManager


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
    pre_element = driver.find_element(By.CSS_SELECTOR, "div#listing pre")
    rows = pre_element.text.strip().split("\n")[2:]
    a_tags = driver.find_elements(By.XPATH, "//pre//a")

    if len(rows) != len(a_tags):
        print("Warning: Number of rows and <a> tags do not match!")

    data = []
    for i, row in enumerate(rows):
        parts = row.split(maxsplit=3)
        if len(parts) < 4:
            continue

        last_modified = parts[0]
        size = f"{parts[1]} {parts[2]}"
        key = parts[3]
        full_url = a_tags[i].get_attribute("href") if i < len(a_tags) else None

        content_type = "unknown"
        if "checksum" in key.lower():
            content_type = "checksum"
        elif "artist" in key.lower():
            content_type = "artists"
        elif "master" in key.lower():
            content_type = "masters"
        elif "label" in key.lower():
            content_type = "labels"
        elif "release" in key.lower():
            content_type = "releases"

        data.append({
            "last_modified": last_modified,
            "size": size,
            "key": key,
            "content": content_type,
            "URL": full_url
        })

    return pd.DataFrame(data)


# Function to download files
def download_file(url, filename, progress_var, progress_bar, status_label):
    try:
        status_label.config(text=f"Downloading {filename}...", foreground="blue")
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024  # 1 Kibibyte
        progress_var.set(0)
        progress_bar["maximum"] = total_size

        with open(filename, "wb") as file:
            for data in response.iter_content(block_size):
                file.write(data)
                progress_var.set(progress_var.get() + len(data))

        status_label.config(text=f"Download completed: {filename}", foreground="green")
        messagebox.showinfo("Success", f"Download of {filename} completed.")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to download {filename}: {e}")
        status_label.config(text="Download Failed", foreground="red")


# Function to start the download in a separate thread
def start_download(urls, filenames, progress_var, progress_bar, status_label):
    def download_all():
        for url, filename in zip(urls, filenames):
            download_file(url, filename, progress_var, progress_bar, status_label)
    Thread(target=download_all).start()


# Main GUI application
class App:
    def __init__(self, root, data_df):
        self.root = root
        self.data_df = data_df

        self.root.title("Discogs Data Downloader")
        self.root.geometry("800x500")

        self.check_vars = []
        self.entries = []

        # Frame for data table
        table_frame = ttk.Frame(root)
        table_frame.pack(expand=True, fill=tk.BOTH, padx=10, pady=10)

        self.tree = ttk.Treeview(table_frame, columns=("month", "content", "size"), show="headings")
        self.tree.heading("month", text="Month")
        self.tree.heading("content", text="Content")
        self.tree.heading("size", text="Size")

        for col in self.tree["columns"]:
            self.tree.column(col, width=100)

        # Adding rows with checkboxes
        for i, row in data_df.iterrows():
            check_var = tk.BooleanVar(value=False)
            self.check_vars.append((check_var, row["URL"]))
            self.tree.insert("", "end", values=row[:-1].tolist())

        self.tree.pack(expand=True, fill=tk.BOTH)

        # Bottom frame for controls
        bottom_frame = ttk.Frame(root)
        bottom_frame.pack(fill=tk.X, padx=10, pady=10)

        self.download_button = ttk.Button(bottom_frame, text="Download Selected", command=self.download_selected)
        self.download_button.pack(side=tk.LEFT, padx=10)

        self.progress_var = tk.IntVar()
        self.progress_bar = ttk.Progressbar(bottom_frame, variable=self.progress_var, length=400)
        self.progress_bar.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=10)

        self.status_label = ttk.Label(bottom_frame, text="Ready", font=("Arial", 12), foreground="blue")
        self.status_label.pack(side=tk.LEFT, padx=10)

    def download_selected(self):
        selected_urls = []
        selected_filenames = []

        for var, url in self.check_vars:
            if var.get():
                selected_urls.append(url)
                selected_filenames.append(os.path.basename(url))

        if not selected_urls:
            messagebox.showwarning("Warning", "No files selected!")
            return

        start_download(selected_urls, selected_filenames, self.progress_var, self.progress_bar, self.status_label)


# Main function to scrape data and launch GUI
def main():
    url = "https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html"
    headless_mode = True

    driver = setup_driver(headless=headless_mode)
    try:
        print(f"Opening URL: {url}")
        driver.get(url)

        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
        )

        links = driver.find_elements(By.TAG_NAME, "a")
        urls = [link.get_attribute("href") for link in links if link.get_attribute("href") is not None]
        urls = [url for url in urls if
                url != "https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html?prefix=data/"]

        if not urls:
            print("No URLs found on the page.")
            return

        last_url = urls[-1]
        print(f"Navigating to the last link: {last_url}")
        driver.get(last_url)

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div#listing pre"))
        )

        data_df = scrape_table_with_selenium(driver)

        if not data_df.empty:
            data_df["last_modified"] = pd.to_datetime(data_df["last_modified"])
            data_df["month"] = data_df["last_modified"].dt.to_period("M")

            data_df = data_df[["month", "content", "size", "last_modified", "key", "URL"]]

            root = tk.Tk()
            app = App(root, data_df)
            root.mainloop()
        else:
            print("No data found or an error occurred while scraping.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
