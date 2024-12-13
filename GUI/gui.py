import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from threading import Thread
import requests
import os
import pandas as pd
import platform
import subprocess
from pathlib import Path
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from webdriver_manager.microsoft import EdgeChromiumDriverManager
import ttkbootstrap as tb
from ttkbootstrap import Style


def setup_driver(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
    service = Service(EdgeChromiumDriverManager().install())
    return webdriver.Edge(service=service, options=options)


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


class App:
    def __init__(self, root, data_df):
        self.root = root
        self.data_df = data_df
        self.stop_flag = False

        self.root.title("Discogs Data Downloader")
        self.root.tk.call('tk', 'scaling', 1.0)

        self.check_vars = {}
        self.checkbuttons = {}

        self.create_menu()
        self.create_widgets()
        self.log_to_console("Welcome! Welcome to the Discogs Data Downloader.", "INFO")
        self.log_to_console("Fetching the latest datasets, please wait...", "INFO")

        # Center the window on the screen
        window_width = 300
        window_height = 600
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        position_top = (screen_height // 2) - (window_height // 2)
        position_right = (screen_width // 2) - (window_width // 2)
        self.root.geometry(f"{window_width}x{window_height}+{position_right}+{position_top}")

    def create_menu(self):
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="Exit", command=self.root.quit)
        menubar.add_cascade(label="File", menu=file_menu)

        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="About",
                              command=lambda: messagebox.showinfo("About", "Discogs Data Downloader"))
        menubar.add_cascade(label="Help", menu=help_menu)

    def create_widgets(self):
        main_frame = tb.Frame(self.root)
        main_frame.pack(expand=True, fill=tk.BOTH, padx=10, pady=10)

        style = Style()
        style.configure("Treeview.Heading", background="#212121", foreground="#f8f9fa", font=("Arial", 12, "bold"),
                        relief="flat")

        self.tree_frame = tb.Frame(main_frame)
        self.tree_frame.pack(expand=True, fill=tk.BOTH)

        # Only " ", "month", "content", "size", "Downloaded" columns will be shown
        self.tree = tb.Treeview(
            self.tree_frame,
            columns=(" ", "month", "content", "size", "Downloaded"),
            show="headings"
        )

        for col in self.tree["columns"]:
            self.tree.heading(col, text=col.capitalize(), anchor=tk.CENTER)
            if col == " ":
                width = 30
            else:
                width = 100
            self.tree.column(col, width=width, anchor=tk.CENTER)

        self.tree.tag_configure("evenrow", background="#343a40", foreground="#f8f9fa")
        self.tree.tag_configure("oddrow", background="#495057", foreground="#f8f9fa")
        self.tree.pack(expand=True, fill=tk.BOTH)

        self.tree.bind("<Configure>", lambda e: self.position_checkbuttons())
        self.tree.bind("<ButtonRelease-1>", lambda e: self.position_checkbuttons())
        self.tree.bind("<<TreeviewSelect>>", lambda e: self.position_checkbuttons())
        self.tree_frame.bind("<Configure>", lambda e: self.position_checkbuttons())

        control_frame = tb.Frame(self.root, padding=5)
        control_frame.pack(fill=tk.X)
        tb.Button(control_frame, text="Download Selected", command=self.download_selected).pack(side=tk.LEFT, padx=5)
        tb.Button(control_frame, text="Stop", command=self.stop_download).pack(side=tk.LEFT, padx=5)
        tb.Button(control_frame, text="Open Discogs Folder", command=self.open_discogs_folder).pack(side=tk.LEFT,
                                                                                                   padx=5)

        progress_frame = tb.Frame(self.root, padding=5)
        progress_frame.pack(fill=tk.X)
        self.progress_var = tk.IntVar()
        self.progress_bar = tb.Progressbar(progress_frame, variable=self.progress_var, length=250)
        self.progress_bar.pack(fill=tk.X, padx=5, pady=5)

        self.status_label = tb.Label(
            self.root,
            text="Ready",
            font=("Arial", 12),
            anchor="center"
        )
        self.status_label.pack(fill=tk.X, side=tk.BOTTOM)

        console_frame = tb.Frame(self.root, padding=5)
        console_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.console_text = tk.Text(console_frame, height=8, state=tk.DISABLED, wrap=tk.WORD)
        self.console_text.pack(fill=tk.BOTH, expand=True)

    def populate_table(self, data_df):
        for row in self.tree.get_children():
            self.tree.delete(row)
        for cb in self.checkbuttons.values():
            cb.destroy()
        self.check_vars.clear()
        self.checkbuttons.clear()

        unique_months = data_df["month"].unique()
        color_map = {}
        for i, month in enumerate(unique_months):
            if i % 2 == 0:
                color_map[month] = "month1"
            else:
                color_map[month] = "month2"

        self.tree.tag_configure("month1", background="#343a40", foreground="#f8f9fa")
        self.tree.tag_configure("month2", background="#495057", foreground="#f8f9fa")

        # Only the specified columns from data_df are inserted into the tree.
        # data_df columns: ["month", "content", "size", "Downloaded"]
        for i, (_, row) in enumerate(data_df.iterrows()):
            tag = color_map[row["month"]]
            # First value is an empty string for the checkbox column
            values = ["", row["month"], row["content"], row["size"], row["Downloaded"]]
            item_id = self.tree.insert("", "end", values=values, tags=(tag,))
            var = tk.IntVar(value=0)
            cb = tb.Checkbutton(self.tree, variable=var)
            self.check_vars[item_id] = var
            self.checkbuttons[item_id] = cb

        self.position_checkbuttons()

    def position_checkbuttons(self):
        self.root.update_idletasks()

        if not self.checkbuttons:
            return

        for item_id, cb in self.checkbuttons.items():
            bbox = self.tree.bbox(item_id, column=0)
            if not bbox:
                cb.place_forget()
            else:
                x, y, width, height = bbox
                self.root.update_idletasks()
                cb_width = cb.winfo_reqwidth()
                cb_height = cb.winfo_reqheight()

                cb_x = x + (width - cb_width) // 2
                cb_y = y + (height - cb_height) // 2

                cb.place(in_=self.tree, x=cb_x, y=cb_y + 1, width=height - 2, height=height - 2)

    def log_to_console(self, message, message_type="INFO"):
        self.console_text.config(state=tk.NORMAL)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prefixes = {
            "INFO": "[INFO]",
            "WARNING": "[WARNING]",
            "ERROR": "[ERROR]"
        }
        colors = {
            "INFO": "#00FF00",
            "WARNING": "#FFFF00",
            "ERROR": "#FF0000"
        }
        prefix = prefixes.get(message_type, "[INFO]")
        color = colors.get(message_type, "#FFFFFF")
        formatted_message = f"{timestamp} {prefix} {message}\n"
        self.console_text.tag_config(message_type, foreground=color)
        self.console_text.insert(tk.END, formatted_message, message_type)
        self.console_text.see(tk.END)
        self.console_text.config(state=tk.DISABLED)

    def save_to_file(self):
        try:
            downloads_dir = Path.home() / "Downloads"
            discogs_dir = downloads_dir / "Discogs"
            datasets_dir = discogs_dir / "Datasets"
            datasets_dir.mkdir(parents=True, exist_ok=True)
            file_path = datasets_dir / "discogs_dataset_list.csv"
            self.data_df.to_csv(file_path, sep="\t", index=False)
            self.log_to_console(f"Data saved as {file_path}.")
        except Exception as e:
            self.log_to_console(f"Error: {e}")

    def download_file(self, url, filename, folder_name):
        try:
            self.log_to_console("Download started...", "INFO")
            downloads_dir = Path.home() / "Downloads"
            discogs_dir = downloads_dir / "Discogs"
            datasets_dir = discogs_dir / "Datasets"
            target_dir = datasets_dir / folder_name
            target_dir.mkdir(parents=True, exist_ok=True)
            file_path = target_dir / filename

            response = requests.get(url, stream=True)
            total_size = int(response.headers.get('content-length', 0))
            block_size = 1024
            self.progress_var.set(0)
            self.progress_bar["maximum"] = total_size

            downloaded_size = 0
            with open(file_path, "wb") as file:
                for data in response.iter_content(block_size):
                    if self.stop_flag:
                        self.status_label.config(text="Download Stopped")
                        self.log_to_console("Download Stopped", "WARNING")
                        return
                    file.write(data)
                    downloaded_size += len(data)
                    self.progress_var.set(downloaded_size)
                    percentage = (downloaded_size / total_size) * 100
                    self.status_label.config(text=f"Downloading: %{percentage:.2f}")

            self.status_label.config(text="Download Completed!")
            self.log_to_console(f"{filename} successfully downloaded: {file_path}", "INFO")

            # Update data_df after download completion
            self.data_df.loc[self.data_df["URL"] == url, "Downloaded"] = "✔"

            # Update the table
            self.populate_table(self.data_df[["month", "content", "size", "Downloaded"]])

        except Exception as e:
            self.log_to_console(f"Error: {e}", "ERROR")

    def start_download(self, url, filename, last_modified):
        self.stop_flag = False
        folder_name = last_modified.strftime("%Y-%m")
        Thread(target=self.download_file, args=(url, filename, folder_name)).start()

    def stop_download(self):
        self.stop_flag = True
        self.log_to_console("Stopping the download...")

    def download_selected(self):
        checked_items = [item for item, var in self.check_vars.items() if var.get() == 1]
        if not checked_items:
            messagebox.showwarning("Warning", "No file selected!")
            return

        for item in checked_items:
            values = self.tree.item(item, "values")
            # values: ["", month, content, size, Downloaded]
            # Find this row in data_df:
            month_val = values[1]
            content_val = values[2]
            size_val = values[3]
            downloaded_val = values[4]

            # Locate matching row in data_df:
            row_data = self.data_df[
                (self.data_df["month"] == month_val) &
                (self.data_df["content"] == content_val) &
                (self.data_df["size"] == size_val) &
                (self.data_df["Downloaded"] == downloaded_val)
            ]
            if not row_data.empty:
                url = row_data["URL"].values[0]
                last_modified = row_data["last_modified"].values[0]
                last_modified = pd.to_datetime(last_modified)
                filename = os.path.basename(url)
                self.start_download(url, filename, last_modified)

    def open_discogs_folder(self):
        try:
            downloads_dir = Path.home() / "Downloads"
            discogs_dir = downloads_dir / "Discogs"
            if not discogs_dir.exists():
                self.log_to_console(f"{discogs_dir} folder not found!")
                messagebox.showerror("Error", f"{discogs_dir} folder not found!")
                return
            if platform.system() == "Windows":
                os.startfile(discogs_dir)
            elif platform.system() == "Darwin":
                subprocess.run(["open", str(discogs_dir)])
            else:
                subprocess.run(["xdg-open", str(discogs_dir)])
        except Exception as e:
            self.log_to_console(f"Cannot open folder: {e}")
            messagebox.showerror("Error", f"Cannot open folder: {e}")

    def export_to_csv(self):
        save_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV Files", "*.csv")])
        if save_path:
            self.data_df.to_csv(save_path, index=False)
            self.log_to_console(f"Data exported as {save_path}.")

    def mark_downloaded_files(self, data_df):
        downloads_dir = Path.home() / "Downloads"
        discogs_dir = downloads_dir / "Discogs"
        datasets_dir = discogs_dir / "Datasets"

        downloaded_status = []
        for _, row in data_df.iterrows():
            folder_name = str(row["month"])
            filename = os.path.basename(row["URL"])
            file_path = datasets_dir / folder_name / filename
            if file_path.exists():
                downloaded_status.append("✔")  # Tick
            else:
                downloaded_status.append("✖")  # Cross
        data_df["Downloaded"] = downloaded_status
        return data_df

    def start_scraping(self):
        url = "https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html"
        driver = setup_driver(headless=True)
        try:
            self.log_to_console(f"Opening URL: {url}")
            driver.get(url)
            WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
            )
            links = driver.find_elements(By.TAG_NAME, "a")
            urls = [link.get_attribute("href") for link in links if link.get_attribute("href") is not None]
            last_url = urls[-1]
            self.log_to_console(f"Navigating to the last link: {last_url}")
            driver.get(last_url)
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div#listing pre"))
            )
            data_df = scrape_table_with_selenium(driver)
            if not data_df.empty:
                data_df["last_modified"] = pd.to_datetime(data_df["last_modified"])
                data_df["month"] = data_df["last_modified"].dt.to_period("M")
                content_order = {"artists": 1, "labels": 2, "masters": 3, "releases": 4}
                data_df = data_df[data_df["content"] != "checksum"]
                data_df["content_order"] = data_df["content"].map(content_order)
                data_df = data_df.sort_values(by=["month", "content_order"], ascending=[False, True])
                # last_modified and URL remain for internal usage
                data_df = self.mark_downloaded_files(data_df)

                self.data_df = data_df
                self.populate_table(data_df[["month", "content", "size", "Downloaded"]])
                self.save_to_file()
                self.log_to_console(
                    "Scraping completed. Please select the dataset you wish to download and click Download.",
                    "INFO")
            else:
                self.log_to_console("No data found.")
        except Exception as e:
            self.log_to_console(f"Error: {e}", "ERROR")
        finally:
            driver.quit()


def main():
    empty_df = pd.DataFrame(columns=["month", "content", "size", "last_modified", "key", "URL"])
    root = tb.Window(themename="darkly")
    root.tk.call("tk", "scaling", 1.0)
    app = App(root, empty_df)
    Thread(target=app.start_scraping, daemon=True).start()
    root.mainloop()


if __name__ == "__main__":
    main()
