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
import ttkbootstrap as tb  # Import ttkbootstrap
from pathlib import Path  # Klasör yönetimi için


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

        self.root.title("Discogs Verileri İndirici")

        self.create_menu()
        self.create_widgets()
        # Hoş geldiniz mesajı
        self.log_to_console("Hoş geldiniz! Discogs Verileri İndirici'ye hoş geldiniz.", "INFO")
        self.log_to_console("Güncel datasetler kazınıyor, lütfen bekleyiniz...", "INFO")
        # Pencereyi ekranın ortasına yerleştir
        window_width = 800
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
        file_menu.add_command(label="Çıkış", command=self.root.quit)
        menubar.add_cascade(label="Dosya", menu=file_menu)

        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="Hakkında",
                              command=lambda: messagebox.showinfo("Hakkında", "Discogs Verileri İndirici"))
        menubar.add_cascade(label="Yardım", menu=help_menu)

    def create_widgets(self):
        table_frame = tb.Frame(self.root)
        table_frame.pack(expand=True, fill=tk.BOTH, padx=10, pady=10)

        self.tree = tb.Treeview(
            table_frame,
            columns=("month", "content", "size"),
            show="headings"
        )
        for col in self.tree["columns"]:
            self.tree.heading(col, text=col.capitalize(), anchor=tk.CENTER)
            self.tree.column(col, width=100, anchor=tk.CENTER)

        # Renklendirme için alternatif satır renkleri
        self.tree.tag_configure("evenrow", background="#343a40", foreground="#f8f9fa")
        self.tree.tag_configure("oddrow", background="#495057", foreground="#f8f9fa")

        self.tree.pack(expand=True, fill=tk.BOTH)

        control_frame = tb.Frame(self.root, padding=5)
        control_frame.pack(fill=tk.X)

        tb.Button(control_frame, text="Seçiliyi İndir", command=self.download_selected).pack(side=tk.LEFT, padx=5)
        tb.Button(control_frame, text="Durdur", command=self.stop_download).pack(side=tk.LEFT, padx=5)
        tb.Button(control_frame, text="Discogs Klasörünü Aç", command=self.open_discogs_folder).pack(side=tk.LEFT,
                                                                                                     padx=5)

        # New button for processing downloaded files
        tb.Button(control_frame, text="İndirilenleri İşle", command=self.process_downloaded_files).pack(side=tk.LEFT,
                                                                                                        padx=5)

        progress_frame = tb.Frame(self.root, padding=5)
        progress_frame.pack(fill=tk.X)

        self.progress_var = tk.IntVar()
        self.progress_bar = tb.Progressbar(progress_frame, variable=self.progress_var, length=250)
        self.progress_bar.pack(fill=tk.X, padx=5, pady=5)

        self.status_label = tb.Label(
            self.root,
            text="Hazır",
            font=("Arial", 12),
            anchor="center"
        )
        self.status_label.pack(fill=tk.X, side=tk.BOTTOM, pady=5)

        # Konsol Kutusu Ekleme
        console_frame = tb.Frame(self.root, padding=5)
        console_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.console_text = tk.Text(console_frame, height=8, state=tk.DISABLED, wrap=tk.WORD)
        self.console_text.pack(fill=tk.BOTH, expand=True)

    def populate_table(self, data_df):
        # Clear existing rows
        for row in self.tree.get_children():
            self.tree.delete(row)

        # Define file path to check for downloaded files
        downloads_dir = Path.home() / "Downloads" / "Discogs" / "Datasets"

        # Configure tags for month-based coloring
        unique_months = data_df["month"].unique()
        color_map = {month: ("month1", "month2")[i % 2] for i, month in enumerate(unique_months)}

        self.tree.tag_configure("month1", background="#343a40", foreground="#f8f9fa")
        self.tree.tag_configure("month2", background="#495057", foreground="#f8f9fa")

        # Configure tag for downloaded files with higher visibility
        self.tree.tag_configure("downloaded", background="#28a745", foreground="#ffffff")

        # Insert rows with appropriate tags
        for _, row in data_df.iterrows():
            # Determine the month-based tag
            month_tag = color_map[row["month"]]

            # Construct the expected file path for the current row
            file_path = downloads_dir / row["month"].strftime("%Y-%m") / row["key"]

            # Check if the file is downloaded
            is_downloaded = file_path.exists()

            # Combine tags: downloaded tag takes precedence
            tags = ("downloaded", month_tag) if is_downloaded else (month_tag,)

            # Insert the row with the combined tags
            self.tree.insert("", "end", values=row.tolist(), tags=tags)

    def log_to_console(self, message, message_type="INFO"):
        """
        Logs messages to the console with timestamp and type.

        :param message: The message to display.
        :param message_type: The type of message: INFO, WARNING, ERROR.
        """
        self.console_text.config(state=tk.NORMAL)

        # Get current timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Define message prefixes and colors
        prefixes = {
            "INFO": "[INFO]",
            "WARNING": "[WARNING]",
            "ERROR": "[ERROR]"
        }
        colors = {
            "INFO": "#00FF00",  # Green
            "WARNING": "#FFFF00",  # Yellow
            "ERROR": "#FF0000"  # Red
        }
        prefix = prefixes.get(message_type, "[INFO]")
        color = colors.get(message_type, "#FFFFFF")  # Default to white

        # Format the message with timestamp
        formatted_message = f"{timestamp} {prefix} {message}\n"

        # Insert the message with the chosen color
        self.console_text.tag_config(message_type, foreground=color)
        self.console_text.insert(tk.END, formatted_message, message_type)

        self.console_text.see(tk.END)
        self.console_text.config(state=tk.DISABLED)

    def save_to_file(self):
        try:
            downloads_dir = Path.home() / "Downloads"
            discogs_dir = downloads_dir / "Discogs"

            # Klasör oluştur
            discogs_dir.mkdir(parents=True, exist_ok=True)

            # Dosyayı kaydet
            file_path = discogs_dir / "discogs_dataset_list.csv"
            self.data_df.to_csv(file_path, sep="\t", index=False)

            self.log_to_console(f"Veri {file_path} olarak kaydedildi.")
        except Exception as e:
            self.log_to_console(f"Hata: {e}")

    def download_file(self, url, filename, folder_name):
        try:
            self.log_to_console("İndirme Başladı...", "INFO")

            # Downloads/Discogs/Datasets klasörü altında tarih bazlı klasör oluşturma
            downloads_dir = Path.home() / "Downloads"
            discogs_dir = downloads_dir / "Discogs"
            datasets_dir = discogs_dir / "Datasets"
            target_dir = datasets_dir / folder_name  # Örn: 2024-12
            target_dir.mkdir(parents=True, exist_ok=True)

            # Dosyanın tam yolu
            file_path = target_dir / filename

            # İndirme işlemi
            response = requests.get(url, stream=True)
            total_size = int(response.headers.get('content-length', 0))
            block_size = 1024
            self.progress_var.set(0)
            self.progress_bar["maximum"] = total_size

            downloaded_size = 0
            with open(file_path, "wb") as file:
                for data in response.iter_content(block_size):
                    if self.stop_flag:
                        self.status_label.config(text="İndirme Durduruldu")
                        self.log_to_console("İndirme Durduruldu", "WARNING")
                        return

                    file.write(data)
                    downloaded_size += len(data)
                    self.progress_var.set(downloaded_size)

                    # Yüzde hesaplama ve etiket güncelleme
                    percentage = (downloaded_size / total_size) * 100
                    self.status_label.config(text=f"İndirme: %{percentage:.2f}")

            self.status_label.config(text="İndirme Tamamlandı!")
            self.log_to_console(f"{filename} başarıyla indirildi: {file_path}", "INFO")
        except Exception as e:
            self.log_to_console(f"Hata: {e}", "ERROR")

    def start_download(self, url, filename, last_modified):
        self.stop_flag = False

        # Dosya için klasör adı oluşturma (tarihi yyyy-mm formatında kullanıyoruz)
        folder_name = last_modified.strftime("%Y-%m")

        # İndirme işlemi başlat
        Thread(target=self.download_file, args=(url, filename, folder_name)).start()


    def stop_download(self):
        self.stop_flag = True
        self.log_to_console("İndirme durduruluyor...")

    def download_selected(self):
        selected_item = self.tree.selection()
        if not selected_item:
            messagebox.showwarning("Uyarı", "Dosya seçilmedi!")
            return

        for item in selected_item:
            values = self.tree.item(item, "values")
            url = values[-1]
            filename = os.path.basename(url)
            last_modified = pd.to_datetime(values[3])  # 'last_modified' alanı 3. sütunda

            self.start_download(url, filename, last_modified)

    def process_downloaded_files(self):
        try:
            downloads_dir = Path.home() / "Downloads" / "Discogs" / "Datasets"
            if not downloads_dir.exists():
                self.log_to_console("İndirilen dosyalar bulunamadı!", "WARNING")
                return

            self.log_to_console("İndirilen dosyalar işleniyor...", "INFO")

            processed_files = []
            for folder in downloads_dir.iterdir():
                if folder.is_dir():
                    for file in folder.iterdir():
                        if file.suffix in ['.csv', '.gz']:  # Example of valid file types
                            processed_files.append(file.name)

            if processed_files:
                self.log_to_console(f"{len(processed_files)} dosya işlendi: {', '.join(processed_files)}", "INFO")
            else:
                self.log_to_console("İşlenecek uygun dosya bulunamadı.", "WARNING")
        except Exception as e:
            self.log_to_console(f"Dosya işleme sırasında hata: {e}", "ERROR")

    def open_discogs_folder(self):
        try:
            # Discogs klasörü yolunu belirle
            downloads_dir = Path.home() / "Downloads"
            discogs_dir = downloads_dir / "Discogs"

            # Platforma göre farklı yöntemlerle klasörü aç
            if not discogs_dir.exists():
                self.log_to_console(f"{discogs_dir} klasörü bulunamadı!")
                messagebox.showerror("Hata", f"{discogs_dir} klasörü bulunamadı!")
                return

            if platform.system() == "Windows":
                os.startfile(discogs_dir)
            elif platform.system() == "Darwin":  # macOS
                subprocess.run(["open", str(discogs_dir)])
            else:  # Linux/Unix
                subprocess.run(["xdg-open", str(discogs_dir)])
        except Exception as e:
            self.log_to_console(f"Klasör açılamadı: {e}")
            messagebox.showerror("Hata", f"Klasör açılamadı: {e}")
    def save_to_file(self):
        try:
            # Downloads/Discogs/Datasets klasörünü oluşturma
            downloads_dir = Path.home() / "Downloads"
            discogs_dir = downloads_dir / "Discogs"
            datasets_dir = discogs_dir / "Datasets"
            datasets_dir.mkdir(parents=True, exist_ok=True)

            # Dosyayı kaydetme
            file_path = datasets_dir / "discogs_dataset_list.csv"
            self.data_df.to_csv(file_path, sep="\t", index=False)

            self.log_to_console(f"Veri {file_path} olarak kaydedildi.")
        except Exception as e:
            self.log_to_console(f"Hata: {e}")

    def export_to_csv(self):
        save_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV Dosyaları", "*.csv")])
        if save_path:
            self.data_df.to_csv(save_path, index=False)
            self.log_to_console(f"Veri {save_path} olarak dışa aktarıldı.")

    def start_scraping(self):
        url = "https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html"
        driver = setup_driver(headless=True)
        try:
            self.log_to_console(f"URL açılıyor: {url}")
            driver.get(url)

            WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
            )

            links = driver.find_elements(By.TAG_NAME, "a")
            urls = [link.get_attribute("href") for link in links if link.get_attribute("href") is not None]

            last_url = urls[-1]
            self.log_to_console(f"Son bağlantıya gidiliyor: {last_url}")
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
                data_df = data_df[["month", "content", "size", "last_modified", "key", "URL"]]

                self.data_df = data_df
                self.populate_table(data_df)
                self.save_to_file()
                self.log_to_console("Scraping tamamlandı. İndirmek istediğiniz veri setini seçiniz ve İndir butonuna tıklayınız.", "INFO")
            else:
                self.log_to_console("Veri bulunamadı.")
        except Exception as e:
            self.log_to_console(f"Hata: {e}", "ERROR")


        finally:
            driver.quit()


def main():
    empty_df = pd.DataFrame(columns=["month", "content", "size", "last_modified", "key", "URL"])

    root = tb.Window(themename="darkly")
    app = App(root, empty_df)

    Thread(target=app.start_scraping, daemon=True).start()

    root.mainloop()


if __name__ == "__main__":
    main()
