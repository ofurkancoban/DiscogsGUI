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

        self.root.title("Discogs Verileri İndirici")
        self.root.tk.call('tk', 'scaling', 1.0)

        self.check_vars = {}
        self.checkbuttons = {}

        self.create_menu()
        self.create_widgets()
        self.log_to_console("Hoş geldiniz! Discogs Verileri İndirici'ye hoş geldiniz.", "INFO")
        self.log_to_console("Güncel datasetler kazınıyor, lütfen bekleyiniz...", "INFO")

        # Pencereyi ekranın ortasına yerleştir
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
        file_menu.add_command(label="Çıkış", command=self.root.quit)
        menubar.add_cascade(label="Dosya", menu=file_menu)

        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="Hakkında",
                              command=lambda: messagebox.showinfo("Hakkında", "Discogs Verileri İndirici"))
        menubar.add_cascade(label="Yardım", menu=help_menu)

    def create_widgets(self):
        main_frame = tb.Frame(self.root)
        main_frame.pack(expand=True, fill=tk.BOTH, padx=10, pady=10)

        style = Style()
        style.configure("Treeview.Heading", background="#212121", foreground="#f8f9fa", font=("Arial", 12, "bold"),
                        relief="flat")

        self.tree_frame = tb.Frame(main_frame)
        self.tree_frame.pack(expand=True, fill=tk.BOTH)

        # Sadece " ", "month", "content", "size", "Downloaded" kolonları gösterilecek
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
        tb.Button(control_frame, text="Seçiliyi İndir", command=self.download_selected).pack(side=tk.LEFT, padx=5)
        tb.Button(control_frame, text="Durdur", command=self.stop_download).pack(side=tk.LEFT, padx=5)
        tb.Button(control_frame, text="Discogs Klasörünü Aç", command=self.open_discogs_folder).pack(side=tk.LEFT,
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

        # Sadece belirtilen kolonları data_df'den alarak tree'ye ekliyoruz
        # data_df columns: ["month", "content", "size", "Downloaded"]
        for i, (_, row) in enumerate(data_df.iterrows()):
            tag = color_map[row["month"]]
            # İlk değer checkbox kolonuna boş string
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
            self.log_to_console(f"Veri {file_path} olarak kaydedildi.")
        except Exception as e:
            self.log_to_console(f"Hata: {e}")

    def download_file(self, url, filename, folder_name):
        try:
            self.log_to_console("İndirme Başladı...", "INFO")
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
                        self.status_label.config(text="İndirme Durduruldu")
                        self.log_to_console("İndirme Durduruldu", "WARNING")
                        return
                    file.write(data)
                    downloaded_size += len(data)
                    self.progress_var.set(downloaded_size)
                    percentage = (downloaded_size / total_size) * 100
                    self.status_label.config(text=f"İndirme: %{percentage:.2f}")

            self.status_label.config(text="İndirme Tamamlandı!")
            self.log_to_console(f"{filename} başarıyla indirildi: {file_path}", "INFO")

            # İndirme tamamlandığında data_df'yi güncelle
            self.data_df.loc[self.data_df["URL"] == url, "Downloaded"] = "✔"

            # Tabloyu güncelle
            self.populate_table(self.data_df[["month", "content", "size", "Downloaded"]])

        except Exception as e:
            self.log_to_console(f"Hata: {e}", "ERROR")

    def start_download(self, url, filename, last_modified):
        self.stop_flag = False
        folder_name = last_modified.strftime("%Y-%m")
        Thread(target=self.download_file, args=(url, filename, folder_name)).start()

    def stop_download(self):
        self.stop_flag = True
        self.log_to_console("İndirme durduruluyor...")

    def download_selected(self):
        checked_items = [item for item, var in self.check_vars.items() if var.get() == 1]
        if not checked_items:
            messagebox.showwarning("Uyarı", "Dosya seçilmedi!")
            return

        # Bu bölümde URL, last_modified elde etmek için orijinal data_df kullanılmalı
        # Çünkü tabloya sadece gösterilen kolonlar var. Eğer orijinal data_df kullanacaksanız
        # burayı ona göre güncelleyin. Aşağıda basitçe her satırı tekrar data_df ile eşleştiriyoruz.

        # İndirilecek URL'leri bulurken month ve content üzerinden eşleşme yapabiliriz.
        # Ancak ideal olarak key veya URL'yi data_df'de saklamamız gerekir.
        # Burada basit bir yaklaşım için data_df'yi önceden global olarak saklıyoruz.
        # Orijinal data_df tüm bilgileri içeriyor.

        # Bu nedenle en iyisi last_modified ve URL bilgilerini data_df üzerinden alacağız.
        # data_df'de her satır unique olduğundan month, content, size ve Downloaded'a dayalı
        # bir eşleşme yapacağız. (Ya da 'key' üzerinden eşleşmeyi düşünebilirsiniz)
        # En garantisi 'URL' kolonunu da bu tabloda tutmaktır. Şu an URL kolonunu saklamıyoruz
        # tabloda ama data_df'de var. data_df zaten class'ta var.

        # Bu yüzden download_selected'ta index tabanlı bir yaklaşım yerine,
        # populate_table'dan sonra tree'ye eklenen satırların item_id'sini data_df satır indexine
        # mapping yapabiliriz. Bunun için bir dictionary tutabiliriz.
        # Ancak şu anki yapıda bunu basitçe çözeceğiz:
        # populate_table'a data_df.iterrows yaparken item_id -> df index map'ini kaydedebiliriz.
        # Bunun için aşağıda basit bir çözüm implement edeceğiz.

        checked_data = []
        for item in checked_items:
            values = self.tree.item(item, "values")
            # values: ["", month, content, size, Downloaded]
            # Buna karşılık data_df'de bu satırı bulalım:
            month_val = values[1]
            content_val = values[2]
            size_val = values[3]
            downloaded_val = values[4]

            # data_df'den eşleşen satırı bul:
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
                self.log_to_console(f"{discogs_dir} klasörü bulunamadı!")
                messagebox.showerror("Hata", f"{discogs_dir} klasörü bulunamadı!")
                return
            if platform.system() == "Windows":
                os.startfile(discogs_dir)
            elif platform.system() == "Darwin":
                subprocess.run(["open", str(discogs_dir)])
            else:
                subprocess.run(["xdg-open", str(discogs_dir)])
        except Exception as e:
            self.log_to_console(f"Klasör açılamadı: {e}")
            messagebox.showerror("Hata", f"Klasör açılamadı: {e}")

    def export_to_csv(self):
        save_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV Dosyaları", "*.csv")])
        if save_path:
            self.data_df.to_csv(save_path, index=False)
            self.log_to_console(f"Veri {save_path} olarak dışa aktarıldı.")

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
                # Sadece gerekli kolonlar + last_modified ve URL dahili kullanım için saklanabilir
                # Ancak tabloya gösterilmeyecekler
                # Indirme işlemlerinde kullanmak için last_modified ve URL kalsın, ama görüntüleme yaparken kullanmayacağız.
                # Son aşamada tabloya aktarmadan önce Downloaded ekliyoruz.
                data_df = self.mark_downloaded_files(data_df)

                # Görüntülenecek kolonlar: month, content, size, Downloaded
                # Yine de data_df içinde last_modified, URL vs. kalsın indirme için.
                # Ama populate_table'a verirken direk bu data_df'yi verebiliriz.
                # populate_table sadece belirtilen kolonları okuyor.

                self.data_df = data_df
                self.populate_table(data_df[["month", "content", "size", "Downloaded"]])
                self.save_to_file()
                self.log_to_console(
                    "Scraping tamamlandı. İndirmek istediğiniz veri setini seçiniz ve İndir butonuna tıklayınız.",
                    "INFO")
            else:
                self.log_to_console("Veri bulunamadı.")
        except Exception as e:
            self.log_to_console(f"Hata: {e}", "ERROR")
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
