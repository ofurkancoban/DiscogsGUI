import os
import time
import math
import gzip
import requests
import platform
import subprocess
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from threading import Thread, Lock

import ttkbootstrap as ttk
from ttkbootstrap import Style
from ttkbootstrap.constants import *
from ttkbootstrap.dialogs import Messagebox
from tkinter import messagebox
from tkinter.scrolledtext import ScrolledText

###############################################################################
#                           XML → DataFrame ve Dönüşüm
###############################################################################
def xml_to_df(xml_path):
    data_dict = {}
    current_path = []

    for event, elem in ET.iterparse(xml_path, events=("start", "end")):
        if event == "start":
            current_path.append(elem.tag)
            for attr, value in elem.attrib.items():
                if len(current_path) > 1:
                    tag_name = f"{'_'.join(current_path[-2:])}_{attr}"
                else:
                    tag_name = f"{elem.tag}_{attr}"
                if tag_name not in data_dict:
                    data_dict[tag_name] = []
                data_dict[tag_name].append(value)
        elif event == "end":
            if elem.text and not elem.text.isspace():
                if len(current_path) > 1:
                    tag_name = '_'.join(current_path[-2:])
                else:
                    tag_name = current_path[-1]
                if tag_name not in data_dict:
                    data_dict[tag_name] = []
                data_dict[tag_name].append(elem.text.strip())
            current_path.pop()
            elem.clear()

    max_len = max(len(lst) for lst in data_dict.values())
    for key, value in data_dict.items():
        data_dict[key] = value + [None]*(max_len - len(value))
    return pd.DataFrame(data_dict)

def convert_extracted_file_to_csv(extracted_file_path, output_csv_path):
    if not extracted_file_path.exists():
        return False
    df = xml_to_df(extracted_file_path)
    df.to_csv(output_csv_path, index=False)
    return True

###############################################################################
#                              CHUNK Fonksiyonu
###############################################################################
def chunk_xml_file(xml_file_path, logger_func, chunk_size_mb=200, records_per_file=10000):
    """
    Eğer xml_file_path boyutu chunk_size_mb MB'tan büyükse:
      - discogs_20241201_labels.xml  →  discogs_20241201_labels_chunks/chunk_0.xml ...
      - Orijinal discogs_20241201_labels.xml silinir
    Aksi halde chunk yapılmaz, orijinal dosya kalır.
    """

    start_time = time.time()
    file_size_bytes = xml_file_path.stat().st_size
    file_size_mb = file_size_bytes / (1024 * 1024)

    logger_func(f"File {xml_file_path.name} size = {file_size_mb:.0f} MB", "INFO")

    # 1) 200 MB'tan küçükse chunk yapılmaz, orijinal dosya korunur
    if file_size_mb < chunk_size_mb:
        logger_func(f"No chunking needed (< {chunk_size_mb}MB). Keeping {xml_file_path.name}.", "INFO")
        return

    # 2) Büyük dosya (>= 200 MB) ise chunk'leme işlemi
    logger_func(f"File size={file_size_mb:.0f}MB >= {chunk_size_mb}MB. Starting chunk...", "INFO")

    album_counter = 0
    record_counter = 0
    file_counter = 0
    output_file = None

    # Örnek: discogs_20241201_labels.xml
    base_stem = xml_file_path.stem  # discogs_20241201_labels
    parent_dir = xml_file_path.parent
    chunk_folder = parent_dir / f"{base_stem}_chunks"  # discogs_20241201_labels_chunks
    chunk_folder.mkdir(parents=True, exist_ok=True)

    with open(xml_file_path, "r", encoding="utf-8") as data_file:
        for line in data_file:
            # Her <release id=...> satırında sayacı artır
            if "<release id=" in line:
                album_counter += 1
                # records_per_file’e ulaşınca yeni chunk dosyası
                if record_counter % records_per_file == 0:
                    if output_file:
                        output_file.write("</root>\n")
                        output_file.close()
                        file_counter += 1
                        elapsed_time = time.time() - start_time
                        logger_func(
                            f"Created chunk_{file_counter}.xml - total releases so far: {album_counter}, "
                            f"elapsed: {elapsed_time/60:.0f} min",
                            "INFO"
                        )
                    # Yeni dosya ismi
                    chunk_name = f"chunk_{file_counter}.xml"
                    chunk_path = chunk_folder / chunk_name
                    output_file = open(chunk_path, "w", encoding="utf-8")
                    output_file.write("<root>\n")

                record_counter += 1

            if output_file:
                output_file.write(line)

    # Son chunk'ı kapat
    if output_file:
        output_file.write("</root>\n")
        output_file.close()
        file_counter += 1

        logger_func(f"File Created: chunk_{file_counter}.xml", "INFO")
        logger_func(f"Total releases processed: {album_counter}", "INFO")
        elapsed_time = time.time() - start_time
        logger_func(f"Elapsed Time: {elapsed_time/60:.0f} minutes", "INFO")
        logger_func(f"Number of chunk files: {file_counter}", "INFO")

    # Chunk bitti → orijinali siliyoruz
    try:
        xml_file_path.unlink()
        logger_func(f"Deleted original file: {xml_file_path.name}", "INFO")
    except Exception as e:
        logger_func(f"Error deleting {xml_file_path.name}: {e}", "ERROR")



###############################################################################
#                           S3 Yardımcıları
###############################################################################
def human_readable_size(num_bytes):
    if num_bytes < 1024:
        return f"{num_bytes} B"
    elif num_bytes < 1024**2:
        return f"{num_bytes // 1024} KB"
    elif num_bytes < 1024**3:
        return f"{num_bytes // (1024**2)} MB"
    else:
        return f"{num_bytes // (1024**3)} GB"

def list_directories_from_s3(base_url="https://discogs-data-dumps.s3.us-west-2.amazonaws.com/", prefix="data/"):
    import xml.etree.ElementTree as ET
    url = base_url + "?prefix=" + prefix + "&delimiter=/"
    r = requests.get(url)
    r.raise_for_status()
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    root = ET.fromstring(r.text)
    dirs = []
    for cp in root.findall(ns+'CommonPrefixes'):
        p = cp.find(ns+'Prefix').text
        dirs.append(p)
    return dirs

def list_files_in_directory(base_url, directory_prefix):
    import xml.etree.ElementTree as ET
    url = base_url + "?prefix=" + directory_prefix
    r = requests.get(url)
    r.raise_for_status()
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    root = ET.fromstring(r.text)
    data = []
    for content in root.findall(ns+'Contents'):
        key = content.find(ns+'Key').text
        size_str = content.find(ns+'Size').text
        last_modified = content.find(ns+'LastModified').text

        try:
            size_bytes = int(size_str)
            size_hr = human_readable_size(size_bytes)
        except:
            size_hr = "0 B"

        ctype = "unknown"
        lname = key.lower()
        if "checksum" in lname:
            ctype = "checksum"
        elif "artist" in lname:
            ctype = "artists"
        elif "master" in lname:
            ctype = "masters"
        elif "label" in lname:
            ctype = "labels"
        elif "release" in lname:
            ctype = "releases"

        data.append({
            "last_modified": last_modified,
            "size": size_hr,
            "key": key,
            "content": ctype,
            "URL": base_url + key
        })
    return pd.DataFrame(data)

###############################################################################
#                           CollapsingFrame (UI)
###############################################################################
from ttkbootstrap import Style
from ttkbootstrap.constants import *
PATH = Path(__file__).parent / 'assets'

class CollapsingFrame(ttk.Frame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.columnconfigure(0, weight=1)
        self.cumulative_rows = 0
        self.images = [
            ttk.PhotoImage(file=PATH/'icons8-double-up-30.png'),
            ttk.PhotoImage(file=PATH/'icons8-double-right-30.png')
        ]

    def add(self, child, title="", bootstyle=PRIMARY, **kwargs):
        if child.winfo_class() != 'TFrame':
            return
        frm = ttk.Frame(self, bootstyle=bootstyle)
        frm.grid(row=self.cumulative_rows, column=0, sticky=EW)
        header = ttk.Label(master=frm, text=title, bootstyle=(bootstyle, INVERSE))
        if kwargs.get('textvariable'):
            header.configure(textvariable=kwargs.get('textvariable'))
        header.pack(side=LEFT, fill=BOTH, padx=10)
        def _func(c=child): return self._toggle_open_close(c)
        btn = ttk.Button(master=frm, image=self.images[0], bootstyle=bootstyle, command=_func)
        btn.pack(side=RIGHT)
        child.btn = btn

        child.grid(row=self.cumulative_rows + 1, column=0, sticky=NSEW)
        self.cumulative_rows += 2

    def _toggle_open_close(self, child):
        if child.winfo_viewable():
            child.grid_remove()
            child.btn.configure(image=self.images[1])
        else:
            child.grid()
            child.btn.configure(image=self.images[0])

###############################################################################
#                           Ana Uygulama UI
###############################################################################
class DiscogsDownloaderUI(ttk.Frame):
    def __init__(self, master, data_df, **kwargs):
        super().__init__(master, **kwargs)
        self.pack(fill=BOTH, expand=YES)
        self.data_df = data_df
        self.stop_flag = False

        self.check_vars = {}
        self.checkbuttons = {}

        # Banner
        self.banner_image = ttk.PhotoImage(file=str(PATH / "logo.png"))
        banner = ttk.Label(self, image=self.banner_image)
        banner.pack(side="top", fill="x")

        # Icon dictionary
        image_files = {
            'settings': 'icons8-settings-30.png',
            'stop-download-light': 'icons8-cancel-30.png',
            'download': 'icons8-download-30.png',
            'refresh': 'icons8-refresh-30.png',
            'stop-light': 'icons8-cancel-30.png',
            'opened-folder': 'icons8-folder-30.png',
            'logo': 'logo.png',
            'fetch': 'icons8-data-transfer-30.png',
            'banner': 'banner.png',
            'delete': 'icons8-trash-30.png',
            'extract': 'icons8-open-archive-30.png',
            'convert': 'icons8-export-csv-30.png'
        }
        self.photoimages = []
        for key, val in image_files.items():
            _file = PATH / val
            if _file.exists():
                self.photoimages.append(ttk.PhotoImage(name=key, file=str(_file)))

        # Üst düğme çubuğu
        buttonbar = ttk.Frame(self, style='primary.TFrame')
        buttonbar.pack(fill=X, pady=1, side=TOP)
        _value = 'Log: Ready.'
        self.setvar('scroll-message', _value)

        # Fetch
        btn_fetch = ttk.Button(buttonbar, text='Fetch Data', image='fetch', compound=TOP, command=self.start_scraping)
        btn_fetch.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # Download
        btn_dl = ttk.Button(buttonbar, text='Download', image='download', compound=TOP, command=self.download_selected)
        btn_dl.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # Stop
        btn_stop = ttk.Button(buttonbar, text='Stop', image='stop-light', compound=TOP, command=self.stop_download)
        btn_stop.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # Refresh
        btn_refresh = ttk.Button(buttonbar, text='Refresh', image='refresh', compound=TOP, command=self.refresh_data)
        btn_refresh.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # Delete
        btn_delete = ttk.Button(buttonbar, text='Delete', image='delete', compound=TOP, command=self.delete_selected)
        btn_delete.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # Extract
        btn_extract = ttk.Button(buttonbar, text='Extract', image='extract', compound=TOP, command=self.extract_selected)
        btn_extract.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # Convert
        btn_convert = ttk.Button(buttonbar, text='Convert', image='convert', compound=TOP, command=self.convert_selected)
        btn_convert.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # Settings
        btn_settings = ttk.Button(buttonbar, text='Settings', image='settings', compound=TOP,
                                  command=lambda: Messagebox.ok(message='Open Settings'))
        btn_settings.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        #######################################################################
        #                Sol Panel (Data Summary, Status)
        #######################################################################
        left_panel = ttk.Frame(self, style='bg.TFrame', width=250)
        left_panel.pack(side=LEFT, fill=Y)
        left_panel.pack_propagate(False)

        ds_cf = CollapsingFrame(left_panel)
        ds_cf.pack(fill=X, pady=1)

        ds_frm = ttk.Frame(ds_cf, padding=5)
        ds_frm.columnconfigure(1, weight=1)
        ds_cf.add(child=ds_frm, title='Data Summary', bootstyle=SECONDARY)

        lbl1 = ttk.Label(ds_frm, text='Download Folder:')
        lbl1.grid(row=0, column=0, sticky=W, pady=2)

        lbl2 = ttk.Label(ds_frm, textvariable='downloadfolder')
        lbl2.grid(row=1, column=0, sticky=W, padx=0, pady=2)
        downloads_dir = Path.home() / "Downloads" / "Discogs"
        self.setvar('downloadfolder', f" → {str(downloads_dir)}")

        lbl3 = ttk.Label(ds_frm, text='Size of Downloaded Files:')
        lbl3.grid(row=2, column=0, sticky=W, pady=2)

        self.downloaded_size_var = ttk.StringVar(value="Calculating...")
        lbl4 = ttk.Label(ds_frm, textvariable=self.downloaded_size_var)
        lbl4.grid(row=3, column=0, sticky=W, padx=0, pady=2)

        sep = ttk.Separator(ds_frm, bootstyle=SECONDARY)
        sep.grid(row=6, column=0, columnspan=2, pady=10, sticky=EW)

        btn_open_folder = ttk.Button(ds_frm, text='Open Folder', command=self.open_discogs_folder,
                                     bootstyle=LINK, image='opened-folder', compound=LEFT)
        btn_open_folder.grid(row=5, column=0, columnspan=2, sticky=W)

        status_cf = CollapsingFrame(left_panel)
        status_cf.pack(fill=BOTH, pady=1)

        status_frm = ttk.Frame(status_cf, padding=10)
        status_frm.columnconfigure(1, weight=1)
        ds_cf.add(child=status_frm, title='Status', bootstyle=SECONDARY)

        lbl5 = ttk.Label(status_frm, textvariable='prog-message', font='Arial 10 bold')
        lbl5.grid(row=0, column=0, columnspan=2, sticky=W)
        self.setvar('prog-message', 'Idle...')

        pb = ttk.Progressbar(status_frm, length=200, mode="determinate", bootstyle=SUCCESS)
        pb.grid(row=1, column=0, columnspan=2, sticky=EW, pady=(10, 5))
        self.pb = pb
        self.pb["value"] = 0

        lbl6 = ttk.Label(status_frm, textvariable='prog-time-started')
        lbl6.grid(row=2, column=0, columnspan=2, sticky=EW, pady=2)
        self.setvar('prog-time-started', 'Not started')

        lbl7 = ttk.Label(status_frm, textvariable='prog-speed')
        lbl7.grid(row=3, column=0, columnspan=2, sticky=EW, pady=2)
        self.setvar('prog-speed', 'Speed: 0.00 MB/s')

        lbl8 = ttk.Label(status_frm, textvariable='prog-time-elapsed')
        lbl8.grid(row=4, column=0, columnspan=2, sticky=EW, pady=2)
        self.setvar('prog-time-elapsed', 'Elapsed: 0 sec')

        lbl9 = ttk.Label(status_frm, textvariable='prog-time-left')
        lbl9.grid(row=5, column=0, columnspan=2, sticky=EW, pady=2)
        self.setvar('prog-time-left', 'Left: 0 sec')

        sep2 = ttk.Separator(status_frm, bootstyle=SECONDARY)
        sep2.grid(row=6, column=0, columnspan=2, pady=10, sticky=EW)

        btn_stop_status = ttk.Button(status_frm, text='Stop', command=self.stop_download,
                                     bootstyle=LINK, image='stop-download-light', compound=LEFT)
        btn_stop_status.grid(row=7, column=0, columnspan=2, sticky=W)

        sep3 = ttk.Separator(status_frm, bootstyle=SECONDARY)
        sep3.grid(row=8, column=0, columnspan=2, pady=10, sticky=EW)

        lbl_curfile = ttk.Label(status_frm, textvariable='current-file-msg')
        lbl_curfile.grid(row=9, column=0, columnspan=2, pady=2, sticky=EW)
        self.setvar('current-file-msg', 'No file downloading')

        lbl_logo = ttk.Label(left_panel, image='logo', style='bg.TLabel')
        lbl_logo.pack(side='right')

        #######################################################################
        #            Sağ Panel (Treeview + Console)
        #######################################################################
        right_panel = ttk.Frame(self, padding=(2, 0))
        right_panel.pack(side=RIGHT, fill=BOTH, expand=NO)

        self.style = ttk.Style()
        self.style.configure("Treeview.Heading", padding=(0, 11), font=("Arial", 13))
        right_panel.columnconfigure(0, weight=1)
        right_panel.rowconfigure(0, weight=1)
        right_panel.rowconfigure(1, weight=1)

        tv = ttk.Treeview(right_panel, show='headings', height=16, style="Treeview")
        tv.configure(columns=(" ", "month", "content", "size", "Downloaded", "Extracted", "Processed"))
        tv.column(" ", width=1, anchor=CENTER)
        tv.column("month", width=15, anchor=CENTER)
        tv.column("content", width=20, anchor=CENTER)
        tv.column("size", width=25, anchor=CENTER)
        tv.column("Downloaded", width=35, anchor=CENTER)
        tv.column("Extracted", width=35, anchor=CENTER)
        tv.column("Processed", width=35, anchor=CENTER)

        vsb = ttk.Scrollbar(right_panel, orient="vertical", command=tv.yview)
        tv.configure(yscrollcommand=vsb.set)
        tv.grid(row=0, column=0, sticky='nsew', pady=1)
        vsb.grid(row=0, column=1, sticky='ns')

        def _scrollbar_set(first, last):
            vsb.set(first, last)
            self.position_checkbuttons()

        tv.configure(yscrollcommand=_scrollbar_set)

        scroll_cf = CollapsingFrame(right_panel)
        scroll_cf.grid(row=1, column=0, columnspan=2, sticky='nsew')

        output_container = ttk.Frame(scroll_cf, padding=0)
        _value = 'Log: Ready.'
        self.setvar('scroll-message', _value)

        console_frame = ttk.Frame(output_container)
        console_frame.pack(fill=BOTH, expand=NO)
        console_frame.columnconfigure(0, weight=1)
        console_frame.rowconfigure(0, weight=1)

        st = ttk.Text(console_frame, wrap='word', state='disabled', height=15)
        st.grid(row=0, column=0, sticky='nsew')
        self.console_text = st

        console_vsb = ttk.Scrollbar(console_frame, orient='vertical', command=st.yview)
        console_vsb.grid(row=0, column=1, sticky='ns')
        st.configure(yscrollcommand=console_vsb.set)

        scroll_cf.add(output_container, textvariable='scroll-message')

        self.tree = tv
        self.tree.bind("<Configure>", lambda e: self.position_checkbuttons())
        self.tree.bind("<Motion>", lambda e: self.position_checkbuttons())
        self.tree.bind("<ButtonRelease-1>", lambda e: self.position_checkbuttons())
        self.tree.bind("<<TreeviewSelect>>", lambda e: self.position_checkbuttons())
        right_panel.bind("<Configure>", lambda e: self.position_checkbuttons())

        self.log_to_console("Welcome to the Discogs Data Downloader", "INFO")
        self.log_to_console("The application is fetching data automatically, please wait...", "INFO")
        self.after(100, self.start_scraping)
        self.update_downloaded_size()

    ###########################################################################
    #        "ilgiliTarih" = last_modified.strftime("%Y-%m") FOLDER LOGIC
    ###########################################################################
    def get_subfolder_for_date(self, last_modified):
        date_str = last_modified.strftime("%Y-%m")
        return Path.home() / "Downloads" / "Discogs" / date_str

    def refresh_data(self):
        if self.data_df.empty:
            self.log_to_console("No data to refresh. Fetch data first.", "WARNING")
        else:
            self.populate_table(self.data_df)
            self.log_to_console("Data refreshed.", "INFO")

    def populate_table(self, data_df):
        for cb in self.checkbuttons.values():
            cb.destroy()
        self.check_vars.clear()
        self.checkbuttons.clear()

        for row in self.tree.get_children():
            self.tree.delete(row)

        if "month" not in data_df.columns:
            self.log_to_console("No 'month' column found in data.", "WARNING")
            return

        unique_months = data_df["month"].unique()
        color_map = {}
        for i, month in enumerate(unique_months):
            color_map[month] = "month1" if i % 2 == 0 else "month2"

        self.tree.tag_configure("month1", background="#343a40", foreground="#f8f9fa")
        self.tree.tag_configure("month2", background="#495057", foreground="#f8f9fa")

        for i, (_, row) in enumerate(data_df.iterrows()):
            downloaded_status = row.get("Downloaded", "✖")
            extracted_status = row.get("Extracted", "✖")
            processed_status = row.get("Processed", "✖")
            tag = color_map.get(row["month"], "month1")

            values = [
                "",
                row["month"],
                row["content"],
                row["size"],
                downloaded_status,
                extracted_status,
                processed_status
            ]
            item_id = self.tree.insert("", "end", values=values, tags=(tag,))
            if item_id:
                var = ttk.IntVar(value=0)
                cb = ttk.Checkbutton(self.tree, variable=var)
                self.check_vars[item_id] = var
                self.checkbuttons[item_id] = cb

        self.position_checkbuttons()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.setvar('lastupdate', f"→ {now}")
        self.update_downloaded_size()

    def position_checkbuttons(self):
        self.update_idletasks()
        if not self.checkbuttons:
            return
        first_visible = self.tree.winfo_rooty()
        last_visible = first_visible + self.tree.winfo_height()
        for item_id, cb in list(self.checkbuttons.items()):
            bbox = self.tree.bbox(item_id, column=0)
            if not bbox:
                cb.place_forget()
                continue
            x, y, width, height = bbox
            cb_x = x + (width - cb.winfo_reqwidth()) // 2
            cb_y = y + (height - cb.winfo_reqheight()) // 2
            item_top = self.tree.winfo_rooty() + y
            item_bottom = item_top + height
            if first_visible <= item_bottom and item_top <= last_visible:
                cb.place(in_=self.tree, x=cb_x, y=cb_y + 1, width=height - 2, height=height - 2)
            else:
                cb.place_forget()

    def delete_selected(self):
        checked_items = [item for item, var in self.check_vars.items() if var.get() == 1]
        if not checked_items:
            messagebox.showwarning("Warning", "No file selected!")
            return

        confirm = messagebox.askyesno("Confirm Deletion",
                                      "Are you sure you want to delete the selected files?")
        if not confirm:
            return

        deleted_files = []
        failed_files = []

        for item in checked_items:
            values = self.tree.item(item, "values")
            month_val = values[1]
            content_val = values[2]
            size_val = values[3]
            downloaded_val = values[4]
            extracted_val = values[5]
            processed_val = values[6]

            row_data = self.data_df[
                (self.data_df["month"] == month_val) &
                (self.data_df["content"] == content_val) &
                (self.data_df["size"] == size_val) &
                (self.data_df["Downloaded"] == downloaded_val) &
                (self.data_df["Extracted"] == extracted_val) &
                (self.data_df["Processed"] == processed_val)
            ]
            if not row_data.empty:
                url = row_data["URL"].values[0]
                last_modified = row_data["last_modified"].values[0]
                last_modified = pd.to_datetime(last_modified)
                folder_path = self.get_subfolder_for_date(last_modified)
                filename = os.path.basename(url)
                file_path = folder_path / filename
                csv_path = file_path.with_suffix('.csv')
                xml_path = file_path.with_suffix('')

                try:
                    if file_path.exists():
                        file_path.unlink()
                        deleted_files.append(file_path)
                    if csv_path.exists():
                        csv_path.unlink()
                        deleted_files.append(csv_path)
                    if xml_path.exists() and xml_path.suffix == "":
                        if xml_path.is_file():
                            xml_path.unlink()
                            deleted_files.append(xml_path)

                    self.data_df.loc[self.data_df["URL"] == url, "Downloaded"] = "✖"
                    self.data_df.loc[self.data_df["URL"] == url, "Extracted"] = "✖"
                    self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✖"

                except Exception as e:
                    self.log_to_console(f"Error deleting files for {file_path}: {e}", "ERROR")
                    failed_files.append(file_path)

        if deleted_files:
            self.log_to_console(f"Deleted files: {', '.join(map(str, deleted_files))}", "INFO")
        if failed_files:
            self.log_to_console(f"Failed to delete: {', '.join(map(str, failed_files))}", "WARNING")

        self.populate_table(self.data_df)
        self.update_downloaded_size()

    def log_to_console(self, message, message_type="INFO"):
        self.console_text.config(state='normal')
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"→ [{timestamp}] [{message_type.upper()}]: {message}\n"

        line_start = self.console_text.index("end-1c")
        self.console_text.insert('end', formatted_message)

        line_number_str = line_start.split('.')[0]
        try:
            line_number = int(line_number_str)
        except:
            line_number = 0

        if line_number % 2 == 0:
            line_start_index = f"{line_number}.0"
            line_end_index = f"{line_number}.end"
            self.console_text.tag_add("blue_line", line_start_index, line_end_index)
            self.console_text.tag_config("blue_line", foreground="blue")

        self.console_text.config(state='disabled')
        self.console_text.see('end')

        message_content = message.strip()
        max_header_length = 80
        if len(message_content) > max_header_length:
            message_content = message_content[:max_header_length] + '...'
        self.setvar('scroll-message', f"Log: {message_content}")

    def mark_downloaded_files(self, data_df):
        for col in ["Downloaded", "Extracted", "Processed"]:
            if col not in data_df.columns:
                data_df[col] = "✖"
        for idx, row in data_df.iterrows():
            url = row["URL"]
            filename = os.path.basename(url)
            last_modified = row["last_modified"]
            if pd.isnull(last_modified):
                data_df.at[idx, "Downloaded"] = "✖"
                data_df.at[idx, "Extracted"] = "✖"
                continue
            last_modified = pd.to_datetime(last_modified)
            folder_path = self.get_subfolder_for_date(last_modified)
            file_path = folder_path / filename
            if file_path.exists():
                data_df.at[idx, "Downloaded"] = "✔"
            else:
                data_df.at[idx, "Downloaded"] = "✖"
                data_df.at[idx, "Extracted"] = "✖"
                data_df.at[idx, "Processed"] = "✖"
        return data_df

    def stop_download(self):
        self.stop_flag = True
        self.log_to_console("Operation Stopped. Cleaning up...", "WARNING")

    def get_folder_size(self, folder_path):
        total_size = 0
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                fp = os.path.join(root, file)
                if os.path.exists(fp):
                    total_size += os.path.getsize(fp)
        return total_size

    def update_downloaded_size(self):
        downloads_dir = Path.home() / "Downloads" / "Discogs"
        size_in_bytes = 0
        if downloads_dir.exists():
            size_in_bytes = self.get_folder_size(downloads_dir)
        one_gb = 1024**3
        if size_in_bytes >= one_gb:
            size_in_gb = size_in_bytes // one_gb
            self.downloaded_size_var.set(f"→ {size_in_gb} GB")
        else:
            size_in_mb = size_in_bytes // (1024**2)
            self.downloaded_size_var.set(f"→ {size_in_mb} MB")

    def open_discogs_folder(self):
        downloads_dir = Path.home() / "Downloads" / "Discogs"
        if not downloads_dir.exists():
            self.log_to_console(f"{downloads_dir} folder not found!", "ERROR")
            messagebox.showerror("Error", f"{downloads_dir} folder not found!")
            return
        try:
            if platform.system() == "Windows":
                os.startfile(downloads_dir)
            elif platform.system() == "Darwin":
                subprocess.run(["open", str(downloads_dir)])
            else:
                subprocess.run(["xdg-open", str(downloads_dir)])
        except Exception as e:
            self.log_to_console(f"Cannot open folder: {e}", "ERROR")
            messagebox.showerror("Error", f"Cannot open folder: {e}")

    def save_to_file(self):
        try:
            downloads_dir = Path.home() / "Downloads" / "Discogs"
            downloads_dir.mkdir(parents=True, exist_ok=True)
            file_path = downloads_dir / "discogs_data.csv"
            self.data_df.to_csv(file_path, sep="\t", index=False)
            self.log_to_console(f"Data saved as {file_path}.")
        except Exception as e:
            self.log_to_console(f"Error: {e}", "ERROR")

    def start_scraping(self):
        self.log_to_console("Fetching data, please wait...", "INFO")
        Thread(target=self._scrape_data_s3, daemon=True).start()

    def _scrape_data_s3(self):
        try:
            base_url = "https://discogs-data-dumps.s3.us-west-2.amazonaws.com/"
            prefix = "data/"
            self.log_to_console("Listing directories from S3...", "INFO")
            dirs = list_directories_from_s3(base_url, prefix)
            if not dirs:
                self.log_to_console("No directories found at all.", "WARNING")
                return

            dirs.sort()
            latest_dir = dirs[-1]
            self.log_to_console(f"Latest directory selected: {latest_dir}", "INFO")

            data_df = list_files_in_directory(base_url, latest_dir)
            if not data_df.empty:
                data_df["last_modified"] = pd.to_datetime(data_df["last_modified"])
                data_df["month"] = data_df["last_modified"].dt.to_period("M").astype(str)
                data_df = data_df[data_df["content"] != "checksum"]
                content_order = {"artists": 1, "labels": 2, "masters": 3, "releases": 4}
                data_df["content_order"] = data_df["content"].map(content_order)
                data_df = data_df.sort_values(by=["month", "content_order"], ascending=[False, True])
                data_df.drop(columns=["content_order"], inplace=True)
                data_df = self.mark_downloaded_files(data_df)
                self.data_df = data_df
                self.populate_table(data_df)
                self.save_to_file()
                self.log_to_console("Scraping completed. Data saved automatically.", "INFO")
            else:
                self.log_to_console("No data found in the latest directory.", "WARNING")
        except Exception as e:
            self.log_to_console(f"Error: {e}", "ERROR")

    def download_selected(self):
        checked_items = [item for item, var in self.check_vars.items() if var.get() == 1]
        if not checked_items:
            messagebox.showwarning("Warning", "No file selected!")
            return

        for item in checked_items:
            values = self.tree.item(item, "values")
            month_val = values[1]
            content_val = values[2]
            size_val = values[3]
            downloaded_val = values[4]
            extracted_val = values[5]
            processed_val = values[6]

            row_data = self.data_df[
                (self.data_df["month"] == month_val) &
                (self.data_df["content"] == content_val) &
                (self.data_df["size"] == size_val) &
                (self.data_df["Downloaded"] == downloaded_val) &
                (self.data_df["Extracted"] == extracted_val) &
                (self.data_df["Processed"] == processed_val)
            ]
            if not row_data.empty:
                url = row_data["URL"].values[0]
                last_modified = pd.to_datetime(row_data["last_modified"].values[0])
                filename = os.path.basename(url)
                self.start_download(url, filename, last_modified)

    def start_download(self, url, filename, last_modified):
        self.stop_flag = False
        Thread(target=self.download_file, args=(url, filename, last_modified), daemon=True).start()

    def download_file(self, url, filename, last_modified):
        """
        .gz dosyasını ~/Downloads/Discogs/{YYYY-MM} altına indirir.
        """
        try:
            self.setvar('prog-message', 'Preparing download...')
            head = requests.head(url)
            head.raise_for_status()
            total_size = int(head.headers.get('Content-Length', 0))
            accept_ranges = head.headers.get('Accept-Ranges', 'none')

            folder_path = self.get_subfolder_for_date(last_modified)
            folder_path.mkdir(parents=True, exist_ok=True)
            file_path = folder_path / filename

            if total_size > 0 and accept_ranges.lower() == 'bytes':
                success = self.parallel_download(url, file_path, total_size)
                if not success:
                    if self.stop_flag:
                        self.log_to_console("Operation Stopped", "WARNING")
                        if file_path.exists():
                            file_path.unlink()
                            self.log_to_console(f"Incomplete file {file_path} deleted.", "WARNING")
                        self.setvar('prog-message', 'Idle...')
                        return
                    else:
                        self.log_to_console("Parallel download failed, falling back to single-thread.", "WARNING")
                        self.single_thread_download(url, file_path)
                else:
                    self.log_to_console(f"{file_path.name} successfully downloaded: {file_path}", "INFO")
                    self.data_df.loc[self.data_df["URL"] == url, "Downloaded"] = "✔"
                    self.data_df.loc[self.data_df["URL"] == url, "Extracted"] = "✖"
                    self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✖"
                    self.populate_table(self.data_df)
                    self.update_downloaded_size()
                    self.setvar('prog-message', 'Idle...')
            else:
                self.single_thread_download(url, file_path)

        except Exception as e:
            self.log_to_console(f"Error: {e}", "ERROR")
            if 'file_path' in locals() and file_path.exists():
                file_path.unlink()
                self.log_to_console(f"Incomplete file {file_path} deleted.", "WARNING")

    def parallel_download(self, url, file_path, total_size):
        num_threads = 4
        part_size = total_size // num_threads
        partial_paths = []
        thread_progress = [0]*num_threads
        lock = Lock()

        def download_segment(idx, start, end):
            headers = {"Range": f"bytes={start}-{end}"}
            r = requests.get(url, headers=headers, stream=True)
            part_file = file_path.with_name(file_path.name + f".part{idx}")
            partial_paths.append(part_file)
            with open(part_file, "wb") as f:
                for chunk in r.iter_content(1024*64):
                    if self.stop_flag:
                        return
                    if chunk:
                        f.write(chunk)
                        with lock:
                            thread_progress[idx] += len(chunk)

        start_time = datetime.now()
        self.setvar('prog-time-started', f'Started at: {start_time.strftime("%Y-%m-%d %H:%M:%S")}')
        threads=[]
        for i in range(num_threads):
            start = i*part_size
            end = (i+1)*part_size-1 if i<num_threads-1 else total_size-1
            t = Thread(target=download_segment, args=(i, start, end), daemon=True)
            threads.append(t)
            t.start()

        while any(t.is_alive() for t in threads):
            if self.stop_flag:
                for t in threads:
                    t.join()
                for p in partial_paths:
                    if p.exists():
                        p.unlink()
                return False
            time.sleep(0.5)
            downloaded_size = sum(thread_progress)
            elapsed = (datetime.now()-start_time).total_seconds()
            speed = (downloaded_size/elapsed)/(1024*1024) if elapsed>0 else 0.0
            self.setvar('prog-speed', f'Speed: {speed:.2f} MB/s')
            total_percentage = (downloaded_size/total_size)*100 if total_size>0 else 0
            self.setvar('prog-message', f'Downloading {file_path.name}: {total_percentage:.2f}%')
            self.pb["maximum"]=total_size
            self.pb["value"]=downloaded_size
            elapsed_minutes=int(elapsed)//60
            elapsed_seconds=int(elapsed)%60
            self.setvar('prog-time-elapsed',f'Elapsed: {elapsed_minutes} min {elapsed_seconds} sec')
            if downloaded_size>0:
                rate=downloaded_size/elapsed if elapsed>0 else 0
                left=int((total_size-downloaded_size)/rate) if rate>0 else 0
                left_minutes=left//60
                left_seconds=left%60
                self.setvar('prog-time-left',f'Left: {left_minutes} min {left_seconds} sec')

        for t in threads:
            t.join()
        if self.stop_flag:
            for p in partial_paths:
                if p.exists():
                    p.unlink()
            return False

        with open(file_path,"wb") as f_out:
            for i in range(num_threads):
                part_file=file_path.with_name(file_path.name+f".part{i}")
                if part_file.exists():
                    with open(part_file,"rb") as f_in:
                        f_out.write(f_in.read())
                    part_file.unlink()
        return True

    def single_thread_download(self, url, file_path):
        self.log_to_console("Partial downloads not supported. Using single-threaded download.", "INFO")
        response=requests.get(url,stream=True)
        total_size=int(response.headers.get('content-length',0))
        block_size=1024*64
        self.pb["value"]=0
        self.pb["maximum"]=total_size

        start_time=datetime.now()
        self.setvar('prog-time-started',f'Started at: {start_time.strftime("%Y-%m-%d %H:%M:%S")}')
        downloaded_size=0

        with open(file_path,"wb") as f:
            for data in response.iter_content(block_size):
                if self.stop_flag:
                    self.setvar('prog-message','Idle...')
                    self.log_to_console("Operation Stopped","WARNING")
                    f.close()
                    if file_path.exists():
                        file_path.unlink()
                        self.log_to_console(f"Incomplete file {file_path} deleted.","WARNING")
                    return
                f.write(data)
                downloaded_size+=len(data)
                self.pb["value"]=downloaded_size
                elapsed=(datetime.now()-start_time).total_seconds()
                speed=(downloaded_size/elapsed)/(1024*1024) if elapsed>0 else 0.0
                elapsed_minutes=int(elapsed)//60
                elapsed_seconds=int(elapsed)%60
                self.setvar('prog-speed',f'Speed: {speed:.2f} MB/s')
                self.setvar('prog-time-elapsed',f'Elapsed: {elapsed_minutes} min {elapsed_seconds} sec')

                if total_size>0 and downloaded_size>0:
                    percentage=(downloaded_size/total_size)*100
                    left=int((total_size-downloaded_size)/(downloaded_size/elapsed)) if downloaded_size>0 else 0
                    left_minutes=left//60
                    left_seconds=left%60
                    self.setvar('prog-time-left',f'Left: {left_minutes} min {left_seconds} sec')
                    self.setvar('prog-message',f'Downloading {file_path.name}: {percentage:.2f}%')

        self.setvar('prog-message','Idle...')
        self.log_to_console(f"{file_path.name} successfully downloaded: {file_path}","INFO")
        self.data_df.loc[self.data_df["URL"]==url,"Downloaded"]="✔"
        self.data_df.loc[self.data_df["URL"]==url,"Extracted"]="✖"
        self.data_df.loc[self.data_df["URL"]==url,"Processed"]="✖"
        self.populate_table(self.data_df)
        self.update_downloaded_size()

    def extract_selected(self):
        self.log_to_console("Extracting started...", "INFO")
        self.setvar('prog-message','Waiting 5 seconds before extraction...')
        time.sleep(5)
        self.setvar('prog-message','Extracting now...')

        extracted_files=[]
        failed_files=[]

        checked_items=[item for item,var in self.check_vars.items() if var.get()==1]
        if not checked_items:
            messagebox.showwarning("Warning","No file selected!")
            return

        for item in checked_items:
            values=self.tree.item(item,"values")
            month_val=values[1]
            content_val=values[2]
            size_val=values[3]
            downloaded_val=values[4]
            extracted_val=values[5]
            processed_val=values[6]

            if downloaded_val!="✔":
                self.log_to_console("File not downloaded, cannot extract.","WARNING")
                continue

            row_data=self.data_df[
                (self.data_df["month"]==month_val)&
                (self.data_df["content"]==content_val)&
                (self.data_df["size"]==size_val)&
                (self.data_df["Downloaded"]==downloaded_val)&
                (self.data_df["Extracted"]==extracted_val)&
                (self.data_df["Processed"]==processed_val)
            ]
            if not row_data.empty:
                url=row_data["URL"].values[0]
                last_modified=pd.to_datetime(row_data["last_modified"].values[0])
                folder_path=self.get_subfolder_for_date(last_modified)
                filename=os.path.basename(url)
                gz_path=folder_path/filename

                if gz_path.suffix.lower()==".gz":
                    success=self.extract_gz_file_with_progress(gz_path)
                    if success:
                        output_path=gz_path.with_suffix('')
                        extracted_files.append(output_path)
                        self.log_to_console(f"Extracted: {gz_path} → {output_path}","INFO")
                        self.data_df.loc[self.data_df["URL"]==url,"Extracted"]="✔"
                        self.data_df.loc[self.data_df["URL"]==url,"Processed"]="✖"

                        extracted_size_mb=output_path.stat().st_size/(1024*1024)
                        if extracted_size_mb>200:
                            self.log_to_console(f"File {output_path} is {extracted_size_mb:.0f} MB, chunking now...","INFO")
                            chunk_xml_file(output_path, self.log_to_console, chunk_size_mb=200, records_per_file=10000)
                        else:
                            self.log_to_console(f"No chunk needed. File size={extracted_size_mb:.0f}MB <=200MB","INFO")

                    else:
                        failed_files.append(gz_path)
                        self.log_to_console(f"Error or stopped extracting {gz_path}.","ERROR")
                else:
                    self.log_to_console(f"{gz_path} is not a .gz file.","WARNING")

        self.populate_table(self.data_df)
        if extracted_files:
            self.log_to_console(f"Extracted files: {', '.join(map(str,extracted_files))}","INFO")
        if failed_files:
            self.log_to_console(f"Failed to extract files: {', '.join(map(str,failed_files))}","WARNING")

    def extract_gz_file_with_progress(self,gz_path):
        self.setvar('prog-message','Extracting file...')
        xml_path=gz_path.with_suffix('')
        total_size=gz_path.stat().st_size
        self.pb["value"]=0
        self.pb["maximum"]=total_size

        start_time=datetime.now()
        self.setvar('prog-time-started',f'Started at: {start_time.strftime("%Y-%m-%d %H:%M:%S")}')
        extracted_size=0
        block_size=1024*64

        with gzip.open(gz_path,'rb') as f_in, open(xml_path,'wb') as f_out:
            while not self.stop_flag:
                chunk=f_in.read(block_size)
                if not chunk:
                    break
                f_out.write(chunk)
                extracted_size+=len(chunk)
                self.pb["value"]=extracted_size

                elapsed=(datetime.now()-start_time).total_seconds()
                speed=(extracted_size/elapsed)/(1024*1024) if elapsed>0 else 0.0
                self.setvar('prog-speed',f"Extract Speed: {speed:.2f} MB/s")

                if total_size>0:
                    percentage=(extracted_size/total_size)*100
                    self.setvar('prog-message',f"Extracting {gz_path.name}: {percentage:.2f}%")
                    elapsed_minutes=int(elapsed)//60
                    elapsed_seconds=int(elapsed)%60
                    self.setvar('prog-time-elapsed',f'Elapsed: {elapsed_minutes} min {elapsed_seconds} sec')
                    if extracted_size>0:
                        rate=extracted_size/elapsed if elapsed>0 else 0
                        left=int((total_size-extracted_size)/rate) if rate>0 else 0
                        left_minutes=left//60
                        left_seconds=left%60
                        self.setvar('prog-time-left',f'Left: {left_minutes} min {left_seconds} sec')
                else:
                    self.setvar('prog-message',"Extracting...")

        self.setvar('prog-message','Idle...')
        return not self.stop_flag

    def convert_selected(self):
        checked_items=[item for item,var in self.check_vars.items() if var.get()==1]
        if not checked_items:
            messagebox.showwarning("Warning","No file selected!")
            return

        for item in checked_items:
            values=self.tree.item(item,"values")
            month_val=values[1]
            content_val=values[2]
            size_val=values[3]
            downloaded_val=values[4]
            extracted_val=values[5]
            processed_val=values[6]

            if extracted_val!="✔":
                self.log_to_console("File not extracted, cannot convert.","WARNING")
                continue
            if processed_val=="✔":
                self.log_to_console("File already processed. Skipping...","INFO")
                continue

            row_data=self.data_df[
                (self.data_df["month"]==month_val)&
                (self.data_df["content"]==content_val)&
                (self.data_df["size"]==size_val)&
                (self.data_df["Downloaded"]==downloaded_val)&
                (self.data_df["Extracted"]==extracted_val)&
                (self.data_df["Processed"]==processed_val)
            ]
            if not row_data.empty:
                url=row_data["URL"].values[0]
                last_modified=pd.to_datetime(row_data["last_modified"].values[0])
                folder_path=self.get_subfolder_for_date(last_modified)
                filename=os.path.basename(url)
                gz_path=folder_path/filename
                xml_path=gz_path.with_suffix('')
                if xml_path.exists() and xml_path.suffix=="":
                    csv_file=xml_path.with_suffix('.csv')
                    self.log_to_console(f"Converting {xml_path} to {csv_file}...","INFO")
                    success=convert_extracted_file_to_csv(xml_path,csv_file)
                    if success:
                        self.log_to_console(f"Converted: {xml_path} → {csv_file}","INFO")
                        self.data_df.loc[self.data_df["URL"]==url,"Processed"]="✔"
                    else:
                        self.log_to_console(f"Failed to convert {xml_path}.","ERROR")
                else:
                    self.log_to_console(f"Extracted file {xml_path} not found or not .xml!","ERROR")

        self.populate_table(self.data_df)

###############################################################################
#                              main()
###############################################################################
def main():
    empty_df = pd.DataFrame(columns=[
        "month","content","size","last_modified","key","URL",
        "Downloaded","Extracted","Processed"
    ])
    empty_df["Downloaded"]="✖"
    empty_df["Extracted"]="✖"
    empty_df["Processed"]="✖"

    app=ttk.Window("Discogs Data Downloader",themename="darkly")
    primary_color=app.style.colors.primary
    style=ttk.Style()
    style.configure("Treeview.Heading", background=primary_color,
                    foreground="white", font=("Helvetica", 10, "bold"))

    DiscogsDownloaderUI(app, empty_df)
    window_width=600
    window_height=800
    screen_width=app.winfo_screenwidth()
    screen_height=app.winfo_screenheight()
    center_x=int((screen_width-window_width)/2)
    center_y=int((screen_height-window_height)/2)
    app.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
    app.resizable(False,False)
    app.mainloop()

if __name__=="__main__":
    main()
