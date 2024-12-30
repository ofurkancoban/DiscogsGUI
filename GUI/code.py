import time
import math
import gzip
import os
import shutil  # <-- we use shutil.rmtree() to remove chunk folders
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
import re

###############################################################################
#                          File-Type → XML Tag Mapping
###############################################################################
TAG_MAP = {
    "releases": "<release",
    "artists": "<artist",
    "masters": "<master",
    "labels": "<label"
}


###############################################################################
#                              XML → DataFrame logic
###############################################################################
def xml_to_df(xml_path: Path) -> pd.DataFrame:
    """Convert an XML file to a pandas DataFrame using iterative parsing."""
    data_dict = {}
    current_path = []

    for event, elem in ET.iterparse(str(xml_path), events=("start", "end")):
        if event == "start":
            current_path.append(elem.tag)
            # handle attributes
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

    # If data_dict is empty, return empty
    if not data_dict:
        return pd.DataFrame()

    # unify lengths
    max_len = max(len(lst) for lst in data_dict.values())
    for key, value in data_dict.items():
        if len(value) < max_len:
            data_dict[key] = value + [None] * (max_len - len(value))

    df = pd.DataFrame(data_dict)
    return df


###############################################################################
#                            Helper to convert a single XML file to CSV
###############################################################################
def convert_extracted_file_to_csv(extracted_file_path: Path, output_csv_path: Path) -> bool:
    """
    Use xml_to_df logic to convert the extracted XML file into a CSV.
    Returns True if successful, False otherwise.
    """
    if not extracted_file_path.exists():
        return False
    try:
        df = xml_to_df(extracted_file_path)
        df.to_csv(output_csv_path, index=False)
        return True
    except Exception as e:
        print(f"[ERROR] convert_extracted_file_to_csv: {e}")
        return False


###############################################################################
#                   CHUNKING LOGIC (By Content Type via TAG_MAP)
###############################################################################
def chunk_xml_by_type(xml_file_path: Path, content_type: str, records_per_file: int = 10000):
    """
    Chunk a Discogs XML file by the appropriate tag for its 'content_type'.
    e.g. 'releases' => chunk by <release ...>
         'artists'  => chunk by <artist ...>
         'masters'  => chunk by <master ...>
         'labels'   => chunk by <label ...>
    If content_type is unknown, default to <release ...>.

    We detect new records by a regex on the start tag (like <release ...id=).
    Then we buffer lines until the *next* record starts, writing each record
    to the currently open chunk file. Each chunk is wrapped in <root>...</root>.
    """
    start_time = time.time()
    record_counter = 0
    file_counter = 0

    # Decide which tag pattern to look for
    chunk_base_tag = TAG_MAP.get(content_type, "<release")
    tag_pattern = re.compile(rf"<\s*{content_type[:-1]}\b.*?id\s*=", re.IGNORECASE) if content_type in TAG_MAP else \
        re.compile(r"<\s*release\b.*?id\s*=", re.IGNORECASE)

    output_folder = f"chunked_{content_type}"
    output_folder_path = xml_file_path.parent / output_folder
    output_folder_path.mkdir(exist_ok=True)

    with xml_file_path.open("r", encoding="utf-8", errors="replace") as data_file:
        # Prepare first chunk
        output_file_path = output_folder_path / f"chunk_{file_counter}.xml"
        output_file = output_file_path.open("w", encoding="utf-8")
        output_file.write("<root>\n")

        record_lines = []
        for line in data_file:
            # If we detect a new record and we already have lines for a record in buffer,
            # that means the lines in record_lines belong to the *previous* record.
            if tag_pattern.search(line):
                if record_lines:
                    output_file.write(''.join(record_lines))
                    record_counter += 1

                    # Check if we need a new chunk
                    if record_counter % records_per_file == 0:
                        output_file.write("</root>\n")
                        output_file.close()
                        file_counter += 1
                        output_file_path = output_folder_path / f"chunk_{file_counter}.xml"
                        output_file = output_file_path.open("w", encoding="utf-8")
                        output_file.write("<root>\n")

                    record_lines = []

            record_lines.append(line)

        # Write any leftover
        if record_lines:
            output_file.write(''.join(record_lines))
            record_counter += 1

        output_file.write("</root>\n")
        output_file.close()
        file_counter += 1

    elapsed_time = time.time() - start_time
    print(f"[INFO] Done chunking {xml_file_path.name}.")
    print(f"       Created {file_counter} chunk files in '{output_folder}', "
          f"total {record_counter} records. Elapsed: {elapsed_time / 60:.1f} min.")


###############################################################################
#                             S3 + UI + Main Logic
###############################################################################

PATH = Path(__file__).parent / 'assets'


def human_readable_size(num_bytes):
    """Convert a file size in bytes to a human-readable string (KB, MB, or GB),
       with 0 digits after the decimal."""
    if num_bytes < 1024:
        return f"{num_bytes} B"
    elif num_bytes < 1024 ** 2:
        return f"{num_bytes // 1024} KB"
    elif num_bytes < 1024 ** 3:
        return f"{num_bytes // (1024 ** 2)} MB"
    else:
        return f"{num_bytes // (1024 ** 3)} GB"


def list_directories_from_s3(base_url="https://discogs-data-dumps.s3.us-west-2.amazonaws.com/", prefix="data/"):
    """Retrieve a list of 'directories' (common prefixes) from the S3 XML listing."""
    import xml.etree.ElementTree as ET

    url = base_url + "?prefix=" + prefix + "&delimiter=/"
    r = requests.get(url)
    r.raise_for_status()
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    root = ET.fromstring(r.text)
    dirs = []
    for cp in root.findall(ns + 'CommonPrefixes'):
        p = cp.find(ns + 'Prefix').text
        dirs.append(p)
    return dirs


def list_files_in_directory(base_url, directory_prefix):
    """List all files (key, size, last_modified) in a particular S3 directory prefix."""
    import xml.etree.ElementTree as ET

    url = base_url + "?prefix=" + directory_prefix
    r = requests.get(url)
    r.raise_for_status()
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    root = ET.fromstring(r.text)

    data = []
    for content in root.findall(ns + 'Contents'):
        key = content.find(ns + 'Key').text
        size_str = content.find(ns + 'Size').text
        last_modified = content.find(ns + 'LastModified').text

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


class CollapsingFrame(ttk.Frame):
    """A collapsible Frame widget for grouping content."""

    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.columnconfigure(0, weight=1)
        self.cumulative_rows = 0
        self.images = [
            ttk.PhotoImage(file=PATH / 'icons8-double-up-30.png'),
            ttk.PhotoImage(file=PATH / 'icons8-double-right-30.png')
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

        def _func(c=child):
            return self._toggle_open_close(c)

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


class DiscogsDownloaderUI(ttk.Frame):
    def __init__(self, master, data_df, **kwargs):
        super().__init__(master, **kwargs)
        self.pack(fill=BOTH, expand=YES)
        self.data_df = data_df
        self.stop_flag = False

        # For checkboxes in table
        self.check_vars = {}
        self.checkbuttons = {}

        # Banner image
        self.banner_image = ttk.PhotoImage(file="assets/logo.png")
        banner = ttk.Label(self, image=self.banner_image)
        banner.pack(side="top", fill="x")

        # Load icons
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
        imgpath = Path(__file__).parent / 'assets'
        for key, val in image_files.items():
            _path = imgpath / val
            if _path.exists():
                self.photoimages.append(ttk.PhotoImage(name=key, file=_path))

        # Top button bar
        buttonbar = ttk.Frame(self, style='primary.TFrame')
        buttonbar.pack(fill=X, pady=1, side=TOP)
        _value = 'Log: Ready.'
        self.setvar('scroll-message', _value)

        btn = ttk.Button(buttonbar, text='Fetch Data', image='fetch', compound=TOP, command=self.start_scraping)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        _func = self.download_selected
        btn = ttk.Button(buttonbar, text='Download', image='download', compound=TOP, command=_func)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        _func = self.stop_download
        btn = ttk.Button(buttonbar, text='Stop', image='stop-light', compound=TOP, command=_func)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        _func = self.refresh_data
        btn = ttk.Button(buttonbar, text='Refresh', image='refresh', compound=TOP, command=_func)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        btn = ttk.Button(buttonbar, text='Delete', image='delete', compound=TOP,
                         command=self.delete_selected)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        btn = ttk.Button(buttonbar, text='Extract', image='extract', compound=TOP,
                         command=self.extract_selected)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # UPDATED "CONVERT" BUTTON
        btn = ttk.Button(buttonbar, text='Convert', image='convert', compound=TOP,
                         command=self.convert_selected)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        _func = lambda: Messagebox.ok(message='Open Settings')
        btn = ttk.Button(buttonbar, text='Settings', image='settings', compound=TOP, command=_func)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # Left panel
        left_panel = ttk.Frame(self, style='bg.TFrame', width=250)
        left_panel.pack(side=LEFT, fill=Y)
        left_panel.pack_propagate(False)

        ds_cf = CollapsingFrame(left_panel)
        ds_cf.pack(fill=X, pady=1)

        ds_frm = ttk.Frame(ds_cf, padding=5)
        ds_frm.columnconfigure(1, weight=1)
        ds_cf.add(child=ds_frm, title='Data Summary', bootstyle=SECONDARY)

        lbl = ttk.Label(ds_frm, text='Download Folder:')
        lbl.grid(row=0, column=0, sticky=W, pady=2)
        lbl = ttk.Label(ds_frm, textvariable='downloadfolder')
        lbl.grid(row=1, column=0, sticky=W, padx=0, pady=2)
        downloads_dir = Path.home() / "Downloads" / "Discogs"
        self.setvar('downloadfolder', f" → {str(downloads_dir)}")

        lbl = ttk.Label(ds_frm, text='Size of Downloaded Files:')
        lbl.grid(row=2, column=0, sticky=W, pady=2)

        self.downloaded_size_var = ttk.StringVar(value="Calculating...")
        lbl = ttk.Label(ds_frm, textvariable=self.downloaded_size_var)
        lbl.grid(row=3, column=0, sticky=W, padx=0, pady=2)

        sep = ttk.Separator(ds_frm, bootstyle=SECONDARY)
        sep.grid(row=6, column=0, columnspan=2, pady=10, sticky=EW)

        _func = self.open_discogs_folder
        open_btn = ttk.Button(ds_frm, text='Open Folder', command=_func, bootstyle=LINK,
                              image='opened-folder', compound=LEFT)
        open_btn.grid(row=5, column=0, columnspan=2, sticky=W)

        # Status panel
        status_cf = CollapsingFrame(left_panel)
        status_cf.pack(fill=BOTH, pady=1)

        status_frm = ttk.Frame(status_cf, padding=10)
        status_frm.columnconfigure(1, weight=1)
        status_cf.add(child=status_frm, title='Status', bootstyle=SECONDARY)

        lbl = ttk.Label(status_frm, textvariable='prog-message', font='Arial 10 bold')
        lbl.grid(row=0, column=0, columnspan=2, sticky=W)
        self.setvar('prog-message', 'Idle...')

        pb = ttk.Progressbar(status_frm, length=200, mode="determinate", bootstyle=SUCCESS)
        pb.grid(row=1, column=0, columnspan=2, sticky=EW, pady=(10, 5))
        self.pb = pb
        self.pb["value"] = 0

        lbl = ttk.Label(status_frm, textvariable='prog-time-started')
        lbl.grid(row=2, column=0, columnspan=2, sticky=EW, pady=2)
        self.setvar('prog-time-started', 'Not started')

        lbl = ttk.Label(status_frm, textvariable='prog-speed')
        lbl.grid(row=3, column=0, columnspan=2, sticky=EW, pady=2)
        self.setvar('prog-speed', 'Speed: 0.00 MB/s')

        lbl = ttk.Label(status_frm, textvariable='prog-time-elapsed')
        lbl.grid(row=4, column=0, columnspan=2, sticky=EW, pady=2)
        self.setvar('prog-time-elapsed', 'Elapsed: 0 sec')

        lbl = ttk.Label(status_frm, textvariable='prog-time-left')
        lbl.grid(row=5, column=0, columnspan=2, sticky=EW, pady=2)
        self.setvar('prog-time-left', 'Left: 0 sec')

        sep = ttk.Separator(status_frm, bootstyle=SECONDARY)
        sep.grid(row=6, column=0, columnspan=2, pady=10, sticky=EW)

        _func = self.stop_download
        btn = ttk.Button(status_frm, text='Stop', command=_func, bootstyle=LINK,
                         image='stop-download-light', compound=LEFT)
        btn.grid(row=7, column=0, columnspan=2, sticky=W)

        sep = ttk.Separator(status_frm, bootstyle=SECONDARY)
        sep.grid(row=8, column=0, columnspan=2, pady=10, sticky=EW)

        lbl = ttk.Label(status_frm, textvariable='current-file-msg')
        lbl.grid(row=9, column=0, columnspan=2, pady=2, sticky=EW)
        self.setvar('current-file-msg', 'No file downloading')

        lbl = ttk.Label(left_panel, image='logo', style='bg.TLabel')
        lbl.pack(side='right')

        # Right panel
        right_panel = ttk.Frame(self, padding=(2, 0))
        right_panel.pack(side=RIGHT, fill=BOTH, expand=NO)

        self.style = ttk.Style()
        self.style.configure(
            "Treeview.Heading",
            padding=(0, 11),
            font=("Arial", 13)
        )
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

        buttonbar2 = ttk.Frame(self, style='primary.TFrame')
        buttonbar2.pack(fill=X, pady=1, side=BOTTOM)

        def _scrollbar_set(first, last):
            vsb.set(first, last)
            self.position_checkbuttons()

        tv.configure(yscrollcommand=_scrollbar_set)

        for col in tv["columns"]:
            tv.heading(col, text=col.capitalize(), anchor=CENTER)

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

        # Start scraping after short delay
        self.after(100, self.start_scraping)
        self.update_downloaded_size()

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
            tag = color_map.get(row["month"], "month1")
            downloaded_status = row.get("Downloaded", "✖")
            extracted_status = row.get("Extracted", "✖")
            processed_status = row.get("Processed", "✖")

            values = [
                "",
                row["month"],
                row["content"],
                row["size"],
                downloaded_status,
                extracted_status,
                processed_status,
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
            cb_width = cb.winfo_reqwidth()
            cb_height = cb.winfo_reqheight()

            cb_x = x + (width - cb_width) // 2
            cb_y = y + (height - cb_height) // 2

            item_top = self.tree.winfo_rooty() + y
            item_bottom = item_top + height

            if first_visible <= item_bottom and item_top <= last_visible:
                cb.place(in_=self.tree, x=cb_x, y=cb_y + 1, width=height - 2, height=height - 2)
            else:
                cb.place_forget()

    def delete_selected(self):
        """
        Delete the file(s) for downloaded, extracted,
        and processed if applicable from disk,
        then set all states to ✖.
        """
        checked_items = [item for item, var in self.check_vars.items() if var.get() == 1]
        if not checked_items:
            messagebox.showwarning("Warning", "No file selected!")
            return

        confirm = messagebox.askyesno("Confirm Deletion",
                                      "Are you sure you want to delete the selected file(s)?")
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
                folder_name = row_data["month"].values[0]
                filename = os.path.basename(url)
                file_path = Path.home() / "Downloads" / "Discogs" / "Datasets" / folder_name / filename

                csv_path = file_path.with_suffix('.csv')

                try:
                    if file_path.exists():
                        file_path.unlink()
                        deleted_files.append(file_path)
                    if csv_path.exists():
                        csv_path.unlink()
                        deleted_files.append(csv_path)

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
        line_number = int(line_number_str)

        # Color every second line blue
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
        """Ensure columns 'Downloaded', 'Extracted', 'Processed' exist, defaulting to ✖,
           and check on disk if each file is present."""
        for col in ["Downloaded", "Extracted", "Processed"]:
            if col not in data_df.columns:
                data_df[col] = "✖"

        downloads_dir = Path.home() / "Downloads" / "Discogs" / "Datasets"
        for idx, row in data_df.iterrows():
            folder_name = str(row["month"])
            filename = os.path.basename(row["URL"])
            file_path = downloads_dir / folder_name / filename

            downloaded_status = "✖"
            extracted_status = "✖"
            processed_status = "✖"

            # Check if the compressed file is present
            if file_path.exists():
                downloaded_status = "✔"

                # If it's .gz, check for extracted .xml
                if file_path.suffix.lower() == ".gz":
                    extracted_file = file_path.with_suffix('')  # .gz -> .xml
                    if extracted_file.exists() and extracted_file.suffix.lower() == ".xml":
                        extracted_status = "✔"
                        # If .csv also exists, mark processed
                        csv_file = extracted_file.with_suffix('.csv')
                        if csv_file.exists():
                            processed_status = "✔"
                else:
                    # If it's not gz but maybe raw .xml
                    if file_path.suffix.lower() == ".xml":
                        extracted_status = "✔"
                        # check .csv
                        csv_file = file_path.with_suffix('.csv')
                        if csv_file.exists():
                            processed_status = "✔"

            data_df.at[idx, "Downloaded"] = downloaded_status
            data_df.at[idx, "Extracted"] = extracted_status
            data_df.at[idx, "Processed"] = processed_status
        return data_df

    def start_download(self, url, filename, last_modified):
        self.stop_flag = False
        folder_name = last_modified.strftime("%Y-%m")
        Thread(target=self.download_file, args=(url, filename, folder_name), daemon=True).start()

    def parallel_download(self, url, filename, folder_name, total_size):
        downloads_dir = Path.home() / "Downloads" / "Discogs" / "Datasets"
        target_dir = downloads_dir / folder_name
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / filename

        num_threads = 4
        part_size = total_size // num_threads

        partial_paths = []
        thread_progress = [0] * num_threads
        lock = Lock()

        def download_segment(idx, start, end):
            headers = {"Range": f"bytes={start}-{end}"}
            r = requests.get(url, headers=headers, stream=True)
            part_file = file_path.with_name(file_path.name + f".part{idx}")
            partial_paths.append(part_file)
            with open(part_file, "wb") as f:
                for chunk in r.iter_content(1024 * 64):
                    if self.stop_flag:
                        return
                    if chunk:
                        f.write(chunk)
                        with lock:
                            thread_progress[idx] += len(chunk)

        start_time = datetime.now()
        self.setvar('prog-time-started', f'Started at: {start_time.strftime("%Y-%m-%d %H:%M:%S")}')
        threads = []
        for i in range(num_threads):
            start = i * part_size
            end = (i + 1) * part_size - 1 if i < num_threads - 1 else total_size - 1
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
            elapsed = (datetime.now() - start_time).total_seconds()
            speed = (downloaded_size / elapsed) / (1024 * 1024) if elapsed > 0 else 0.0
            self.setvar('prog-speed', f'Speed: {speed:.2f} MB/s')
            total_percentage = (downloaded_size / total_size) * 100 if total_size > 0 else 0
            self.setvar('prog-message', f'Downloading {filename}: {total_percentage:.2f}%')
            self.pb["maximum"] = total_size
            self.pb["value"] = downloaded_size
            elapsed_minutes = int(elapsed) // 60
            elapsed_seconds = int(elapsed) % 60
            self.setvar('prog-time-elapsed', f'Elapsed: {elapsed_minutes} min {elapsed_seconds} sec')
            if downloaded_size > 0:
                rate = downloaded_size / elapsed if elapsed > 0 else 0
                left = int((total_size - downloaded_size) / rate) if rate > 0 else 0
                left_minutes = left // 60
                left_seconds = left % 60
                self.setvar('prog-time-left', f'Left: {left_minutes} min {left_seconds} sec')

        for t in threads:
            t.join()

        if self.stop_flag:
            for p in partial_paths:
                if p.exists():
                    p.unlink()
            return False

        with open(file_path, "wb") as f_out:
            for i in range(num_threads):
                part_file = file_path.with_name(file_path.name + f".part{i}")
                if part_file.exists():
                    with open(part_file, "rb") as f_in:
                        f_out.write(f_in.read())
                    part_file.unlink()

        return True

    def download_file(self, url, filename, folder_name):
        try:
            self.setvar('prog-message', 'Preparing download...')
            head = requests.head(url)
            head.raise_for_status()
            total_size = int(head.headers.get('Content-Length', 0))
            accept_ranges = head.headers.get('Accept-Ranges', 'none')

            if total_size > 0 and accept_ranges.lower() == 'bytes':
                success = self.parallel_download(url, filename, folder_name, total_size)
                if not success:
                    if self.stop_flag:
                        self.log_to_console("Operation Stopped", "WARNING")
                        file_path = (Path.home() / "Downloads" / "Discogs" / "Datasets" / folder_name / filename)
                        if file_path.exists():
                            file_path.unlink()
                            self.log_to_console(f"Incomplete file {file_path} deleted.", "WARNING")
                        self.setvar('prog-message', 'Idle...')
                        return
                    else:
                        self.log_to_console("Parallel download failed, falling back to single-thread.", "WARNING")
                        self.single_thread_download(url, filename, folder_name)
                else:
                    downloads_dir = Path.home() / "Downloads" / "Discogs" / "Datasets"
                    file_path = downloads_dir / folder_name / filename
                    self.log_to_console(f"{filename} successfully downloaded: {file_path}", "INFO")
                    # Mark downloaded as ✔, extracted -> ✖, processed -> ✖
                    self.data_df.loc[self.data_df["URL"] == url, "Downloaded"] = "✔"
                    self.data_df.loc[self.data_df["URL"] == url, "Extracted"] = "✖"
                    self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✖"
                    self.populate_table(self.data_df)
                    self.update_downloaded_size()
                    self.setvar('prog-message', 'Idle...')
            else:
                self.single_thread_download(url, filename, folder_name)

        except Exception as e:
            self.log_to_console(f"Error: {e}", "ERROR")
            if 'file_path' in locals() and file_path.exists():
                file_path.unlink()
                self.log_to_console(f"Incomplete file {file_path} deleted.", "WARNING")

    def single_thread_download(self, url, filename, folder_name):
        self.log_to_console("Partial downloads not supported. Using single-threaded download.", "INFO")
        downloads_dir = Path.home() / "Downloads" / "Discogs" / "Datasets"
        target_dir = downloads_dir / folder_name
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / filename

        response = requests.get(url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024 * 64
        self.pb["value"] = 0
        self.pb["maximum"] = total_size

        start_time = datetime.now()
        self.setvar('prog-time-started', f'Started at: {start_time.strftime("%Y-%m-%d %H:%M:%S")}')
        downloaded_size = 0

        with open(file_path, "wb") as file:
            for data in response.iter_content(block_size):
                if self.stop_flag:
                    self.setvar('prog-message', 'Idle...')
                    self.log_to_console("Operation Stopped", "WARNING")
                    file.close()
                    if file_path.exists():
                        os.remove(file_path)
                        self.log_to_console(f"Incomplete file {file_path} deleted.", "WARNING")
                    return
                file.write(data)
                downloaded_size += len(data)
                self.pb["value"] = downloaded_size
                elapsed = (datetime.now() - start_time).total_seconds()

                speed = (downloaded_size / elapsed) / (1024 * 1024) if elapsed > 0 else 0.0
                self.setvar('prog-speed', f'Speed: {speed:.2f} MB/s')
                self.setvar('prog-time-elapsed', f'Elapsed: {int(elapsed) // 60} min {int(elapsed) % 60} sec')

                if total_size > 0 and downloaded_size > 0:
                    percentage = (downloaded_size / total_size) * 100
                    left = int(
                        (total_size - downloaded_size) / (downloaded_size / elapsed)) if downloaded_size > 0 else 0
                    left_minutes = left // 60
                    left_seconds = left % 60
                    self.setvar('prog-time-left', f'Left: {left_minutes} min {left_seconds} sec')
                    self.setvar('prog-message', f'Downloading {filename}: {percentage:.2f}%')
                    self.setvar('current-file-msg', f'Current file: {file_path}')

        self.setvar('prog-message', 'Idle...')
        self.log_to_console(f"{filename} successfully downloaded: {file_path}", "INFO")
        self.data_df.loc[self.data_df["URL"] == url, "Downloaded"] = "✔"
        self.data_df.loc[self.data_df["URL"] == url, "Extracted"] = "✖"
        self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✖"
        self.populate_table(self.data_df)
        self.update_downloaded_size()

    def stop_download(self):
        self.stop_flag = True
        self.log_to_console("Operation Stopped. Cleaning up...", "WARNING")

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
                last_modified = row_data["last_modified"].values[0]
                last_modified = pd.to_datetime(last_modified)
                filename = os.path.basename(url)
                self.start_download(url, filename, last_modified)

    def extract_gz_file_with_progress(self, file_path):
        self.setvar('prog-message', 'Extracting file...')
        output_path = file_path.with_suffix('')
        total_size = file_path.stat().st_size
        self.pb["value"] = 0
        self.pb["maximum"] = total_size

        start_time = datetime.now()
        self.setvar('prog-time-started', f'Started at: {start_time.strftime("%Y-%m-%d %H:%M:%S")}')
        extracted_size = 0
        block_size = 1024 * 64

        with gzip.open(file_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
            while not self.stop_flag:
                chunk = f_in.read(block_size)
                if not chunk:
                    break
                f_out.write(chunk)
                extracted_size += len(chunk)
                self.pb["value"] = extracted_size

                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed > 0:
                    speed = (extracted_size / elapsed) / (1024 * 1024)
                else:
                    speed = 0.0

                self.setvar('prog-speed', f"Extract Speed: {speed:.2f} MB/s")

                if total_size > 0:
                    percentage = (extracted_size / total_size) * 100
                    self.setvar('prog-message', f"Extracting {file_path.name}: {percentage:.2f}%")
                    elapsed_minutes = int(elapsed) // 60
                    elapsed_seconds = int(elapsed) % 60
                    self.setvar('prog-time-elapsed', f'Elapsed: {elapsed_minutes} min {elapsed_seconds} sec')
                    if extracted_size > 0:
                        rate = extracted_size / elapsed if elapsed > 0 else 0
                        left = int((total_size - extracted_size) / rate) if rate > 0 else 0
                        left_minutes = left // 60
                        left_seconds = left % 60
                        self.setvar('prog-time-left', f'Left: {left_minutes} min {left_seconds} sec')
                else:
                    self.setvar('prog-message', "Extracting...")

        self.setvar('prog-message', 'Idle...')
        return not self.stop_flag

    def extract_selected(self):
        self.log_to_console("Extracting started...", "INFO")
        self.setvar('prog-message', 'Waiting 5 seconds before extraction...')
        time.sleep(5)
        self.setvar('prog-message', 'Extracting now...')

        extracted_files = []
        failed_files = []

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

            if downloaded_val != "✔":
                self.log_to_console("File not downloaded, cannot extract.", "WARNING")
                continue

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
                folder_name = row_data["month"].values[0]
                filename = os.path.basename(url)
                file_path = Path.home() / "Downloads" / "Discogs" / "Datasets" / folder_name / filename

                if file_path.suffix.lower() == ".gz":
                    success = self.extract_gz_file_with_progress(file_path)
                    if success:
                        output_path = file_path.with_suffix('')
                        extracted_files.append(output_path)
                        self.log_to_console(f"Extracted: {file_path} → {output_path}", "INFO")
                        self.data_df.loc[self.data_df["URL"] == url, "Extracted"] = "✔"
                        self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✖"
                    else:
                        failed_files.append(file_path)
                        self.log_to_console(f"Error or stopped extracting {file_path}.", "ERROR")
                else:
                    # Maybe raw .xml or something else
                    if file_path.exists() and file_path.suffix.lower() == ".xml":
                        self.log_to_console(f"File {file_path} is already .xml; no extraction needed.", "INFO")
                        self.data_df.loc[self.data_df["URL"] == url, "Extracted"] = "✔"
                        self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✖"
                    else:
                        self.log_to_console(f"{file_path} is not a .gz file.", "WARNING")

        self.populate_table(self.data_df)
        if extracted_files:
            self.log_to_console(f"Extracted files: {', '.join(map(str, extracted_files))}", "INFO")
        if failed_files:
            self.log_to_console(f"Failed to extract files: {', '.join(map(str, failed_files))}", "WARNING")

    ###########################################################################
    #                           CONVERT SELECTED
    ###########################################################################
    def convert_selected(self):
        """
        Convert extracted .xml file(s) to a single CSV.
        If the file is large, chunk it first, then read all chunk_*.xml into
        one combined DataFrame, and finally write exactly ONE CSV named after
        the original extracted XML file.

        After CSV is created, remove the chunk folder.
        """
        checked_items = [item for item, var in self.check_vars.items() if var.get() == 1]
        if not checked_items:
            messagebox.showwarning("Warning", "No file selected!")
            return

        for item in checked_items:
            values = self.tree.item(item, "values")
            month_val = values[1]
            content_val = values[2]  # e.g. "releases", "artists", etc.
            size_val = values[3]
            downloaded_val = values[4]
            extracted_val = values[5]
            processed_val = values[6]

            if extracted_val != "✔":
                self.log_to_console("File not extracted, cannot convert.", "WARNING")
                continue
            if processed_val == "✔":
                self.log_to_console("File already processed. Skipping...", "INFO")
                continue

            row_data = self.data_df[
                (self.data_df["month"] == month_val) &
                (self.data_df["content"] == content_val) &
                (self.data_df["size"] == size_val) &
                (self.data_df["Downloaded"] == downloaded_val) &
                (self.data_df["Extracted"] == extracted_val) &
                (self.data_df["Processed"] == processed_val)
                ]

            if row_data.empty:
                continue

            url = row_data["URL"].values[0]
            folder_name = row_data["month"].values[0]
            filename = os.path.basename(url)
            # e.g. discogs_20240101_releases.xml (after .gz is stripped)
            extracted_file = (
                    Path.home() / "Downloads" / "Discogs" / "Datasets" / folder_name / filename
            ).with_suffix("")  # remove .gz => .xml

            if extracted_file.exists() and extracted_file.suffix.lower() == ".xml":
                file_size_mb = extracted_file.stat().st_size / (1024 * 1024)
                combined_csv = extracted_file.with_suffix(".csv")  # final single CSV

                if file_size_mb > 512:
                    # 1) Chunk the big XML
                    self.log_to_console(f"File {extracted_file} is {file_size_mb:.2f} MB. Chunking...", "INFO")
                    chunk_xml_by_type(extracted_file, content_type=content_val, records_per_file=10000)

                    # 2) Read each chunk into memory & combine
                    chunks_dir = extracted_file.parent / f"chunked_{content_val}"
                    all_dfs = []
                    for chunk_file in sorted(chunks_dir.glob("chunk_*.xml")):
                        self.log_to_console(f"Reading chunk {chunk_file.name}...", "INFO")
                        try:
                            df = xml_to_df(chunk_file)
                            if not df.empty:
                                all_dfs.append(df)
                        except Exception as e:
                            self.log_to_console(f"Failed to parse {chunk_file}: {e}", "ERROR")

                    if all_dfs:
                        final_df = pd.concat(all_dfs, ignore_index=True)
                        self.log_to_console(f"Writing combined CSV -> {combined_csv}", "INFO")
                        final_df.to_csv(combined_csv, index=False)
                        self.log_to_console(f"All chunks combined into {combined_csv}", "INFO")

                        # 3) Remove chunk folder
                        try:
                            shutil.rmtree(chunks_dir)
                            self.log_to_console(f"Deleted chunk folder: {chunks_dir}", "INFO")
                        except Exception as e:
                            self.log_to_console(f"Failed to remove chunk folder {chunks_dir}: {e}", "ERROR")

                        # Mark processed
                        self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✔"
                    else:
                        self.log_to_console("No dataframes found. Possibly parse errors?", "WARNING")

                else:
                    # Single-file is small enough, parse directly
                    self.log_to_console(f"Converting single XML -> {combined_csv}", "INFO")
                    success = convert_extracted_file_to_csv(extracted_file, combined_csv)
                    if success:
                        self.log_to_console(f"Converted {extracted_file} → {combined_csv}", "INFO")
                        self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✔"
                    else:
                        self.log_to_console(f"Failed to convert {extracted_file}.", "ERROR")
            else:
                self.log_to_console(f"Extracted file {extracted_file} not found or not .xml!", "ERROR")

        self.populate_table(self.data_df)

    def get_folder_size(self, folder_path):
        total_size = 0
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.exists(file_path):
                    total_size += os.path.getsize(file_path)
        return total_size

    def update_downloaded_size(self):
        downloads_dir = Path.home() / "Downloads" / "Discogs"
        size_in_bytes = self.get_folder_size(downloads_dir)
        one_gb = 1024 ** 3
        if size_in_bytes >= one_gb:
            size_in_gb = size_in_bytes // one_gb
            self.downloaded_size_var.set(f"→ {size_in_gb} GB")
        else:
            size_in_mb = size_in_bytes // (1024 ** 2)
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


def main():
    # Initialize columns including "Processed"
    empty_df = pd.DataFrame(columns=["month", "content", "size", "last_modified", "key", "URL",
                                     "Downloaded", "Extracted", "Processed"])
    empty_df["Downloaded"] = "✖"
    empty_df["Extracted"] = "✖"
    empty_df["Processed"] = "✖"

    app = ttk.Window("Discogs Data Downloader", themename="darkly")
    primary_color = app.style.colors.primary
    style = ttk.Style()
    style.configure("Treeview.Heading", background=primary_color, foreground="white", font=("Helvetica", 10, "bold"))

    DiscogsDownloaderUI(app, empty_df)
    window_width = 750
    window_height = 750

    screen_width = app.winfo_screenwidth()
    screen_height = app.winfo_screenheight()

    center_x = int((screen_width - window_width) / 2)
    center_y = int((screen_height - window_height) / 2)

    app.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
    app.resizable(False, False)

    app.mainloop()


if __name__ == "__main__":
    main()
