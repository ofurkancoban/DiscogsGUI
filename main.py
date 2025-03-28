import gzip
import sys
import shutil  # <-- I use shutil.rmtree() to remove chunk folders
import requests
import platform
import subprocess
import pandas as pd
import xml.etree.ElementTree as ET
from threading import Thread, Lock
import webbrowser  # <-- for opening social media links
import csv
import queue
from tkinter import filedialog
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import StringVar  # Import StringVar
import time
from datetime import datetime, timedelta
import json
import os
import re
import math
from pathlib import Path
from tkinter import messagebox
from PIL import Image, ImageDraw, ImageFont, ImageFilter
from tkinter import filedialog, StringVar, messagebox, BooleanVar
###############################################################################
#                              XML → DataFrame logic
###############################################################################

def sanitize_line(line: str) -> str:
    """
    Removes invalid XML chars and replaces bare '&' with &amp;.
    """
    line = re.sub(r'[^\x09\x0A\x0D\x20-\uD7FF\uE000-\uFFFD]', '', line)
    line = re.sub(r'&(?![a-zA-Z0-9#]+;)', '&amp;', line)
    return line

def chunk_xml_by_type(xml_file: Path, content_type: str, records_per_file=10000, logger=None):
    """
    A purely line-based chunker for old Discogs dumps that have no single root.
    No ElementTree usage => no 'junk after document element'.
    We just scan for <artist>...</artist>, <label>...</label>, or <release>...</release>.

    :param xml_file: the path to the old .xml
    :param content_type: e.g. 'artists', 'labels', or 'releases'
    :param records_per_file: how many <record_tag> blocks per chunk
    :param logger: optional logging function
    """
    record_tag = content_type[:-1].lower()  # 'artists'->'artist', 'labels'->'label', 'releases'->'release'
    start_pat = re.compile(fr'<{record_tag}\b', re.IGNORECASE)
    end_pat   = re.compile(fr'</{record_tag}>', re.IGNORECASE)

    chunk_folder = xml_file.parent / f"chunked_{content_type}"
    chunk_folder.mkdir(exist_ok=True)

    if logger:
        logger(f"[LINE-BASED] Chunking '{xml_file.name}' for <{record_tag}> blocks.", "INFO")

    chunk_count = 0
    record_count = 0
    inside_record = False
    buffer_lines = []
    current_chunk_file = None

    def open_new_chunk():
        nonlocal chunk_count, current_chunk_file, record_count
        chunk_count += 1
        chunk_path = chunk_folder / f"chunk_{str(chunk_count).zfill(6)}.xml"
        current_chunk_file = open(chunk_path, 'w', encoding='utf-8')
        current_chunk_file.write(f'<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<{content_type}>\n')
        record_count = 0
        if logger:
            logger(f"Created new chunk: {chunk_path.name}", "INFO")
        else:
            print(f"Created new chunk: {chunk_path.name}")

    def close_chunk():
        nonlocal current_chunk_file
        if current_chunk_file:
            current_chunk_file.write(f"</{content_type}>")
            current_chunk_file.close()
            current_chunk_file = None

    # Start the first chunk
    open_new_chunk()

    with xml_file.open('r', encoding='utf-8', errors='ignore') as f:
        for raw_line in f:
            line = sanitize_line(raw_line)

            if not inside_record:
                # Check if line has <artist> or <label> or <release>
                if start_pat.search(line):
                    inside_record = True
                    buffer_lines = [line]
            else:
                # We are inside a record block, keep collecting
                buffer_lines.append(line)
                if end_pat.search(line):
                    # Found closing tag
                    record_xml = ''.join(buffer_lines)
                    current_chunk_file.write(record_xml + '\n')
                    record_count += 1
                    inside_record = False
                    buffer_lines = []

                    if record_count >= records_per_file:
                        close_chunk()
                        open_new_chunk()

    close_chunk()

    if logger:
        logger(f"[LINE-BASED] Finished. Created {chunk_count} chunk(s).", "INFO")
    else:
        print(f"[LINE-BASED] Chunking complete. {chunk_count} chunk(s) in {chunk_folder}")

def xml_to_df(xml_path: Path, record_tag: str) -> pd.DataFrame:
    """
    Convert an XML file to a pandas DataFrame, handling two levels of nested tags.
    Automatically detects list-type columns based on multiple occurrences of tags.
    Nested tags are stored as lists and serialized as JSON strings for CSV compatibility.
    """
    records = []
    current_record = {}
    current_path = []
    nested_data = {}
    tag_counts = {}

    for event, elem in ET.iterparse(str(xml_path), events=("start", "end")):
        if event == "start":
            current_path.append(elem.tag)
            if elem.tag == record_tag:
                current_record = {}
                nested_data = {}
                tag_counts = {}
        elif event == "end":
            if elem.tag == record_tag:
                # Merge nested_data into current_record
                for key, value in nested_data.items():
                    if key in current_record:
                        if isinstance(current_record[key], list):
                            current_record[key].append(value)
                        else:
                            current_record[key] = [current_record[key], value]
                    else:
                        current_record[key] = value
                records.append(current_record)
                current_record = {}
                nested_data = {}
                tag_counts = {}
            else:
                if elem.text and not elem.text.isspace():
                    if len(current_path) >= 3:
                        # Two levels deep
                        parent_tag = current_path[-3]
                        child_tag = current_path[-2]
                        full_tag = f"{parent_tag}_{child_tag}_{elem.tag}"
                    elif len(current_path) == 2:
                        # One level deep
                        full_tag = f"{current_path[-2]}_{elem.tag}"
                    else:
                        # Root level
                        full_tag = elem.tag

                    # Initialize tag count
                    tag_counts[full_tag] = tag_counts.get(full_tag, 0) + 1

                    value = elem.text.strip()
                    if tag_counts[full_tag] > 1:
                        # If tag occurs multiple times, store as list
                        if full_tag in nested_data:
                            nested_data[full_tag].append(value)
                        else:
                            nested_data[full_tag] = [nested_data.get(full_tag, []), value]
                    else:
                        nested_data[full_tag] = value
                # Handle attributes
                for attr, value in elem.attrib.items():
                    if len(current_path) >= 3:
                        parent_tag = current_path[-3]
                        child_tag = current_path[-2]
                        tag_name = f"{parent_tag}_{child_tag}_{elem.tag}_{attr}"
                    elif len(current_path) == 2:
                        tag_name = f"{current_path[-2]}_{elem.tag}_{attr}"
                    else:
                        tag_name = f"{elem.tag}_{attr}"

                    # Initialize tag count
                    tag_counts[tag_name] = tag_counts.get(tag_name, 0) + 1

                    if tag_counts[tag_name] > 1:
                        # If attribute occurs multiple times, store as list
                        if tag_name in nested_data:
                            nested_data[tag_name].append(value)
                        else:
                            nested_data[tag_name] = [nested_data.get(tag_name, []), value]
                    else:
                        nested_data[tag_name] = value
            current_path.pop()
            elem.clear()

    # Create DataFrame from records
    df = pd.DataFrame(records)

    # Dynamically serialize list-type columns as JSON strings
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)

    return df


###############################################################################
#                            Helper to convert a single XML file to CSV
###############################################################################
def convert_extracted_file_to_csv(extracted_file_path: Path, output_csv_path: Path, record_tag: str, logger=None) -> bool:
    """
    Use xml_to_df logic to convert the extracted XML file into a CSV.
    Handles nested tags by writing them as JSON lists.
    Returns True if successful, False otherwise.
    """
    if not extracted_file_path.exists():
        if logger:
            logger(f"File not found: {extracted_file_path}", "ERROR")
        else:
            print(f"[ERROR] File not found: {extracted_file_path}")
        return False
    try:
        df = xml_to_df(extracted_file_path, record_tag)
        df.to_csv(output_csv_path, index=False)
        if logger:
            logger(f"Converted {extracted_file_path} to {output_csv_path}", "INFO")
        else:
            print(f"[INFO] Converted {extracted_file_path} to {output_csv_path}")
        return True
    except Exception as e:
        if logger:
            logger(f"convert_extracted_file_to_csv: {e}", "ERROR")
        else:
            print(f"[ERROR] convert_extracted_file_to_csv: {e}")
        return False


###############################################################################
#                   CHUNKING LOGIC (Using iterparse to avoid mismatch)
###############################################################################



###############################################################################
#                  STREAMING: Two-pass approach for each chunk
###############################################################################
def update_columns_from_chunk(chunk_file_path: Path, all_columns: set, record_tag: str, logger=None):
    """
    1. Pass: Parse the chunk file line by line.
       - Add discovered tag/attribute names to the 'all_columns' set.
       - No data is stored in memory.
    """
    current_path = []
    for event, elem in ET.iterparse(str(chunk_file_path), events=("start", "end")):
        if event == "start":
            current_path.append(elem.tag)
            # Discover attributes
            for attr, value in elem.attrib.items():
                if len(current_path) >= 3:
                    # Two levels deep
                    parent_tag = current_path[-3]
                    current_tag = current_path[-2]
                    tag_name = f"{parent_tag}_{current_tag}_{elem.tag}_{attr}"
                elif len(current_path) == 2:
                    # One level deep
                    tag_name = f"{current_path[-2]}_{elem.tag}_{attr}"
                else:
                    # Root or unexpected depth
                    tag_name = f"{elem.tag}_{attr}"
                all_columns.add(tag_name)
        elif event == "end":
            if elem.text and not elem.text.isspace():
                if len(current_path) >= 3:
                    # Two levels deep
                    parent_tag = current_path[-3]
                    current_tag = current_path[-2]
                    tag_name = f"{parent_tag}_{current_tag}_{elem.tag}"
                elif len(current_path) == 2:
                    # One level deep
                    tag_name = f"{current_path[-2]}_{elem.tag}"
                else:
                    # Root or unexpected depth
                    tag_name = elem.tag
                all_columns.add(tag_name)

            if elem.tag == record_tag:
                # Record ended, do nothing except ensure we clear
                pass

            current_path.pop()
            elem.clear()

    if logger:
        logger(f"Updated columns from {chunk_file_path.name}. Total columns now: {len(all_columns)}", "INFO")
    else:
        print(f"Updated columns from {chunk_file_path.name}. Total columns now: {len(all_columns)}")


def write_chunk_to_csv(chunk_file_path: Path, csv_writer: csv.DictWriter, all_columns: list, record_tag: str,
                       logger=None):
    """
    2. Pass: Parse the chunk file line by line.
             For each <record_tag>...</record_tag> record, write a single row to the CSV.
             Nested tags are serialized as JSON strings.
    """
    current_path = []
    record_data = {}
    nested_data = {}

    for event, elem in ET.iterparse(str(chunk_file_path), events=("start", "end")):
        if event == "start":
            current_path.append(elem.tag)
            # Save attributes
            for attr, value in elem.attrib.items():
                if len(current_path) >= 3:
                    # Two levels deep
                    parent_tag = current_path[-3]
                    current_tag = current_path[-2]
                    tag_name = f"{parent_tag}_{current_tag}_{elem.tag}_{attr}"
                elif len(current_path) == 2:
                    # One level deep
                    tag_name = f"{current_path[-2]}_{elem.tag}_{attr}"
                else:
                    # Root or unexpected depth
                    tag_name = f"{elem.tag}_{attr}"
                # Handle multiple attributes by storing in lists
                if tag_name in nested_data:
                    if isinstance(nested_data[tag_name], list):
                        nested_data[tag_name].append(value)
                    else:
                        nested_data[tag_name] = [nested_data[tag_name], value]
                else:
                    nested_data[tag_name] = value

        elif event == "end":
            if elem.text and not elem.text.isspace():
                if len(current_path) >= 3:
                    # Two levels deep
                    parent_tag = current_path[-3]
                    current_tag = current_path[-2]
                    tag_name = f"{parent_tag}_{current_tag}_{elem.tag}"
                elif len(current_path) == 2:
                    # One level deep
                    tag_name = f"{current_path[-2]}_{elem.tag}"
                else:
                    # Root or unexpected depth
                    tag_name = elem.tag
                # Handle multiple tags by storing in lists
                if tag_name in nested_data:
                    if isinstance(nested_data[tag_name], list):
                        nested_data[tag_name].append(elem.text.strip())
                    else:
                        nested_data[tag_name] = [nested_data[tag_name], elem.text.strip()]
                else:
                    nested_data[tag_name] = elem.text.strip()

            if elem.tag == record_tag:
                # Record completed, write to CSV
                # Merge nested_data into current_record
                for key, value in nested_data.items():
                    if key in record_data:
                        if isinstance(record_data[key], list):
                            record_data[key].append(value)
                        else:
                            record_data[key] = [record_data[key], value]
                    else:
                        record_data[key] = value

                # Serialize nested structures as JSON strings
                row_to_write = {}
                for col in all_columns:
                    value = record_data.get(col, None)
                    if isinstance(value, (dict, list)):
                        row_to_write[col] = json.dumps(value)
                    else:
                        row_to_write[col] = value
                csv_writer.writerow(row_to_write)
                record_data.clear()
                nested_data.clear()

            current_path.pop()
            elem.clear()

    if logger:
        logger(f"Written data from {chunk_file_path.name} to CSV.", "INFO")
    else:
        print(f"Written data from {chunk_file_path.name} to CSV.")


def convert_progress_callback(self, current_step, total_steps):
    """Called when each chunk is completed during the conversion process."""
    if total_steps == 0:
        percentage = 0
    else:
        percentage = (current_step / total_steps) * 100

    self.pb["value"] = percentage
    self.prog_message_var.set(f"Converting: {percentage:.2f}%")
    self.update_idletasks()


def convert_chunked_files_to_csv(
    chunk_folder: Path,
    output_csv: Path,
    content_type: str,
    logger=None,
    progress_cb=None  # Callback for progress updates
):
    """
    1) Discover columns across all chunk files (pass 1).
    2) Write all chunk files to CSV (pass 2).
    If progress_cb(current_step, total_steps) is provided,
    it will be called after each chunk is processed.
    """
    import csv
    record_tag = content_type[:-1]  # "releases" -> "release"
    chunk_files = sorted(chunk_folder.glob("chunk_*.xml"))
    if not chunk_files:
        if logger:
            logger(f"[WARNING] No chunk_*.xml files found in {chunk_folder}", "WARNING")
        else:
            print(f"[WARNING] No chunk_*.xml files found in {chunk_folder}")
        return

    # 1) PASS: Discover columns
    total_chunks = len(chunk_files)
    current_step = 0
    all_columns = set()

    # Total steps: PASS 1 + PASS 2 = 2 * total_chunks
    total_steps = 2 * total_chunks

    for cf in chunk_files:
        update_columns_from_chunk(cf, all_columns, record_tag=record_tag, logger=logger)
        current_step += 1

        # Update progress bar after each chunk
        if progress_cb:
            progress_cb(current_step, total_steps)

    all_columns = sorted(all_columns)  # Keep columns ordered

    # 2) PASS: Write to CSV
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_columns)
        writer.writeheader()

        for cf in chunk_files:
            write_chunk_to_csv(cf, writer, all_columns, record_tag=record_tag, logger=logger)
            current_step += 1
            if progress_cb:
                progress_cb(current_step, total_steps)

    if logger:
        logger(f"Done! Created CSV: {output_csv}", "INFO")
    else:
        print(f"[INFO] Done! Created CSV: {output_path}")


###############################################################################
#                             S3 + UI + Main Logic
###############################################################################

if getattr(sys, 'frozen', False):
    # PyInstaller ile paketlenmiş
    BASE_DIR = Path(sys._MEIPASS)
else:
    # Normal Python çalışması
    BASE_DIR = Path(__file__).parent

PATH = BASE_DIR / 'assets'


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


def extract_date_from_key(key):
    """
    Verilen key içerisindeki 'discogs_YYYYMMDD_' desenini yakalar.
    Örneğin: "data/2017/discogs_20170101_artists.xml.gz"
    """
    m = re.search(r'discogs_(\d{8})_', key)
    if m:
        date_str = m.group(1)  # Örneğin "20170101"
        try:
            return datetime.strptime(date_str, "%Y%m%d")
        except Exception as e:
            print(f"Error parsing date from key '{key}': {e}")
    return None

def get_month_from_key(key):
    """
    key içerisindeki tarihi alır ve "YYYY-MM" formatında döner.
    Eğer tarih çıkarılamazsa boş string döner.
    """
    dt = extract_date_from_key(key)
    return dt.strftime("%Y-%m") if dt else ""

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

    def add(self, child, title="", bootstyle=PRIMARY, **kwargs):
        if child.winfo_class() != 'TFrame':
            return
        # Create the header frame with a fixed height
        frm = ttk.Frame(self, bootstyle=bootstyle, height=43)  # Set fixed height for grey background
        frm.grid(row=self.cumulative_rows, column=0, sticky=EW, pady=(0, 0))
        frm.grid_propagate(False)  # Prevent the frame from shrinking to fit content

        header = ttk.Label(master=frm, text=title, bootstyle=(bootstyle, INVERSE))
        if kwargs.get('textvariable'):
            header.configure(textvariable=kwargs.get('textvariable'))
        # Center the label vertically in the frame
        header.place(relx=0, rely=0.5, x=10, anchor='w')

        child.grid(row=self.cumulative_rows + 1, column=0, sticky=NSEW, pady=(0, 0))
        self.cumulative_rows += 2


class DiscogsDataProcessorUI(ttk.Frame):
    def __init__(self, master, data_df, **kwargs):
        super().__init__(master, **kwargs)
        self.pack(fill=BOTH, expand=YES)
        self.data_df = data_df
        self.stop_flag = False
        # [UPDATED] New variable: Download folder (default: ~/Downloads/Discogs)
        default_download_dir = Path.home() / "Downloads" / "Discogs"
        self.download_dir_var = StringVar(value=str(default_download_dir))  # Use StringVar

        # Add status indicator variables
        self.status_indicator_visible = True
        self.status_indicator_active = False
        self.blink_after_id = None

        # Initialize StringVar variables for status
        self.prog_message_var = StringVar(value='Idle...')
        self.prog_speed_var = StringVar(value='Speed: 0.00 MB/s')
        self.prog_time_started_var = StringVar(value='Not started')
        self.prog_time_elapsed_var = StringVar(value='Elapsed: 0 sec')
        self.prog_time_left_var = StringVar(value='Left: 0 sec')
        self.scroll_message_var = StringVar(value='Log: Ready.')

        # For checkboxes in table
        self.check_vars = {}
        self.checkbuttons = {}

        #######################################################################
        # 1) Load all images in a dictionary + self.photoimages
        #######################################################################
        image_files = {
            'settings': 'icons8-settings-30.png',
            'info': 'icons8-info-30.png',
            'download': 'icons8-download-30.png',
            'stop': 'icons8-cancel-30.png',
            'refresh': 'icons8-refresh-30.png',
            'opened-folder': 'icons8-folder-30.png',
            'fetch': 'icons8-data-transfer-30.png',
            'delete': 'icons8-trash-30.png',
            'extract': 'icons8-open-archive-30.png',
            'convert': 'icons8-export-csv-30.png',
            'coverart': 'icons8-image-30.png',
            'logo': 'logo.png',
            'linkedin': 'linkedin.png',
            'github': 'github.png',
            'kaggle': 'kaggle.png',
            'avatar': 'avatar.png'
        }

        self.photoimages = {}
        imgpath = BASE_DIR / 'assets'
        for key, val in image_files.items():
            _path = imgpath / val
            if _path.exists():
                try:
                    self.photoimages[key] = ttk.PhotoImage(name=key, file=_path)
                except Exception as e:
                    print(f"[ERROR] Failed to load image: {e}")
            else:
                print(f"[WARNING] Image file not found: {_path}")

        #######################################################################
        # NEW TOP BANNER FRAME
        #######################################################################
        top_banner_frame = ttk.Frame(self)
        top_banner_frame.pack(side=TOP, fill=X)

        # [UPDATED] Discogs logo on the left with click event
        if 'logo' in self.photoimages:
            banner = ttk.Label(top_banner_frame, image='logo')
            banner.pack(side=LEFT, padx=10, pady=5)
            # Open discogs.com on click
            banner.bind("<Button-1>", lambda e: self.open_url("https://www.discogs.com"))
        else:
            banner = ttk.Label(top_banner_frame, text="Discogs Data Processor", font=("Arial", 16, "bold"))
            banner.pack(side=LEFT, padx=10, pady=5)

        # Social media icons on the right
        social_frame = ttk.Frame(top_banner_frame)
        social_frame.pack(side=RIGHT, padx=10, pady=5)

        if 'linkedin' in self.photoimages:
            btn_linkedin = ttk.Button(
                social_frame,
                image='linkedin',
                bootstyle=LINK,
                command=lambda: self.open_url("https://www.linkedin.com/in/ofurkancoban/")
            )
            btn_linkedin.pack(side=LEFT, padx=2)

        if 'github' in self.photoimages:
            btn_github = ttk.Button(
                social_frame,
                image='github',
                bootstyle=LINK,
                command=lambda: self.open_url("https://github.com/ofurkancoban")
            )
            btn_github.pack(side=LEFT, padx=2)

        if 'kaggle' in self.photoimages:
            btn_kaggle = ttk.Button(
                social_frame,
                image='kaggle',
                bootstyle=LINK,
                command=lambda: self.open_url("https://www.kaggle.com/ofurkancoban")
            )
            btn_kaggle.pack(side=LEFT, padx=2)

        #######################################################################
        # TOP BUTTON BAR
        #######################################################################
        buttonbar = ttk.Frame(self, style='primary.TFrame')
        buttonbar.pack(fill=X, pady=1, side=TOP)
        self.scroll_message_var.set('Log: Ready.')

        # 1. Fetch Data
        btn = ttk.Button(buttonbar, text='Fetch Data', image='fetch', compound=TOP, command=self.start_scraping)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # 3. Download
        btn = ttk.Button(buttonbar, text='Download', image='download', compound=TOP, command=self.download_selected)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # 4. Extract
        btn = ttk.Button(buttonbar, text='Extract', image='extract', compound=TOP, command=self.extract_selected)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # 5. Convert
        btn = ttk.Button(buttonbar, text='Convert', image='convert', compound=TOP, command=self.convert_selected)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # 6. Delete
        btn = ttk.Button(buttonbar, text='Delete', image='delete', compound=TOP, command=self.delete_selected)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        btn = ttk.Button(
            buttonbar,
            text='Cover Art',
            # you could pick an icon from your assets if desired, e.g. 'info' or 'settings'
            image='coverart',
            compound=TOP,
            command=self.open_coverart_window  # <--- We'll define this below
        )
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        # Settings
        btn = ttk.Button(
            buttonbar,
            text='Settings',
            image='settings',
            compound=TOP,
            command=self.open_settings  # Settings window
        )
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)


        # [UPDATED] Info Button (no scrollbar in the info window)
        btn = ttk.Button(
            buttonbar,
            text='Info',
            image='info',
            compound=TOP,
            command=self.open_info
        )
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

        #######################################################################
        # LEFT PANEL
        #######################################################################
        left_panel = ttk.Frame(self, style='bg.TFrame', width=250)
        left_panel.pack(side=LEFT, fill=BOTH, expand=True)
        left_panel.pack_propagate(False)

        ds_cf = CollapsingFrame(left_panel)
        ds_cf.pack(fill=BOTH, expand=True, pady=1)

        ds_frm = ttk.Frame(ds_cf, padding=0)
        ds_frm.columnconfigure(0, weight=1)
        ds_frm.rowconfigure(0, weight=1)
        ds_cf.add(child=ds_frm, title='Data Summary', bootstyle=SECONDARY)

        lbl = ttk.Label(ds_frm, text='Download Folder:', padding=(10, 0))  # Added left padding
        lbl.grid(row=0, column=0, sticky=W, pady=2)

        self.download_folder_label = ttk.Label(ds_frm, textvariable=self.download_dir_var, padding=(10, 0))
        self.download_folder_label.grid(row=1, column=0, sticky=W, padx=0, pady=2)

        lbl = ttk.Label(ds_frm, text='Size of Downloaded Files:', padding=(10, 0))
        lbl.grid(row=2, column=0, sticky=W, pady=2)

        self.downloaded_size_var = StringVar(value="Calculating...")
        lbl = ttk.Label(ds_frm, textvariable=self.downloaded_size_var, padding=(10, 0))
        lbl.grid(row=3, column=0, sticky=W, padx=0, pady=2)

        _func = self.open_discogs_folder
        open_btn = ttk.Button(ds_frm, text='Open Folder', command=_func,
                              image='opened-folder', compound=LEFT)
        open_btn.grid(row=5, column=0, columnspan=2, sticky=EW)

        # UI başlatılırken __init__ metodunda, örneğin sol panelin altına ekleyebilirsiniz:
        self.scrape_year_var = StringVar(value=str(datetime.now().year))
        years = [str(year) for year in range(2008, datetime.now().year + 1)]
        year_frame = ttk.Frame(self, padding=7)
        year_frame.pack(side=TOP, fill=X)
        ttk.Label(year_frame, text="Year:").pack(side=LEFT, padx=(0, 5))
        year_combobox = ttk.Combobox(year_frame, values=years, textvariable=self.scrape_year_var, width=6)
        year_combobox.pack(side=LEFT)
        year_combobox.bind("<<ComboboxSelected>>", self.on_year_change)

        # ─── AUTO MODE TOGGLE EKLE ───────────────────────────────
        self.auto_mode_var = BooleanVar(value=True)
        auto_toggle = ttk.Checkbutton(
            year_frame,
            text="Auto Mode",
            variable=self.auto_mode_var,
            bootstyle="round-toggle"  # ttkbootstrap'ta yuvarlak toggle için uygun bootstyle
        )
        auto_toggle.pack(side=RIGHT, padx=(10, 15))

        # Status panel
        status_cf = CollapsingFrame(left_panel)
        status_cf.pack(fill=BOTH, expand=True, pady=1)

        status_frm = ttk.Frame(status_cf, padding=0)
        status_frm.columnconfigure(0, weight=1)
        status_cf.add(child=status_frm, title='Status', bootstyle=SECONDARY)

        # Create status header frame with indicator
        status_header = ttk.Frame(status_frm)
        status_header.grid(row=0, column=0, columnspan=2, sticky=W, pady=(10,5), padx=10)

        # Add status indicator canvas
        self.status_indicator = ttk.Canvas(status_header, width=10, height=10)
        self.status_indicator.pack(side=LEFT, padx=(0,5))
        self.indicator_oval = self.status_indicator.create_oval(2, 2, 8, 8, fill='gray', outline='')

        # Status message next to indicator
        lbl = ttk.Label(status_header, textvariable=self.prog_message_var, font='Arial 11 bold')
        lbl.pack(side=LEFT)

        # Add progress bar back
        self.pb = ttk.Progressbar(status_frm, bootstyle="success-striped")
        self.pb.grid(row=1, column=0, columnspan=2, sticky=EW, padx=10, pady=5)

        self.prog_current_file_var = StringVar(value="File: none")
        lbl = ttk.Label(status_frm, textvariable=self.prog_current_file_var, padding=(10, 0))
        lbl.grid(row=2, column=0, columnspan=2, sticky=EW, pady=2)

        lbl = ttk.Label(status_frm, textvariable=self.prog_time_started_var, padding=(10, 0))
        lbl.grid(row=3, column=0, columnspan=2, sticky=EW, pady=2)
        self.prog_time_started_var.set('Not started')

        lbl = ttk.Label(status_frm, textvariable=self.prog_speed_var, padding=(10, 0))
        lbl.grid(row=4, column=0, columnspan=2, sticky=EW, pady=2)
        self.prog_speed_var.set('Speed: 0.00 MB/s')

        lbl = ttk.Label(status_frm, textvariable=self.prog_time_elapsed_var, padding=(10, 0))
        lbl.grid(row=5, column=0, columnspan=2, sticky=EW, pady=2)
        self.prog_time_elapsed_var.set('Elapsed: 0 sec')

        lbl = ttk.Label(status_frm, textvariable=self.prog_time_left_var, padding=(10, 0))
        lbl.grid(row=6, column=0, columnspan=2, sticky=EW, pady=2)
        self.prog_time_left_var.set('Left: 0 sec')

        stop_btn = ttk.Button(status_frm, command=self.stop_download, image='stop', text='Stop', compound=LEFT)
        stop_btn.grid(row=7, column=0, columnspan=2, sticky=EW)

        lbl_ver = ttk.Label(left_panel, text="v.1.2", style='bg.TLabel')
        lbl_ver.pack(side='bottom', anchor='center', pady=2)
        lbl_name = ttk.Label(left_panel, text="ofurkancoban", style='bg.TLabel')
        lbl_name.pack(side='bottom', anchor='center', pady=2)

        # Add avatar at the bottom (if exists)
        if 'avatar' in self.photoimages:
            lbl = ttk.Label(left_panel, image='avatar', style='bg.TLabel')
        else:
            lbl = ttk.Label(left_panel, text="Avatar", style='bg.TLabel')
        lbl.pack(side='bottom', anchor='center', pady=2)

        #######################################################################
        # RIGHT PANEL
        #######################################################################
        right_panel = ttk.Frame(self, padding=(2, 0))
        right_panel.pack(side=RIGHT, fill=BOTH, expand=NO)

        self.style = ttk.Style()
        self.style.configure(
            "Treeview.Heading",
            padding=(0, 11),
            font=("Arial", 13, "bold")
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
        self.scroll_message_var.set(_value)

        console_frame = ttk.Frame(output_container)
        console_frame.pack(fill=BOTH, expand=NO)
        console_frame.columnconfigure(0, weight=1)
        console_frame.rowconfigure(0, weight=1)

        # Create Text widget
        st = ttk.Text(console_frame, wrap='word', state='disabled', height=15)
        st.grid(row=0, column=0, sticky='nsew')

        # Create ttk Scrollbar
        console_scrollbar = ttk.Scrollbar(console_frame, orient='vertical', command=st.yview,
                                          style='Vertical.TScrollbar')
        console_scrollbar.grid(row=0, column=1, sticky='ns')

        # Configure Text widget to use the scrollbar
        st.configure(yscrollcommand=console_scrollbar.set)

        self.console_text = st

        scroll_cf.add(output_container, textvariable=self.scroll_message_var)

        self.tree = tv

        self.tree.bind("<Configure>", lambda e: self.position_checkbuttons())
        self.tree.bind("<Motion>", lambda e: self.position_checkbuttons())
        self.tree.bind("<ButtonRelease-1>", lambda e: self.position_checkbuttons())
        self.tree.bind("<<TreeviewSelect>>", lambda e: self.position_checkbuttons())
        right_panel.bind("<Configure>", lambda e: self.position_checkbuttons())

        self.log_to_console("Welcome to the Discogs Data Processor", "INFO")
        self.log_to_console("The application is fetching data automatically, please wait...", "INFO")

        # Start scraping after short delay
        self.after(100, self.start_scraping)
        self.update_downloaded_size()

    # -------------------------------------------------------------------------
    # NEW FUNCTION: OPEN COVERART WINDOW
    # -------------------------------------------------------------------------
    def scrape_years_from_html(url):
        """
        Verilen URL'deki HTML içeriğinden <a> etiketlerindeki 4 basamaklı yıl değerlerini (örn. "2021/") kazır.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
            html = response.text
            # <a ...>2021/</a> desenindeki yılı yakalar
            years = re.findall(r'<a[^>]*>(\d{4})/</a>', html)
            return sorted(set(years))
        except Exception as e:
            print(f"Error scraping years: {e}")
            return []

    def on_year_change(self, event):
        selected_year = self.scrape_year_var.get()
        self.log_to_console(f"Selected year: {selected_year}", "INFO")
        # Örneğin, S3'te "data/2021/" dizinini kullanarak dosyaları listeleyelim:
        target_prefix = f"data/{selected_year}/"
        self.update_files_for_year(target_prefix)
    def update_files_for_year(self, directory_prefix):
        """
        S3'te belirtilen prefix (ör: "data/2021/") ile ilgili dosyaları listeler,
        data_df'yi günceller ve tabloyu yeniden populate eder.
        """
        base_url = "https://discogs-data-dumps.s3.us-west-2.amazonaws.com/"
        try:
            # list_files_in_directory fonksiyonu S3'dan dosya bilgilerini getiriyor:
            data_df = list_files_in_directory(base_url, directory_prefix)
            if not data_df.empty:
                data_df["last_modified"] = pd.to_datetime(data_df["last_modified"])
                data_df["month"] = data_df["key"].apply(get_month_from_key)
                data_df = data_df[data_df["content"] != "checksum"]
                content_order = {"artists": 1, "labels": 2, "masters": 3, "releases": 4}
                data_df["content_order"] = data_df["content"].map(content_order)
                data_df = data_df.sort_values(by=["month", "content_order"], ascending=[False, True])
                data_df.drop(columns=["content_order"], inplace=True)
                data_df = self.mark_downloaded_files(data_df)
                self.data_df = data_df
                self.populate_table(data_df)
                self.save_to_file()
                self.log_to_console(f"{directory_prefix} files listed.", "INFO")
            else:
                self.log_to_console("File not found.", "WARNING")
        except Exception as e:
            self.log_to_console(f"update_files_for_year hatası: {e}", "ERROR")
    def open_coverart_window(self):
        """
        Opens a new Toplevel window to:
         1) Select an image
         2) Choose YEAR + spelled-out MONTH from combobox
         3) Select a .ttf font file (optional)
         4) Apply coverart => writes "YEAR - MONTH" to the image
         5) The output file is named after the selected MONTH and saved to ~/Discogs/Cover Arts
        """
        wm_win = ttk.Toplevel(self)
        wm_win.title("Create Cover Art")

        # Pencere boyutlarını belirleyin
        window_width = 500
        window_height = 500

        # Ekran boyutlarını alın
        screen_width = wm_win.winfo_screenwidth()
        screen_height = wm_win.winfo_screenheight()

        # Pencereyi ekranın ortasına yerleştirmek için koordinatları hesaplayın
        center_x = int((screen_width - window_width) / 2)
        center_y = int((screen_height - window_height) / 2)

        # Geometry ayarını güncelleyin (örneğin: "500x500+center_x+center_y")
        wm_win.geometry(f"{window_width}x{window_height}+{center_x}+{center_y}")
        wm_win.resizable(False, False)

        # Spelled-out months
        month_choices = [
            "JANUARY", "FEBRUARY", "MARCH", "APRIL",
            "MAY", "JUNE", "JULY", "AUGUST",
            "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER"
        ]

        # By default, pick the current year and spelled-out month
        now = datetime.now()
        default_year = str(now.year)
        default_month = month_choices[now.month - 1]

        # Tk variables for form fields
        self.wm_image_path = ttk.StringVar(value="")
        self.wm_font_path = ttk.StringVar(value="")
        self.wm_selected_year = ttk.StringVar(value=default_year)
        self.wm_selected_month = ttk.StringVar(value=default_month)

        # 1) Image file selection
        frm_top = ttk.Frame(wm_win, padding=10)
        frm_top.pack(fill="x", pady=5)
        lbl_img = ttk.Label(frm_top, text="Select Image:", width=12)
        lbl_img.pack(side="left")

        ent_img = ttk.Entry(frm_top, textvariable=self.wm_image_path, width=25)
        ent_img.pack(side="left", padx=5)

        def browse_image():
            filepath = filedialog.askopenfilename(
                title="Select an Image",
                filetypes=[
                    ("Image files", "*.jpg *.jpeg *.png *.bmp *.gif *.tiff *.webp"),
                    ("All files", "*.*")
                ]
            )
            if filepath:
                self.wm_image_path.set(filepath)

        btn_img = ttk.Button(frm_top, text="Browse", command=browse_image)
        btn_img.pack(side="left")

        # 2) Year & spelled-out Month
        frm_ym = ttk.Frame(wm_win, padding=10)
        frm_ym.pack(fill="x", pady=5)

        lbl_year = ttk.Label(frm_ym, text="Year:")
        lbl_year.grid(row=0, column=0, padx=5, sticky="e")
        now = datetime.now()
        cmb_year = ttk.Combobox(
            frm_ym,
            values=[str(y) for y in range(2008, now.year + 10)],
            textvariable=self.wm_selected_year,
            width=6
        )
        cmb_year.grid(row=0, column=1, padx=5, sticky="w")

        lbl_month = ttk.Label(frm_ym, text="Month:")
        lbl_month.grid(row=0, column=2, padx=5, sticky="e")
        cmb_month = ttk.Combobox(frm_ym, values=month_choices,
                                 textvariable=self.wm_selected_month, width=10)
        cmb_month.grid(row=0, column=3, padx=5, sticky="w")

        # 3) Font selection (optional)
        frm_font = ttk.Frame(wm_win, padding=10)
        frm_font.pack(fill="x", pady=5)

        lbl_font = ttk.Label(frm_font, text="Font (.ttf):")
        lbl_font.pack(side="left")

        ent_font = ttk.Entry(frm_font, textvariable=self.wm_font_path, width=25)
        ent_font.pack(side="left", padx=5)

        def browse_font():
            font_file = filedialog.askopenfilename(
                title="Select a TTF Font",
                filetypes=[("TrueType Font", "*.ttf"), ("All files", "*.*")]
            )
            if font_file:
                self.wm_font_path.set(font_file)

        btn_font = ttk.Button(frm_font, text="Browse", command=browse_font)
        btn_font.pack(side="left")

        # 4) Apply coverart => new file = selected month
        frm_btn = ttk.Frame(wm_win, padding=10)
        frm_btn.pack(fill="x", pady=10)



        # Assuming BASE_DIR is defined elsewhere, e.g.:
        # BASE_DIR = Path(__file__).parent

        def draw_text_with_drop_shadow(image, text, position, font, text_color, shadow_params):
            """
            Draws text onto an RGBA image with a drop shadow effect.

            Parameters:
              image         : PIL.Image (must be in RGBA mode)
              text          : The text string to be drawn
              position      : (x, y) tuple for the text's top-left position (will be converted to int)
              font          : PIL.ImageFont instance
              text_color    : RGBA tuple for the main text color, e.g. (255,255,255,255)
              shadow_params : Dictionary with keys:
                              - "angle": shadow angle in degrees (e.g. 30)
                              - "distance": shadow distance in pixels (e.g. 3)
                              - "spread": fraction (e.g. 0.20 for 20% of text size)
                              - "blur_radius": Gaussian blur radius in pixels (e.g. 22)
            """
            angle = shadow_params.get("angle", 30)
            distance = shadow_params.get("distance", 3)
            spread = shadow_params.get("spread", 0.2)
            blur_radius = shadow_params.get("blur_radius", 10)

            # Compute offset based on angle and distance
            dx = int(round(distance * math.cos(math.radians(angle))))
            dy = int(round(distance * math.sin(math.radians(angle))))

            # Convert the provided position to integers
            base_x, base_y = int(round(position[0])), int(round(position[1]))

            # Get the text bounding box using textbbox
            dummy_draw = ImageDraw.Draw(image)
            bbox = dummy_draw.textbbox((0, 0), text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]

            # Calculate spread pixels based on the larger dimension
            spread_pixels = int(max(text_width, text_height) * spread)

            # Create a grayscale mask for the text
            mask_size = (text_width + 2 * spread_pixels, text_height + 2 * spread_pixels)
            mask = Image.new("L", mask_size, 0)
            mask_draw = ImageDraw.Draw(mask)
            mask_draw.text((spread_pixels, spread_pixels), text, font=font, fill=255)

            # Apply spread (using MaxFilter) if needed
            if spread_pixels > 0:
                filter_size = spread_pixels * 2 + 1
                mask = mask.filter(ImageFilter.MaxFilter(filter_size))
            # Apply Gaussian blur for the drop shadow effect
            mask = mask.filter(ImageFilter.GaussianBlur(radius=blur_radius))

            # Create an image for the shadow using the mask
            shadow_color = (0, 0, 0, 255)
            shadow = Image.new("RGBA", mask_size, shadow_color)
            shadow.putalpha(mask)

            # Calculate shadow position; subtract spread_pixels and add the offset (dx, dy)
            shadow_position = (base_x - spread_pixels + dx, base_y - spread_pixels + dy)
            image.alpha_composite(shadow, dest=shadow_position)

            # Finally, draw the main text at the original (integer) position
            draw = ImageDraw.Draw(image)
            draw.text((base_x, base_y), text, font=font, fill=text_color)

        def apply_coverart():
            # Get the image path from the UI (assume self.wm_image_path is a StringVar)
            img_path = self.wm_image_path.get().strip()
            # If no valid image is selected, use the default 'cover_art.png' from the assets folder.
            if not img_path or not os.path.exists(img_path):
                default_img = BASE_DIR / 'assets' / 'cover_art.png'
                if default_img.exists():
                    img_path = str(default_img)
                else:
                    messagebox.showerror("Error", "No image selected and default image not found!")
                    return

            font_path = self.wm_font_path.get().strip()
            year_val = self.wm_selected_year.get()
            month_val = self.wm_selected_month.get()  # e.g. "JANUARY"

            # Prepare the two lines: year on top, month below.
            year_text = year_val
            month_text = month_val

            # Font sizes
            year_font_size = 150
            month_font_size = 150

            # Load the TTF font if provided, or try default fonts.
            try:
                if font_path and os.path.exists(font_path):
                    year_font = ImageFont.truetype(font_path, size=year_font_size)
                    month_font = ImageFont.truetype(font_path, size=month_font_size)
                else:
                    try:
                        year_font = ImageFont.truetype("assets/dreamorphanagehv-regular.otf", size=year_font_size)
                        month_font = ImageFont.truetype("assets/dreamorphanagehv-regular.otf", size=month_font_size)
                    except Exception:
                        year_font = ImageFont.load_default()
                        month_font = ImageFont.load_default()
            except Exception as e:
                self.log_to_console(f"Error loading font: {e}", "ERROR")
                messagebox.showerror("Font Error", f"Could not load font:\n{e}")
                return

            try:
                with Image.open(img_path) as img:
                    # Ensure image is in RGBA mode for transparency support.
                    if img.mode != "RGBA":
                        img = img.convert("RGBA")

                    # Set up drop shadow parameters:
                    shadow_params = {
                        "angle": 30,  # 30 degrees
                        "distance": 1,  # 3px
                        "spread": 0.002,  # 20% spread
                        "blur_radius": 10  # 22px blur (size)
                    }
                    text_color = (255, 255, 255, 255)  # White

                    # Create a drawing object to compute text sizes.
                    draw = ImageDraw.Draw(img)
                    year_bbox = draw.textbbox((0, 0), year_text, font=year_font)
                    year_width = year_bbox[2] - year_bbox[0]
                    year_height = year_bbox[3] - year_bbox[1]
                    month_bbox = draw.textbbox((0, 0), month_text, font=month_font)
                    month_width = month_bbox[2] - month_bbox[0]
                    month_height = month_bbox[3] - month_bbox[1]

                    line_spacing = 50  # Spacing between the two lines
                    total_text_height = year_height + line_spacing + month_height
                    block_width = max(year_width, month_width)

                    # Calculate starting coordinates to center the text block,
                    # and add a vertical offset (y_offset) to move the text downward.
                    y_offset = 150  # Adjust this value as needed
                    x_start = (img.width - block_width) / 2
                    y_start = (img.height - total_text_height) / 2 + y_offset

                    # Draw each line with the drop shadow effect.
                    draw_text_with_drop_shadow(
                        image=img,
                        text=year_text,
                        position=(x_start + (block_width - year_width) / 2, y_start),
                        font=year_font,
                        text_color=text_color,
                        shadow_params=shadow_params
                    )
                    draw_text_with_drop_shadow(
                        image=img,
                        text=month_text,
                        position=(x_start + (block_width - month_width) / 2, y_start + year_height + line_spacing),
                        font=month_font,
                        text_color=text_color,
                        shadow_params=shadow_params
                    )

                    # Save the resulting image in the "Cover Arts" folder inside the Discogs folder.
                    discogs_folder = Path(self.download_dir_var.get())
                    cover_arts_folder = discogs_folder / "Cover Arts"
                    cover_arts_folder.mkdir(parents=True, exist_ok=True)
                    original_ext = os.path.splitext(img_path)[1]  # e.g., ".jpg" or ".png"
                    output_filename = f"{year_val}-{month_val.upper()}{original_ext}"
                    output_path = cover_arts_folder / output_filename

                    img.save(output_path)

                self.log_to_console(f"Cover art created(year & month) => {output_path}", "INFO")
                self.after(0, lambda: messagebox.showinfo("Cover Art", f"Cover Art image saved as:\n{output_path}"))
                self.after(0, wm_win.destroy)

            except Exception as e:
                self.log_to_console(f"Error creating image: {e}", "ERROR")
                messagebox.showerror("Image Error", f"Could not create cover art:\n{e}")

        btn_apply = ttk.Button(frm_btn, text="Apply", bootstyle="success", command=apply_coverart)
        btn_apply.pack(side="left", padx=5)

        btn_close = ttk.Button(frm_btn, text="Close", command=wm_win.destroy)
        btn_close.pack(side="left", padx=5)

    ###########################################################################
    #  EKLENDİ: Artık update_progress_bar sınıf içinde bir metot
    ###########################################################################
    def update_progress_bar(self, current_bytes, total_bytes):
        """Simple progress bar update, called during download."""
        if total_bytes > 0:
            percentage = (current_bytes / total_bytes) * 100
            self.pb['value'] = percentage
        else:
            self.pb['value'] = 0
        self.update_idletasks()

    def update_time_info(self, downloaded_size, total_size, start_time):
        """Update elapsed and left time for download progress."""
        elapsed = (datetime.now() - start_time).total_seconds()

        if elapsed > 0:
            speed = (downloaded_size / elapsed) / (1024 * 1024)  # MB/s
            self.after(0, self.prog_speed_var.set, f"Speed: {speed:.2f} MB/s")
            self.after(0, self.prog_time_elapsed_var.set, f"Elapsed: {int(elapsed) // 60} min {int(elapsed) % 60} sec")

            if downloaded_size > 0:
                percentage = (downloaded_size / total_size) * 100
                left = (total_size - downloaded_size) / (downloaded_size / elapsed) if downloaded_size > 0 else 0
                left_minutes = int(left // 60)
                left_seconds = int(left % 60)
                self.after(0, self.prog_time_left_var.set, f"Left: {left_minutes} min {left_seconds} sec")
                self.after(0, self.prog_message_var.set, f"Downloading: {percentage:.2f}%")

    def open_settings(self):
        """Allows the user to select a download folder and creates a Discogs folder.
           Then automatically starts the Fetch Data process."""
        chosen_dir = filedialog.askdirectory(
            title="Select Download Folder",
            initialdir=self.download_dir_var.get()
        )
        if chosen_dir:
            discogs_dir = Path(chosen_dir) / "Discogs"
            try:
                discogs_dir.mkdir(parents=True, exist_ok=True)
                self.download_dir_var.set(str(discogs_dir))
                self.log_to_console(f"Download folder changed to: {discogs_dir}", "INFO")
                self.update_downloaded_size()
                self.log_to_console("Fetching data automatically after changing download folder...", "INFO")
                self.start_scraping()
            except Exception as e:
                self.log_to_console(f"Error creating Discogs folder: {e}", "ERROR")
                messagebox.showerror("Error", f"Could not create Discogs folder: {e}")
        else:
            self.log_to_console("No folder selected. Keeping current setting.", "INFO")

    def open_info(self):
        """Opens an information window (no scrollbar)."""
        info_window = ttk.Toplevel(self)
        info_window.title("How to Use Discogs Data Processor")
        info_window.geometry("600x500")
        info_window.resizable(False, False)

        # Center the window
        window_width = 600
        window_height = 500

        screen_width = info_window.winfo_screenwidth()
        screen_height = info_window.winfo_screenheight()

        center_x = int((screen_width - window_width) / 2)
        center_y = int((screen_height - window_height) / 2)

        info_window.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')

        # Make the window modal
        info_window.grab_set()

        # Plain Text widget (no scrollbar)
        text_area = ttk.Text(info_window, wrap='word', font=("Arial", 12), height=30)
        text_area.pack(fill=BOTH, expand=True, padx=10, pady=10)
        text_area.config(state='normal')

        # Define tags for headings and normal text
        text_area.tag_configure("heading", font=("Arial", 16, "bold"), spacing1=10, spacing3=10)
        text_area.tag_configure("subheading", font=("Arial", 14, "bold"), spacing1=5, spacing3=5)
        text_area.tag_configure("normal", font=("Arial", 12), spacing1=2, spacing3=2)
        text_area.tag_configure("bullet", font=("Arial", 12), lmargin1=25, lmargin2=50)

        # Information text
        info_text = [
            ("Discogs Data Processor User Guide\n", "heading"),
            ("\nIntroduction\n", "subheading"),
            (
            "This application helps you automatically download, extract, and convert Discogs datasets to CSV format. With its user-friendly interface, you can easily manage the processes.\n",
            "normal"),
            ("\nGetting Started\n", "subheading"),
            ("1. Download Folder Selection:\n", "normal"),
            ("   - Click the Settings button to select your download folder.\n", "bullet"),
            ("   - An automatic Discogs folder is created in the selected folder.\n", "bullet"),
            ("   - By default, the ~/Downloads/Discogs folder is used.\n", "bullet"),
            ("\n2. Fetch Data:\n", "subheading"),
            ("   - When the application first opens, it automatically fetches the latest data.\n", "bullet"),
            ("   - You can also manually fetch data by clicking the Fetch Data button.\n", "bullet"),
            ("\nButton Functions\n", "subheading"),
            ("- Fetch Data: Fetches the latest datasets from Discogs S3.\n", "bullet"),
            ("- Download: Downloads the selected datasets.\n", "bullet"),
            ("- Extract: Extracts the downloaded .gz files to .xml format.\n", "bullet"),
            ("- Convert: Converts the .xml files to CSV.\n", "bullet"),
            ("- Delete: Deletes the selected files.\n", "bullet"),
            ("- Settings: Opens folder selection dialog.\n", "bullet"),
            ("- Info: Shows this user guide.\n", "bullet"),
            ("- Create Cover Art: Allows you to pick an image, a year/month, and place text.\n", "bullet"),
            ("\nUsage Steps\n", "subheading"),
            ("1. Setting the Download Folder:\n", "normal"),
            ("   - Click the Settings button.\n", "bullet"),
            ("   - Select a folder and the Discogs subfolder will be created.\n", "bullet"),
            ("\n2. Downloading Data:\n", "subheading"),
            ("   - Fetch data (automatically or via button).\n", "bullet"),
            ("   - Select rows in the table and press Download.\n", "bullet"),
            ("\n3. Extracting Data:\n", "subheading"),
            ("   - Select .gz files you downloaded and click Extract.\n", "bullet"),
            ("\n4. Converting Data:\n", "subheading"),
            ("   - Select .xml files and click Convert.\n", "bullet"),
            ("\n5. Deleting Files:\n", "subheading"),
            ("   - Select files and click Delete to remove them.\n", "bullet"),
            ("\nStatus Tracking\n", "subheading"),
            ("- Progress bar, speed, time metrics, etc.\n", "bullet"),
            ("\nSupport\n", "subheading"),
            ("- For issues, check the log or contact the developer via top-right icons.\n", "bullet"),
            ("\nThank You!\n", "subheading"),
            ("Happy working with the Discogs Data Processor!\n", "bullet")
        ]

        # Insert text with tags
        for text, tag in info_text:
            text_area.insert('end', text, tag)

        text_area.config(state='disabled')  # Make text read-only

        # Close button
        btn_close = ttk.Button(info_window, text="Close", command=info_window.destroy)
        btn_close.pack(pady=10)

    def open_url(self, url):
        """Open a given URL in the default web browser."""
        webbrowser.open_new_tab(url)

    # _scrape_data_s3 fonksiyonundaki ilgili kısım:
    def _scrape_data_s3(self):
        try:
            base_url = "https://discogs-data-dumps.s3.us-west-2.amazonaws.com/"
            prefix = "data/"
            self.log_to_console("Listing directories from S3...", "INFO")
            dirs = list_directories_from_s3(base_url, prefix)
            if not dirs:
                self.log_to_console("No directories found.", "WARNING")
                return

            # Kullanıcının seçtiği yılı al
            selected_year = self.scrape_year_var.get()  # Örneğin "2025"
            # Seçilen yıla uyan dizinleri filtrele (dizin isimlerinde yıl bilgisi varsa)
            filtered_dirs = [d for d in dirs if selected_year in d]
            if filtered_dirs:
                filtered_dirs.sort()
                target_dir = filtered_dirs[-1]
            else:
                dirs.sort()
                target_dir = dirs[-1]

            self.log_to_console(f"Selected directory: {target_dir}", "INFO")

            data_df = list_files_in_directory(base_url, target_dir)
            if not data_df.empty:
                data_df["last_modified"] = pd.to_datetime(data_df["last_modified"])
                data_df["month"] = data_df["key"].apply(get_month_from_key)
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
                self.log_to_console("No data found in the selected directory.", "WARNING")
        except requests.exceptions.RequestException as e:
            self.log_to_console(f"Network error: {e}", "ERROR")
        except Exception as e:
            self.log_to_console(f"Error: {e}", "ERROR")

    def populate_table(self, data_df):
        """Populates the table with updated data."""
        for cb in self.checkbuttons.values():
            cb.destroy()
        self.check_vars.clear()
        self.checkbuttons.clear()

        # Clear all rows in the treeview
        for row in self.tree.get_children():
            self.tree.delete(row)

        # Ensure the required columns are present
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

        self.position_checkbuttons()  # Recalculate checkbutton positions

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.scroll_message_var.set(f"→ {now}")

        self.update_downloaded_size()
        self.log_to_console("Table updated.", "INFO")

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
        """Delete selected files and their related files (gz, xml, csv)."""
        checked_items = [item for item, var in self.check_vars.items() if var.get() == 1]
        if not checked_items:
            self.log_to_console("No file selected for deletion!", "WARNING")
            return

        # Ask for confirmation
        count = len(checked_items)
        if not messagebox.askyesno(
            "Confirm Deletion",
            f"Are you sure you want to delete {count} selected item(s) and all their related files?"
        ):
            return

        # Keep track of all deleted files
        all_deleted_files = []
        deleted_folders = []

        for item in checked_items:
            values = self.tree.item(item, "values")
            month_val = values[1]
            content_val = values[2]

            # Find the corresponding row in data_df
            row_data = self.data_df[
                (self.data_df["month"] == month_val) &
                (self.data_df["content"] == content_val)
            ]

            if not row_data.empty:
                url = row_data["URL"].values[0]
                folder_name = row_data["month"].values[0]
                filename = os.path.basename(url)
                base_path = Path(self.download_dir_var.get()) / "Datasets" / folder_name / filename

                # Get the base name without any extensions
                base_name = filename.split('.')[0]

                # List of all possible related files with full paths
                related_files = [
                    base_path,  # original .gz file
                    base_path.with_suffix(''),  # file without extension
                    base_path.with_suffix('.xml'),  # .xml file
                    base_path.with_suffix('.xml.tmp'),  # temporary .xml file
                    base_path.parent / f"{base_name}.csv"  # .csv file
                ]

                # Delete chunk folder if it exists
                chunk_folder = base_path.parent / f"chunked_{content_val}"
                if chunk_folder.exists():
                    try:
                        shutil.rmtree(chunk_folder)
                        deleted_folders.append(chunk_folder.name)
                        self.log_to_console(f"Deleted chunk folder: {chunk_folder}", "INFO")
                    except Exception as e:
                        self.log_to_console(f"Error deleting chunk folder {chunk_folder}: {e}", "ERROR")

                # Delete all related files
                files_deleted = []
                for file_path in related_files:
                    if file_path.exists():
                        try:
                            file_path.unlink()
                            files_deleted.append(file_path.name)
                            all_deleted_files.append(file_path.name)
                            self.log_to_console(f"Deleted file: {file_path}", "INFO")
                        except Exception as e:
                            self.log_to_console(f"Error deleting {file_path}: {e}", "ERROR")

                # Reset status in data_df
                self.data_df.loc[self.data_df["URL"] == url, "Downloaded"] = "✖"
                self.data_df.loc[self.data_df["URL"] == url, "Extracted"] = "✖"
                self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✖"

        # Update the table display
        self.populate_table(self.data_df)

        # Update downloaded size
        self.update_downloaded_size()

        # Create detailed completion message
        if all_deleted_files or deleted_folders:
            completion_message = "Deletion Summary:\n"
            if all_deleted_files:
                completion_message += f"\nFiles deleted ({len(all_deleted_files)}):\n"
                completion_message += "\n".join(f"- {file}" for file in all_deleted_files)
            if deleted_folders:
                completion_message += f"\n\nFolders deleted ({len(deleted_folders)}):\n"
                completion_message += "\n".join(f"- {folder}" for folder in deleted_folders)

            # Log the detailed summary
            self.log_to_console(completion_message, "INFO")
            self.log_to_console("Table updated.", "INFO")

            # Show popup with summary
            messagebox.showinfo("Deletion Complete", completion_message)
        else:
            self.log_to_console("No files were found to delete", "WARNING")
            messagebox.showinfo("Deletion Complete", "No files were found to delete")

    def log_to_console(self, message, message_type="INFO"):
        """
        Logs a message to the console_text widget and saves it to a log file.
        Every other line alternates between white and light blue.
        """
        self.console_text.config(state='normal')
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"→ [{timestamp}] [{message_type.upper()}]: {message}\n"

        # Get current line count (excluding the new line we're about to add)
        current_line = int(self.console_text.index('end-1c').split('.')[0])

        # Configure tags for alternating colors
        self.console_text.tag_configure("even_line", foreground="white")
        self.console_text.tag_configure("odd_line", foreground="#63b4f4")

        # Apply tag based on line number
        tag = "even_line" if current_line % 2 == 0 else "odd_line"

        # Insert the message with appropriate color tag
        self.console_text.insert('end', formatted_message, tag)

        # Auto-scroll and update UI
        self.console_text.see('end')
        self.console_text.config(state='disabled')

        # Update scroll message with truncated content
        msg_short = message.strip()
        if len(msg_short) > 80:
            msg_short = msg_short[:80] + '...'
        self.scroll_message_var.set(f"Log: {msg_short}")

        # Save log to file
        try:
            log_path = Path(self.download_dir_var.get()) / "discogs_data.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, 'a', encoding='utf-8') as f:
                f.write(formatted_message)
        except Exception as e:
            print(f"Error saving log to file: {e}")

    def mark_downloaded_files(self, data_df):
        """Ensure columns 'Downloaded', 'Extracted', 'Processed' exist, defaulting to ✖,
           and check on disk if each file is present."""
        for col in ["Downloaded", "Extracted", "Processed"]:
            if col not in data_df.columns:
                data_df[col] = "✖"

        downloads_dir = Path(self.download_dir_var.get()) / "Datasets"
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
                    extracted_file = file_path.with_suffix('')
                    if extracted_file.exists() and extracted_file.suffix.lower() == ".xml":
                        extracted_status = "✔"
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

    def start_download(self, url, filename, folder_name):
        """
        Dosya indirme işlemini başlatır.
        """
        self.log_to_console(f"Starting download: {url}", "INFO")
        self.start_status_indicator()
        self.prog_message_var.set('Downloading...')
        self.prog_current_file_var.set(f"File: {filename}")
        self.pb["value"] = 0
        self.stop_flag = False

        def progress_callback(progress):
            if self.stop_flag:
                return
            self.pb["value"] = progress * 100
            self.prog_message_var.set(f'Downloading: {progress * 100:.1f}%')
            self.update_idletasks()

        def completion_callback(success):
            if success:
                self.log_to_console(f"Download completed: {filename}", "INFO")
                self.prog_message_var.set('Download completed')
            else:
                if self.stop_flag:
                    self.log_to_console(f"Download stopped: {filename}", "WARNING")
                    self.prog_message_var.set('Download stopped')
                else:
                    self.log_to_console(f"Download failed: {filename}", "ERROR")
                    self.prog_message_var.set('Download failed')
            self.stop_status_indicator()

        # Yeni bir thread başlat
        Thread(
            target=self.download_file,
            args=(url, filename, folder_name, progress_callback, completion_callback),
            daemon=True
        ).start()

    def download_file(self, url, filename, folder_name, progress_callback=None, completion_callback=None):
        """
        Dosyayı indirir ve ilerleme durumunu günceller.
        İndirme tamamlandığında completion_callback'i çağırır.
        Auto Mode açıksa, indirme tamamlandığında extract ve convert işlemlerini başlatır.
        """
        download_path = Path(self.download_dir_var.get()) / "Datasets" / folder_name
        download_path.mkdir(parents=True, exist_ok=True)
        file_path = download_path / filename

        try:
            self.log_to_console(f"Downloading from: {url}", "INFO")
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                downloaded_size = 0
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if self.stop_flag:
                            f.close()
                            file_path.unlink(missing_ok=True)
                            if completion_callback:
                                completion_callback(False)
                            return
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            if progress_callback and total_size:
                                progress_callback(downloaded_size / total_size)

            # İndirme başarılı oldu, durumu güncelle
            self.data_df.loc[self.data_df["URL"] == url, "Downloaded"] = "✔"
            self.populate_table(self.data_df)
            
            if completion_callback:
                completion_callback(True)
                
            # Auto Mode açıksa, extract ve convert işlemlerini başlat
            if self.auto_mode_var.get():
                self.log_to_console(f"Auto Mode: Download completed, starting extraction for {filename}", "INFO")
                
                # Extract işlemini başlat
                def auto_extract_callback(success):
                    if success:
                        output_path = file_path.with_suffix('')
                        self.log_to_console(f"Auto Mode: Extracted {file_path} → {output_path}", "INFO")
                        self.data_df.loc[self.data_df["URL"] == url, "Extracted"] = "✔"
                        self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✖"
                        self.populate_table(self.data_df)
                        
                        # Convert işlemini başlat
                        self.log_to_console(f"Auto Mode: Extraction completed, starting conversion for {output_path}", "INFO")
                        self.schedule_conversion(url, folder_name, output_path)
                    else:
                        self.log_to_console(f"Auto Mode: Error extracting {file_path}", "ERROR")
                
                # Dosya .gz uzantılı ise extract et
                if file_path.suffix.lower() == ".gz":
                    self.extract_gz_file_with_progress(file_path, auto_extract_callback)
                else:
                    self.log_to_console(f"Auto Mode: {file_path} is not a .gz file, skipping extraction", "WARNING")
                    
        except Exception as e:
            self.log_to_console(f"Error downloading {url}: {e}", "ERROR")
            import traceback
            self.log_to_console(f"Traceback: {traceback.format_exc()}", "ERROR")
            if completion_callback:
                completion_callback(False)

    def handle_download_status(self, q):
        try:
            status = q.get_nowait()
            if status == 'Download finished':
                self.populate_table(self.data_df)
                self.log_to_console("Download completed", "INFO")
            elif status == 'Download failed':
                self.log_to_console("Download failed", "ERROR")
            else:
                self.log_to_console(status, "ERROR")
        except queue.Empty:
            self.after(100, self.handle_download_status, q)

    def single_thread_download(self, url, filename, folder_name):
        """Single-threaded download implementation."""
        self.log_to_console("Switching to single-threaded download", "INFO")
        self.log_to_console("Reason: Server doesn't support partial downloads", "INFO")

        downloads_dir = Path(self.download_dir_var.get()) / "Datasets"
        target_dir = downloads_dir / folder_name
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / filename

        try:
            response = requests.get(url, stream=True)
            total_size = int(response.headers.get('content-length', 0))
            human_size = human_readable_size(total_size)

            self.log_to_console(f"File size: {human_size}", "INFO")
            self.log_to_console(f"Target path: {file_path}", "INFO")

            block_size = 1024 * 64
            self.pb["value"] = 0
            self.pb["maximum"] = total_size
            self.pb.update()

            self.prog_current_file_var.set(f"File: {filename}")

            start_time = datetime.now()
            self.prog_time_started_var.set(f'Started at: {start_time.strftime("%Y-%m-%d %H:%M:%S")}')
            downloaded_size = 0

            with open(file_path, "wb") as file:
                for data in response.iter_content(block_size):
                    if self.stop_flag:
                        ...
                        return
                    file.write(data)
                    downloaded_size += len(data)
                    self.pb["value"] = downloaded_size
                    self.pb.update()

                    elapsed = (datetime.now() - start_time).total_seconds()
                    speed = (downloaded_size / elapsed) / (1024 * 1024) if elapsed > 0 else 0.0
                    self.prog_speed_var.set(f'Speed: {speed:.2f} MB/s')

                    if total_size > 0 and downloaded_size > 0:
                        percentage = (downloaded_size / total_size) * 100
                        left = int(
                            (total_size - downloaded_size) / (downloaded_size / elapsed)) if downloaded_size > 0 else 0
                        left_minutes = left // 60
                        left_seconds = left % 60
                        self.prog_time_left_var.set(f'Left: {left_minutes} min {left_seconds} sec')
                        self.prog_message_var.set(f'Downloading: {percentage:.2f}%')

            self.prog_message_var.set('Idle...')
            self.log_to_console(f"{filename} successfully downloaded: {file_path}", "INFO")
            self.data_df.loc[self.data_df["URL"] == url, "Downloaded"] = "✔"
            self.data_df.loc[self.data_df["URL"] == url, "Extracted"] = "✖"
            self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✖"
            self.populate_table(self.data_df)
            self.update_downloaded_size()

            self.show_centered_popup(
                "Download Complete",
                f"{filename} successfully downloaded!",
                "info"
            )

        except Exception as e:
            ...

    def stop_download(self):
        self.stop_status_indicator()  # Stop blinking
        """Sets the stop flag to True to halt operations and cleans up any partial files."""
        self.stop_flag = True
        self.log_to_console("Operation Stopped. Cleaning up...", "WARNING")
        self.prog_message_var.set('Stopping...')

        downloads_dir = Path(self.download_dir_var.get()) / "Datasets"
        if downloads_dir.exists():
            for folder in downloads_dir.glob("*"):
                if folder.is_dir():
                    # Clean up chunk folders
                    chunk_folders = list(folder.glob("chunked_*"))
                    for chunk_folder in chunk_folders:
                        try:
                            shutil.rmtree(chunk_folder)
                            self.log_to_console(f"Cleaned up chunk folder: {chunk_folder}", "INFO")
                        except Exception as e:
                            self.log_to_console(f"Error cleaning up {chunk_folder}: {e}", "ERROR")

                    # Clean up partial files
                    for file in folder.glob("*"):
                        if file.name.endswith(('.part*', '.tmp')):
                            try:
                                file.unlink()
                                self.log_to_console(f"Cleaned up partial file: {file}", "INFO")
                            except Exception as e:
                                self.log_to_console(f"Error cleaning up {file}: {e}", "ERROR")

        self.prog_message_var.set('Idle...')
        self.pb["value"] = 0
        self.prog_current_file_var.set("File: none")
        self.prog_speed_var.set("Speed: 0.00 MB/s")
        self.prog_time_left_var.set("Left: 0 sec")
        self.prog_time_elapsed_var.set("Elapsed: 0 sec")
        self.prog_time_started_var.set("Not started")

    def download_selected(self):
        """
        Seçili dosyaları indirir.
        Auto Mode açıkken, her dosya için ayrı ayrı işlem yapar.
        """
        checked_items = [item for item, var in self.check_vars.items() if var.get() == 1]
        if not checked_items:
            messagebox.showwarning("Warning", "No file selected!")
            return

        # Her bir seçili dosya için işlem yap
        for item in checked_items:
            values = self.tree.item(item, "values")
            month_val = values[1]
            content_val = values[2]
            size_val = values[3]
            downloaded_val = values[4]
            extracted_val = values[5]
            processed_val = values[6]

            # Eğer dosya zaten indirilmişse, tekrar indirme
            if downloaded_val == "✔":
                self.log_to_console(f"File already downloaded: {month_val}/{content_val}", "INFO")
                continue

            # Dosya bilgilerini bul
            row_data = self.data_df[
                (self.data_df["month"] == month_val) &
                (self.data_df["content"] == content_val) &
                (self.data_df["size"] == size_val) &
                (self.data_df["Downloaded"] == downloaded_val) &
                (self.data_df["Extracted"] == extracted_val) &
                (self.data_df["Processed"] == processed_val)
            ]
            
            if row_data.empty:
                self.log_to_console(f"Could not find data for {month_val}/{content_val}", "ERROR")
                continue
                
            url = row_data["URL"].values[0]
            folder_name = row_data["month"].values[0]
            filename = os.path.basename(url)
            
            # Dosyayı indir
            self.log_to_console(f"Starting download for {filename}", "INFO")
            self.start_download(url, filename, folder_name)

    def extract_selected(self):
        """
        Seçili dosyaları çıkarır.
        Auto Mode açıkken, her dosya için ayrı ayrı işlem yapar.
        """
        checked_items = [item for item, var in self.check_vars.items() if var.get() == 1]
        if not checked_items:
            messagebox.showwarning("Warning", "No file selected!")
            return

        # Seçili dosyaları çıkar
        Thread(target=self.extract_selected_thread, args=(checked_items,), daemon=True).start()

    def convert_selected(self):
        """
        Seçili dosyaları CSV'ye dönüştürür.
        Auto Mode açıkken, her dosya için ayrı ayrı işlem yapar.
        """
        checked_items = [item for item, var in self.check_vars.items() if var.get() == 1]
        if not checked_items:
            messagebox.showwarning("Warning", "No file selected!")
            return

        # Her bir seçili dosya için işlem yap
        for item in checked_items:
            values = self.tree.item(item, "values")
            month_val = values[1]
            content_val = values[2]
            size_val = values[3]
            downloaded_val = values[4]
            extracted_val = values[5]
            processed_val = values[6]

            # Eğer dosya çıkarılmamışsa, dönüştürme
            if extracted_val != "✔":
                self.log_to_console(f"File not extracted, cannot convert: {month_val}/{content_val}", "WARNING")
                continue

            # Dosya bilgilerini bul
            row_data = self.data_df[
                (self.data_df["month"] == month_val) &
                (self.data_df["content"] == content_val) &
                (self.data_df["size"] == size_val) &
                (self.data_df["Downloaded"] == downloaded_val) &
                (self.data_df["Extracted"] == extracted_val) &
                (self.data_df["Processed"] == processed_val)
            ]
            
            if row_data.empty:
                self.log_to_console(f"Could not find data for {month_val}/{content_val}", "ERROR")
                continue
                
            url = row_data["URL"].values[0]
            folder_name = row_data["month"].values[0]
            filename = os.path.basename(url)
            
            # Çıkarılmış dosya yolunu bul
            extracted_file = Path(self.download_dir_var.get()) / "Datasets" / folder_name / filename
            extracted_file = extracted_file.with_suffix("")  # .gz uzantısını kaldır
            
            if not extracted_file.exists():
                self.log_to_console(f"Extracted file not found: {extracted_file}", "ERROR")
                continue
                
            # Dönüştürme işlemini başlat
            self.log_to_console(f"Scheduling conversion for {extracted_file}", "INFO")
            self.schedule_conversion(url, folder_name, extracted_file)

    # ─── AUTO MODE İŞLEMLERİNİ YÖNETEN YENİ METOD ─────────────────
    def auto_mode_process(self, checked_items):
            """
            Auto Mode aktifken; seçili dosyalar için download, extract, chunking ve convert işlemlerini
            sırasıyla gerçekleştiren zincirleme işlemi yapar. Her adımın tamamlanmasını bekler ve
            tüm işlemler bittikten sonra bir popup ile kullanıcı bilgilendirilir.
            """
            # Seçili dosyalardan URL listesini elde edelim ve download işlemini başlatalım.
            urls = []
            for item in checked_items:
                values = self.tree.item(item, "values")
                month_val = values[1]
                content_val = values[2]
                size_val = values[3]
                row_data = self.data_df[
                    (self.data_df["month"] == month_val) &
                    (self.data_df["content"] == content_val) &
                    (self.data_df["size"] == size_val)
                    ]
                if not row_data.empty:
                    url = row_data["URL"].values[0]
                    urls.append(url)
                    folder_name = row_data["month"].values[0]
                    filename = os.path.basename(url)
                    self.start_download(url, filename, folder_name)
            self.log_to_console("Auto Mode: Download initiated. Waiting for downloads to complete...", "INFO")

            # Bekleme: Tüm seçili dosyaların download tamamlanmasını bekle
            while not self.stop_flag:
                all_downloaded = True
                for url in urls:
                    row = self.data_df[self.data_df["URL"] == url]
                    if not row.empty and row.iloc[0]["Downloaded"] != "✔":
                        all_downloaded = False
                        break
                if all_downloaded:
                    break
                time.sleep(1)
            if self.stop_flag:
                return

            self.log_to_console("Auto Mode: Downloads complete. Starting extraction...", "INFO")
            # Extraction işlemini tetikle (extract_selected metodu tüm seçili dosyaları işler)
            self.extract_selected()
            while not self.stop_flag:
                all_extracted = True
                for url in urls:
                    row = self.data_df[self.data_df["URL"] == url]
                    if not row.empty and row.iloc[0]["Extracted"] != "✔":
                        all_extracted = False
                        break
                if all_extracted:
                    break
                time.sleep(1)
            if self.stop_flag:
                return

            self.log_to_console("Auto Mode: Extraction complete. Starting conversion...", "INFO")
            # Conversion işlemini başlat (convert_selected metodu tüm seçili dosyaları işler)
            self.convert_selected()
            while not self.stop_flag:
                all_converted = True
                for url in urls:
                    row = self.data_df[self.data_df["URL"] == url]
                    if not row.empty and row.iloc[0]["Processed"] != "✔":
                        all_converted = False
                        break
                if all_converted:
                    break
                time.sleep(1)
            if self.stop_flag:
                return

            self.log_to_console("Auto Mode: All operations completed.", "INFO")
            self.after(0,
                       lambda: self.show_centered_popup("Auto Mode", "All operations completed successfully!", "info"))

    def extract_gz_file_with_progress(self, file_path: Path, callback):
        """
        Verilen .gz dosyasını çıkartır ve ilerleme durumunu UI'ye yansıtır.
        İşlem tamamlandığında veya iptal/hata durumunda callback(True) veya callback(False) çağrılır.
        """
        import time
        from datetime import datetime
        import queue

        output_path = file_path.with_suffix('')
        total_size = file_path.stat().st_size
        progress_queue = queue.Queue()
        start_time = datetime.now()
        self.prog_time_started_var.set(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        def extract_worker():
            try:
                temp_output_path = output_path.with_suffix('.xml.tmp')
                with gzip.open(file_path, 'rb') as f_in, open(temp_output_path, 'wb') as f_out:
                    while True:
                        if self.stop_flag:
                            progress_queue.put(('stopped', None))
                            return
                        chunk = f_in.read(1024 * 1024)  # 1 MB'lık parça
                        if not chunk:
                            break
                        f_out.write(chunk)
                        compressed_pos = f_in.fileobj.tell()
                        percent = (compressed_pos / total_size) * 100 if total_size else 0
                        progress_queue.put(('progress', percent))
                if temp_output_path.exists():
                    temp_output_path.rename(output_path)
                progress_queue.put(('done', None))
            except Exception as e:
                progress_queue.put(('error', str(e)))

        def update_progress():
            try:
                while True:
                    msg_type, value = progress_queue.get_nowait()
                    if msg_type == 'progress':
                        self.pb["value"] = value
                        self.prog_message_var.set(f'Extracting: {value:.1f}%')
                    elif msg_type == 'done':
                        self.pb["value"] = 100
                        self.prog_message_var.set('Extraction completed')
                    elif msg_type == 'stopped':
                        self.pb["value"] = 0
                        self.prog_message_var.set('Extraction stopped')
                        temp_output_path = output_path.with_suffix('.xml.tmp')
                        if temp_output_path.exists():
                            temp_output_path.unlink()
                            self.log_to_console(f"Deleted temporary file: {temp_output_path}", "INFO")
                        if output_path.exists():
                            output_path.unlink()
                            self.log_to_console(f"Deleted incomplete XML file: {output_path}", "INFO")
                        callback(False)
                        return
                    elif msg_type == 'error':
                        self.log_to_console(f"Error extracting {file_path}: {value}", "ERROR")
                        callback(False)
                        return
            except queue.Empty:
                pass

            elapsed = datetime.now() - start_time
            mins = int(elapsed.total_seconds()) // 60
            secs = int(elapsed.total_seconds()) % 60
            self.prog_time_elapsed_var.set(f"Elapsed: {mins} min {secs} sec")

            if not extraction_thread.is_alive():
                # Extraction thread tamamlandıysa (hata ya da iptal olmadıysa)
                callback(True)
            else:
                self.after(100, update_progress)

        extraction_thread = Thread(target=extract_worker)
        extraction_thread.start()
        update_progress()

    def get_auto_convert_items(self):
        """
        Auto Mode açıkken, treeview'de Downloaded sütununda "✔", Extracted sütununda "✔" olup
        Processed sütununda henüz "✔" olmayan öğeleri döner.
        """
        auto_items = []
        for item in self.tree.get_children():
            values = self.tree.item(item, "values")
            if values[4] == "✔" and values[5] == "✔" and values[6] != "✔":
                auto_items.append(item)
        return auto_items

    def get_auto_extract_items(self):
        """
        Auto Mode aktifken, treeview'de Downloaded sütununda "✔" olup Extracted sütununda henüz "✔" olmayan öğeleri döner.
        """
        auto_items = []
        for item in self.tree.get_children():
            values = self.tree.item(item, "values")
            # Değer sırası: ["", month, content, size, Downloaded, Extracted, Processed]
            if values[4] == "✔" and values[5] != "✔":
                auto_items.append(item)
        return auto_items

    def extract_selected_thread(self, checked_items):
        """
        Seçili .gz dosyalarını sırayla çıkarır.
        Auto Mode açıkken extraction tamamlandığında otomatik olarak convert işlemini başlatır.
        """
        self.start_status_indicator()
        self.prog_message_var.set('Extracting now...')
        extracted_files = []
        failed_files = []
        items_list = list(checked_items)

        def process_next_item():
            if not items_list:
                self.populate_table(self.data_df)
                if extracted_files:
                    message = f"{extracted_files[-1].name} successfully extracted"
                else:
                    message = "No files were extracted"
                # Sadece Auto Mode kapalıysa popup göster
                if not self.auto_mode_var.get():
                    self.show_centered_popup("Extraction Completed", message, "info")
                else:
                    self.log_to_console("Extraction completed (Auto Mode)", "INFO")
                self.stop_status_indicator()
                return

            item = items_list.pop(0)
            values = self.tree.item(item, "values")
            month_val = values[1]
            content_val = values[2]
            size_val = values[3]
            downloaded_val = values[4]
            extracted_val = values[5]
            processed_val = values[6]

            if downloaded_val != "✔":
                self.log_to_console("File not downloaded, cannot extract.", "WARNING")
                self.after(0, process_next_item)
                return

            row_data = self.data_df[
                (self.data_df["month"] == month_val) &
                (self.data_df["content"] == content_val) &
                (self.data_df["size"] == size_val) &
                (self.data_df["Downloaded"] == downloaded_val) &
                (self.data_df["Extracted"] == extracted_val) &
                (self.data_df["Processed"] == processed_val)
                ]
            if row_data.empty:
                self.after(0, process_next_item)
                return

            url = row_data["URL"].values[0]
            folder_name = row_data["month"].values[0]
            filename = os.path.basename(url)
            file_path = Path(self.download_dir_var.get()) / "Datasets" / folder_name / filename

            if file_path.suffix.lower() == ".gz":
                self.prog_current_file_var.set(f"File: {file_path.name}")

                def extraction_callback(success):
                    if success:
                        output_path = file_path.with_suffix('')
                        extracted_files.append(output_path)
                        self.log_to_console(f"Extracted: {file_path} → {output_path}", "INFO")
                        self.data_df.loc[self.data_df["URL"] == url, "Extracted"] = "✔"
                        self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✖"
                        
                        # Auto Mode açıksa, extract işleminden sonra convert işlemini başlat
                        if self.auto_mode_var.get():
                            self.log_to_console(f"Auto Mode: Scheduling conversion for {output_path}", "INFO")
                            # Conversion işlemini başlatmak için bir event planla
                            # Bu, UI thread'inin extract işlemini tamamlamasına izin verir
                            self.after(100, lambda: self.schedule_conversion(url, folder_name, output_path))
                    else:
                        failed_files.append(file_path)
                        self.log_to_console(f"Error or stopped extracting {file_path}.", "ERROR")
                        output_path = file_path.with_suffix('')
                        temp_path = output_path.with_suffix('.xml.tmp')
                        for cleanup_path in [output_path, temp_path]:
                            if cleanup_path.exists():
                                try:
                                    cleanup_path.unlink()
                                    self.log_to_console(f"Cleaned up: {cleanup_path}", "INFO")
                                except Exception as e:
                                    self.log_to_console(f"Error cleaning up {cleanup_path}: {e}", "ERROR")
                    self.after(0, process_next_item)

                self.extract_gz_file_with_progress(file_path, extraction_callback)
            else:
                if file_path.exists() and file_path.suffix.lower() == ".xml":
                    self.log_to_console(f"File {file_path} is already .xml; no extraction needed.", "INFO")
                    self.data_df.loc[self.data_df["URL"] == url, "Extracted"] = "✔"
                    self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✖"
                    
                    # Auto Mode açıksa ve dosya zaten XML ise, convert işlemini başlat
                    if self.auto_mode_var.get():
                        self.log_to_console(f"Auto Mode: Scheduling conversion for {file_path}", "INFO")
                        # Conversion işlemini başlatmak için bir event planla
                        self.after(100, lambda: self.schedule_conversion(url, folder_name, file_path))
                else:
                    self.log_to_console(f"{file_path} is not a .gz file.", "WARNING")
                self.after(0, process_next_item)

        process_next_item()

    def schedule_conversion(self, url, folder_name, extracted_file):
        """
        Conversion işlemini ayrı bir thread'de başlatır.
        Bu metod UI thread'inde çağrılır ve hemen döner.
        """
        self.log_to_console(f"Auto Mode: Launching conversion thread for {extracted_file}", "INFO")
        # Yeni bir process başlat (thread yerine)
        import multiprocessing
        
        # Önce gerekli parametreleri hazırla
        params = {
            'file_path': str(extracted_file),
            'url': url,
            'folder_name': folder_name,
            'download_dir': self.download_dir_var.get(),
            'content_type': None
        }
        
        # Content type'ı belirle
        row = self.data_df[self.data_df["URL"] == url]
        if not row.empty:
            params['content_type'] = row.iloc[0]["content"]
        else:
            # URL'den içerik türünü çıkarmaya çalış
            if "artist" in url.lower():
                params['content_type'] = "artists"
            elif "label" in url.lower():
                params['content_type'] = "labels"
            elif "master" in url.lower():
                params['content_type'] = "masters"
            elif "release" in url.lower():
                params['content_type'] = "releases"
        
        if not params['content_type']:
            self.log_to_console(f"Cannot determine content type for {extracted_file}", "ERROR")
            return
        
        # Conversion işlemini başlat
        self.log_to_console(f"Auto Mode: Starting conversion process for {extracted_file}", "INFO")
        
        # Conversion işlemini ayrı bir thread'de başlat
        conversion_thread = Thread(
            target=self._run_conversion_process,
            args=(params,),
            daemon=True
        )
        conversion_thread.start()
        
        # UI'ı güncelle
        self.prog_message_var.set(f"Converting {os.path.basename(extracted_file)}...")
        self.pb["value"] = 0

    def _run_conversion_process(self, params):
        """
        Conversion işlemini gerçekleştirir.
        Bu metod ayrı bir thread'de çalışır.
        """
        try:
            extracted_file = Path(params['file_path'])
            content_type = params['content_type']
            url = params['url']
            
            # UI güncellemesi için
            def update_ui(message, progress=None):
                self.after(0, lambda: self.prog_message_var.set(message))
                if progress is not None:
                    self.after(0, lambda: self.pb.configure(value=progress))
            
            update_ui("Chunking XML file...")
            
            # Chunking işlemini başlat
            try:
                chunk_xml_by_type(
                    extracted_file,
                    content_type=content_type,
                    records_per_file=10000,
                    logger=lambda msg, level: self.after(0, lambda: self.log_to_console(msg, level))
                )
                self.after(0, lambda: self.log_to_console(f"Auto Mode: Chunking completed successfully", "INFO"))
            except Exception as e:
                self.after(0, lambda: self.log_to_console(f"Auto Mode: Error during chunking: {str(e)}", "ERROR"))
                update_ui("Error during chunking")
                return
            
            chunk_folder = extracted_file.parent / f"chunked_{content_type}"
            if not chunk_folder.exists():
                self.after(0, lambda: self.log_to_console(f"Auto Mode: Chunk folder not created: {chunk_folder}", "ERROR"))
                update_ui("Chunk folder not created")
                return
                
            combined_csv = extracted_file.with_suffix(".csv")
            
            # Progress callback için bir queue oluştur
            progress_queue = queue.Queue()
            
            def progress_monitor():
                try:
                    while True:
                        try:
                            progress = progress_queue.get(block=False)
                            if progress is None:  # Sentinel value
                                break
                            pct = progress * 100
                            update_ui(f"Converting: {pct:.1f}%", pct)
                        except queue.Empty:
                            break
                        except Exception as e:
                            self.after(0, lambda: self.log_to_console(f"Progress monitor error: {e}", "ERROR"))
                    self.after(100, progress_monitor)  # Check again after 100ms
                except Exception as e:
                    self.after(0, lambda: self.log_to_console(f"Progress monitor thread error: {e}", "ERROR"))
            
            # Start progress monitor
            self.after(0, progress_monitor)
            
            def local_progress_cb(current_step, total_steps):
                try:
                    pct = current_step / total_steps if total_steps else 0
                    progress_queue.put(pct)
                except Exception as e:
                    self.after(0, lambda: self.log_to_console(f"Progress callback error: {e}", "ERROR"))
            
            update_ui("Converting chunks to CSV...")
            self.after(0, lambda: self.log_to_console(f"Auto Mode: Converting chunks to CSV: {chunk_folder}", "INFO"))
            
            # Conversion işlemini başlat
            try:
                convert_chunked_files_to_csv(
                    chunk_folder,
                    combined_csv,
                    content_type=content_type,
                    logger=lambda msg, level: self.after(0, lambda: self.log_to_console(msg, level)),
                    progress_cb=local_progress_cb
                )
                self.after(0, lambda: self.log_to_console(f"Auto Mode: Conversion completed successfully", "INFO"))
                # Stop progress monitor
                progress_queue.put(None)
            except Exception as e:
                self.after(0, lambda: self.log_to_console(f"Auto Mode: Error during conversion: {str(e)}", "ERROR"))
                update_ui("Error during conversion")
                # Stop progress monitor
                progress_queue.put(None)
                return
            
            # Chunk klasörünü temizle
            try:
                if chunk_folder.exists():
                    shutil.rmtree(chunk_folder, ignore_errors=True)
                    self.after(0, lambda: self.log_to_console(f"Auto Mode: Cleaned up chunk folder", "INFO"))
            except Exception as e:
                self.after(0, lambda: self.log_to_console(f"Auto Mode: Error cleaning up chunk folder: {str(e)}", "WARNING"))
            
            # Durumu güncelle
            if combined_csv.exists():
                # UI thread'inde güvenli bir şekilde güncelleme yap
                def update_status():
                    self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✔"
                    self.populate_table(self.data_df)
                    self.prog_message_var.set("Conversion completed")
                    self.log_to_console(f"Auto Mode: Successfully created {combined_csv}", "INFO")
                
                self.after(0, update_status)
            else:
                self.after(0, lambda: self.log_to_console(f"Auto Mode: CSV file not created: {combined_csv}", "ERROR"))
                update_ui("CSV file not created")
        except Exception as e:
            self.after(0, lambda: self.log_to_console(f"Auto Mode: Error in conversion process: {str(e)}", "ERROR"))
            import traceback
            self.after(0, lambda: self.log_to_console(f"Auto Mode: Traceback: {traceback.format_exc()}", "ERROR"))
            self.after(0, lambda: self.prog_message_var.set("Error in conversion"))

    def show_centered_popup(self, title, message, message_type="info"):
        if message_type == "info":
            messagebox.showinfo(title, message, parent=self)
        elif message_type == "warning":
            messagebox.showwarning(title, message, parent=self)
        elif message_type == "error":
            messagebox.showerror(title, message, parent=self)

    def handle_extract_status(self, q):
        try:
            status = q.get_nowait()
            if status == 'Extraction finished':
                self.populate_table(self.data_df)
                self.log_to_console("Extraction completed", "INFO")
            else:
                self.log_to_console(status, "ERROR")
        except queue.Empty:
            self.after(100, self.handle_extract_status, q)

    def get_folder_size(self, folder_path):
        total_size = 0
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.exists(file_path):
                    total_size += os.path.getsize(file_path)
        return total_size

    def update_downloaded_size(self):
        downloads_dir = Path(self.download_dir_var.get())
        if not downloads_dir.exists():
            self.downloaded_size_var.set("→ 0 MB")
            return
        size_in_bytes = self.get_folder_size(downloads_dir)
        one_gb = 1024 ** 3
        if size_in_bytes >= one_gb:
            size_in_gb = size_in_bytes // one_gb
            self.downloaded_size_var.set(f"→ {size_in_gb} GB")
        else:
            size_in_mb = size_in_bytes // (1024 ** 2)
            self.downloaded_size_var.set(f"→ {size_in_mb} MB")

    def open_discogs_folder(self):
        downloads_dir = Path(self.download_dir_var.get())
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
            downloads_dir = Path(self.download_dir_var.get())
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
                self.log_to_console("No directories found.", "WARNING")
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
        except requests.exceptions.RequestException as e:
            self.log_to_console(f"Network error: {e}", "ERROR")
        except Exception as e:
            self.log_to_console(f"Error: {e}", "ERROR")

    def start_status_indicator(self):
        """Start the blinking status indicator."""
        self.status_indicator_active = True
        self.status_indicator_visible = True
        
        # Cancel any existing blink timer
        if self.blink_after_id:
            self.after_cancel(self.blink_after_id)
            
        # Start blinking immediately
        self.blink_status_indicator()
        
    def stop_status_indicator(self):
        """Stop the blinking status indicator."""
        self.status_indicator_active = False
        
        # Cancel any existing blink timer
        if self.blink_after_id:
            self.after_cancel(self.blink_after_id)
            
        # Set to green when stopped
        self.status_indicator.itemconfig(self.indicator_oval, fill='green')
        
    def blink_status_indicator(self):
        """Blink the status indicator between blue and light blue."""
        if not self.status_indicator_active:
            return
            
        if self.status_indicator_visible:
            self.status_indicator.itemconfig(self.indicator_oval, fill='#007bff')  # Blue
        else:
            self.status_indicator.itemconfig(self.indicator_oval, fill='#63b4f4')  # Light blue
            
        self.status_indicator_visible = not self.status_indicator_visible
        
        # Schedule the next blink and store the ID
        self.blink_after_id = self.after(500, self.blink_status_indicator)  # Blink every 500ms

    def convert_single_file(self, url, folder_name, extracted_file):
        """
        Tek bir XML dosyasını CSV'ye dönüştürür.
        Auto Mode için kullanılır.
        """
        self.log_to_console(f"Auto Mode: Starting convert_single_file for {extracted_file}", "INFO")
        try:
            # Dosya türünü belirle (artists, labels, releases, masters)
            content_type = None
            row = self.data_df[self.data_df["URL"] == url]
            if not row.empty:
                content_type = row.iloc[0]["content"]
                self.log_to_console(f"Auto Mode: Content type from data_df: {content_type}", "INFO")
            else:
                # URL'den içerik türünü çıkarmaya çalış
                if "artist" in url.lower():
                    content_type = "artists"
                elif "label" in url.lower():
                    content_type = "labels"
                elif "master" in url.lower():
                    content_type = "masters"
                elif "release" in url.lower():
                    content_type = "releases"
                else:
                    self.log_to_console(f"Cannot determine content type for {extracted_file}", "ERROR")
                    return
                self.log_to_console(f"Auto Mode: Content type from URL: {content_type}", "INFO")
            
            # Dosyanın varlığını kontrol et
            if not extracted_file.exists():
                self.log_to_console(f"Auto Mode: File does not exist: {extracted_file}", "ERROR")
                return
                
            self.log_to_console(f"Auto Mode: Chunking XML by type: {extracted_file}", "INFO")
            
            # Chunking işlemini başlat
            try:
                chunk_xml_by_type(
                    extracted_file,
                    content_type=content_type,
                    records_per_file=10000,
                    logger=self.log_to_console
                )
                self.log_to_console(f"Auto Mode: Chunking completed successfully", "INFO")
            except Exception as e:
                self.log_to_console(f"Auto Mode: Error during chunking: {str(e)}", "ERROR")
                return
            
            chunk_folder = extracted_file.parent / f"chunked_{content_type}"
            if not chunk_folder.exists():
                self.log_to_console(f"Auto Mode: Chunk folder not created: {chunk_folder}", "ERROR")
                return
                
            combined_csv = extracted_file.with_suffix(".csv")
            
            def local_progress_cb(current_step, total_steps):
                pct = (current_step / total_steps) * 100 if total_steps else 0
                self.prog_message_var.set(f"Converting: {pct:.1f}%")
                self.pb["value"] = pct
                self.update_idletasks()  # UI'ı güncelle
            
            self.log_to_console(f"Auto Mode: Converting chunks to CSV: {chunk_folder}", "INFO")
            
            # Conversion işlemini başlat
            try:
                convert_chunked_files_to_csv(
                    chunk_folder,
                    combined_csv,
                    content_type=content_type,
                    logger=self.log_to_console,
                    progress_cb=local_progress_cb
                )
                self.log_to_console(f"Auto Mode: Conversion completed successfully", "INFO")
            except Exception as e:
                self.log_to_console(f"Auto Mode: Error during conversion: {str(e)}", "ERROR")
                return
            
            # Chunk klasörünü temizle
            try:
                if chunk_folder.exists():
                    shutil.rmtree(chunk_folder, ignore_errors=True)
                    self.log_to_console(f"Auto Mode: Cleaned up chunk folder", "INFO")
            except Exception as e:
                self.log_to_console(f"Auto Mode: Error cleaning up chunk folder: {str(e)}", "WARNING")
            
            # Durumu güncelle
            if combined_csv.exists():
                self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✔"
                self.populate_table(self.data_df)
                self.log_to_console(f"Auto Mode: Successfully created {combined_csv}", "INFO")
            else:
                self.log_to_console(f"Auto Mode: CSV file not created: {combined_csv}", "ERROR")
        except Exception as e:
            self.log_to_console(f"Auto Mode: Error in convert_single_file: {str(e)}", "ERROR")
            import traceback
            self.log_to_console(f"Auto Mode: Traceback: {traceback.format_exc()}", "ERROR")

    def convert_single_file_thread(self, url, folder_name, extracted_file):
        """
        Tek bir XML dosyasını CSV'ye dönüştürür.
        Auto Mode için kullanılır ve ayrı bir thread'de çalışır.
        """
        self.log_to_console(f"Auto Mode: Starting convert_single_file_thread for {extracted_file}", "INFO")
        try:
            # Dosya türünü belirle (artists, labels, releases, masters)
            content_type = None
            row = self.data_df[self.data_df["URL"] == url]
            if not row.empty:
                content_type = row.iloc[0]["content"]
                self.log_to_console(f"Auto Mode: Content type from data_df: {content_type}", "INFO")
            else:
                # URL'den içerik türünü çıkarmaya çalış
                if "artist" in url.lower():
                    content_type = "artists"
                elif "label" in url.lower():
                    content_type = "labels"
                elif "master" in url.lower():
                    content_type = "masters"
                elif "release" in url.lower():
                    content_type = "releases"
                else:
                    self.log_to_console(f"Cannot determine content type for {extracted_file}", "ERROR")
                    return
                self.log_to_console(f"Auto Mode: Content type from URL: {content_type}", "INFO")
            
            # Dosyanın varlığını kontrol et
            if not extracted_file.exists():
                self.log_to_console(f"Auto Mode: File does not exist: {extracted_file}", "ERROR")
                return
                
            self.log_to_console(f"Auto Mode: Chunking XML by type: {extracted_file}", "INFO")
            
            # UI güncellemesi için
            def update_ui(message, progress=None):
                self.after(0, lambda: self.prog_message_var.set(message))
                if progress is not None:
                    self.after(0, lambda: self.pb.configure(value=progress))
            
            update_ui("Chunking XML file...")
            
            # Chunking işlemini başlat
            try:
                chunk_xml_by_type(
                    extracted_file,
                    content_type=content_type,
                    records_per_file=10000,
                    logger=self.log_to_console
                )
                self.log_to_console(f"Auto Mode: Chunking completed successfully", "INFO")
            except Exception as e:
                self.log_to_console(f"Auto Mode: Error during chunking: {str(e)}", "ERROR")
                update_ui("Error during chunking")
                return
            
            chunk_folder = extracted_file.parent / f"chunked_{content_type}"
            if not chunk_folder.exists():
                self.log_to_console(f"Auto Mode: Chunk folder not created: {chunk_folder}", "ERROR")
                update_ui("Chunk folder not created")
                return
                
            combined_csv = extracted_file.with_suffix(".csv")
            
            def local_progress_cb(current_step, total_steps):
                pct = (current_step / total_steps) * 100 if total_steps else 0
                update_ui(f"Converting: {pct:.1f}%", pct)
            
            update_ui("Converting chunks to CSV...")
            self.log_to_console(f"Auto Mode: Converting chunks to CSV: {chunk_folder}", "INFO")
            
            # Conversion işlemini başlat
            try:
                convert_chunked_files_to_csv(
                    chunk_folder,
                    combined_csv,
                    content_type=content_type,
                    logger=self.log_to_console,
                    progress_cb=local_progress_cb
                )
                self.log_to_console(f"Auto Mode: Conversion completed successfully", "INFO")
            except Exception as e:
                self.log_to_console(f"Auto Mode: Error during conversion: {str(e)}", "ERROR")
                update_ui("Error during conversion")
                return
            
            # Chunk klasörünü temizle
            try:
                if chunk_folder.exists():
                    shutil.rmtree(chunk_folder, ignore_errors=True)
                    self.log_to_console(f"Auto Mode: Cleaned up chunk folder", "INFO")
            except Exception as e:
                self.log_to_console(f"Auto Mode: Error cleaning up chunk folder: {str(e)}", "WARNING")
            
            # Durumu güncelle
            if combined_csv.exists():
                # UI thread'inde güvenli bir şekilde güncelleme yap
                def update_status():
                    self.data_df.loc[self.data_df["URL"] == url, "Processed"] = "✔"
                    self.populate_table(self.data_df)
                    self.prog_message_var.set("Conversion completed")
                    self.log_to_console(f"Auto Mode: Successfully created {combined_csv}", "INFO")
                
                self.after(0, update_status)
            else:
                self.log_to_console(f"Auto Mode: CSV file not created: {combined_csv}", "ERROR")
                update_ui("CSV file not created")
        except Exception as e:
            self.log_to_console(f"Auto Mode: Error in convert_single_file_thread: {str(e)}", "ERROR")
            import traceback
            self.log_to_console(f"Auto Mode: Traceback: {traceback.format_exc()}", "ERROR")
            self.after(0, lambda: self.prog_message_var.set("Error in conversion"))

    def extract_gz_file_with_progress(self, gz_file_path, completion_callback=None):
        """Extract a .gz file with progress updates."""
        self.log_to_console(f"Extracting: {gz_file_path}", "INFO")
        self.start_status_indicator()  # Start the status indicator
        
        # Set start time
        start_time = datetime.now()
        self.prog_time_started_var.set(f'Started at: {start_time.strftime("%Y-%m-%d %H:%M:%S")}')
        
        # Reset progress metrics
        self.prog_speed_var.set('Speed: 0.00 MB/s')
        self.prog_time_elapsed_var.set('Elapsed: 0 sec')
        self.prog_time_left_var.set('Left: 0 sec')
        
        output_path = gz_file_path.with_suffix('')
        self.prog_message_var.set('Extracting...')
        self.prog_current_file_var.set(f"File: {gz_file_path.name}")
        
        try:
            # Get file size
            total_size = gz_file_path.stat().st_size
            
            # Open files
            with gzip.open(gz_file_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    # Read and write in chunks
                    chunk_size = 1024 * 1024  # 1MB chunks
                    extracted_size = 0
                    
                    while True:
                        chunk = f_in.read(chunk_size)
                        if not chunk:
                            break
                        
                        f_out.write(chunk)
                        extracted_size += len(chunk)
                        
                        # Update progress
                        if total_size > 0:
                            percentage = (extracted_size / total_size) * 100
                            self.pb['value'] = percentage
                            self.prog_message_var.set(f'Extracting: {percentage:.2f}%')
                        
                        # Update speed and time metrics
                        elapsed = (datetime.now() - start_time).total_seconds()
                        if elapsed > 0:
                            speed = (extracted_size / elapsed) / (1024 * 1024)  # MB/s
                            self.prog_speed_var.set(f"Speed: {speed:.2f} MB/s")
                            self.prog_time_elapsed_var.set(f"Elapsed: {int(elapsed) // 60} min {int(elapsed) % 60} sec")
                            
                            if extracted_size > 0 and total_size > 0:
                                left = (total_size - extracted_size) / (extracted_size / elapsed)
                                left_minutes = int(left // 60)
                                left_seconds = int(left % 60)
                                self.prog_time_left_var.set(f"Left: {left_minutes} min {left_seconds} sec")
                        
                        # Update UI
                        self.update_idletasks()
            
            self.log_to_console(f"Extraction completed: {output_path}", "INFO")
            self.prog_message_var.set('Extraction completed')
            
            # Update data_df
            url = None
            for idx, row in self.data_df.iterrows():
                folder_name = str(row["month"])
                filename = os.path.basename(row["URL"])
                file_path = Path(self.download_dir_var.get()) / "Datasets" / folder_name / filename
                if file_path == gz_file_path:
                    url = row["URL"]
                    break
                
            if url:
                self.data_df.loc[self.data_df["URL"] == url, "Extracted"] = "✔"
                self.populate_table(self.data_df)
            
            # Stop the status indicator
            self.stop_status_indicator()
            
            if completion_callback:
                completion_callback(True)
                
        except Exception as e:
            self.log_to_console(f"Error extracting {gz_file_path}: {e}", "ERROR")
            self.prog_message_var.set('Extraction failed')
            self.stop_status_indicator()
            
            if completion_callback:
                completion_callback(False)

    def download_file_with_progress(self, url, output_path, completion_callback=None):
        """Download a file with progress updates."""
        self.log_to_console(f"Downloading: {url}", "INFO")
        self.start_status_indicator()  # Start the status indicator
        
        # Set start time
        start_time = datetime.now()
        self.prog_time_started_var.set(f'Started at: {start_time.strftime("%Y-%m-%d %H:%M:%S")}')
        
        # Reset progress metrics
        self.prog_speed_var.set('Speed: 0.00 MB/s')
        self.prog_time_elapsed_var.set('Elapsed: 0 sec')
        self.prog_time_left_var.set('Left: 0 sec')
        
        self.prog_message_var.set('Downloading...')
        self.prog_current_file_var.set(f"File: {os.path.basename(url)}")
        
        try:
            # Create parent directory if it doesn't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Stream the download with progress updates
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Get total file size
            total_size = int(response.headers.get('content-length', 0))
            
            # Initialize variables for progress tracking
            downloaded_size = 0
            last_update_time = time.time()
            update_interval = 0.5  # Update UI every 0.5 seconds
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if self.stop_flag:
                        self.log_to_console("Download stopped by user", "WARNING")
                        if completion_callback:
                            completion_callback(False)
                        return False
                    
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Update UI at intervals to avoid too frequent updates
                        current_time = time.time()
                        if current_time - last_update_time >= update_interval:
                            # Update progress bar
                            if total_size > 0:
                                percentage = (downloaded_size / total_size) * 100
                                self.pb['value'] = percentage
                                self.prog_message_var.set(f'Downloading: {percentage:.2f}%')
                            
                            # Update speed and time metrics
                            elapsed = (datetime.now() - start_time).total_seconds()
                            if elapsed > 0:
                                # Calculate speed in MB/s
                                speed = (downloaded_size / elapsed) / (1024 * 1024)
                                self.prog_speed_var.set(f"Speed: {speed:.2f} MB/s")
                                
                                # Update elapsed time
                                elapsed_min = int(elapsed) // 60
                                elapsed_sec = int(elapsed) % 60
                                self.prog_time_elapsed_var.set(f"Elapsed: {elapsed_min} min {elapsed_sec} sec")
                                
                                # Estimate remaining time
                                if downloaded_size > 0 and total_size > 0:
                                    left = (total_size - downloaded_size) / (downloaded_size / elapsed)
                                    left_minutes = int(left // 60)
                                    left_seconds = int(left % 60)
                                    self.prog_time_left_var.set(f"Left: {left_minutes} min {left_seconds} sec")
                            
                            # Update UI
                            self.update_idletasks()
                            last_update_time = current_time
            
            self.log_to_console(f"Download completed: {output_path}", "INFO")
            self.prog_message_var.set('Download completed')
            
            # Stop the status indicator
            self.stop_status_indicator()
            
            if completion_callback:
                completion_callback(True)
            return True
            
        except Exception as e:
            self.log_to_console(f"Error downloading {url}: {e}", "ERROR")
            self.prog_message_var.set('Download failed')
            self.stop_status_indicator()
            
            if completion_callback:
                completion_callback(False)
            return False


def main():
    import sys
    empty_df = pd.DataFrame(columns=[
        "month", "content", "size", "last_modified", "key", "URL",
        "Downloaded", "Extracted", "Processed"
    ])
    empty_df["Downloaded"] = "✖"
    empty_df["Extracted"] = "✖"
    empty_df["Processed"] = "✖"

    app = ttk.Window("Discogs Data Processor", themename="darkly")
    primary_color = app.style.colors.primary

    icon_path = BASE_DIR / "assets" / "app_icon.png"
    if icon_path.exists():
        app.iconphoto(True, ttk.PhotoImage(file=icon_path))

    style = ttk.Style()
    style.configure("Treeview.Heading", background=primary_color, foreground="white")

    ui = DiscogsDataProcessorUI(app, empty_df)
    ui.pack(fill=BOTH, expand=True)

    window_width = 770
    window_height = 770

    screen_width = app.winfo_screenwidth()
    screen_height = app.winfo_screenheight()

    center_x = int((screen_width - window_width) / 2)
    center_y = int((screen_height - window_height) / 2)

    app.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
    app.resizable(False, False)

    app.mainloop()


if __name__ == "__main__":
    main()