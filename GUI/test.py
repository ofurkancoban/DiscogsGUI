from datetime import datetime
from pathlib import Path
from threading import Thread
import requests
import os
import pandas as pd
import platform
import subprocess

import ttkbootstrap as ttk
from ttkbootstrap import Style
from ttkbootstrap.constants import *
from ttkbootstrap.dialogs import Messagebox
from tkinter import messagebox
from tkinter.scrolledtext import ScrolledText

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from webdriver_manager.microsoft import EdgeChromiumDriverManager


PATH = Path(__file__).parent / 'assets'


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


class CollapsingFrame(ttk.Frame):
    """A collapsible frame widget that opens and closes with a click."""

    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.columnconfigure(0, weight=1)
        self.cumulative_rows = 0

        # widget images
        self.images = [
            ttk.PhotoImage(file=PATH/'icons8-double-up-30.png'),
            ttk.PhotoImage(file=PATH/'icons8-double-right-30.png')
        ]

    def add(self, child, title="", bootstyle=PRIMARY, **kwargs):
        if child.winfo_class() != 'TFrame':
            return

        frm = ttk.Frame(self, bootstyle=bootstyle)
        frm.grid(row=self.cumulative_rows, column=0, sticky=EW)

        # header title
        header = ttk.Label(master=frm, text=title, bootstyle=(bootstyle, INVERSE))
        if kwargs.get('textvariable'):
            header.configure(textvariable=kwargs.get('textvariable'))
        header.pack(side=LEFT, fill=BOTH, padx=10)

        # header toggle button
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


class DiscogsDownloaderUI(ttk.Frame):
    def __init__(self, master, data_df, **kwargs):
        super().__init__(master, **kwargs)
        self.pack(fill=BOTH, expand=YES)
        self.data_df = data_df
        self.stop_flag = False

        self.check_vars = {}
        self.checkbuttons = {}
        # Load an image for the banner
        self.banner_image = ttk.PhotoImage(file="assets/logo.png")  # Replace with your image file
        banner = ttk.Label(self, image=self.banner_image)
        banner.pack(side="top", fill="x")
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
            'delete' : 'icons8-trash-30.png'
        }

        self.photoimages = []
        imgpath = PATH
        for key, val in image_files.items():
            _path = imgpath / val
            if _path.exists():
                self.photoimages.append(ttk.PhotoImage(name=key, file=_path))

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

        _func = lambda: Messagebox.ok(message='Open Settings')
        btn = ttk.Button(buttonbar, text='Settings', image='settings', compound=TOP, command=_func)
        btn.pack(side=LEFT, ipadx=1, ipady=5, padx=1, pady=1)

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
        sep = ttk.Separator(ds_frm, bootstyle=SECONDARY)
        sep.grid(row=6, column=0, columnspan=2, pady=10, sticky=EW)

        _func = self.open_discogs_folder
        open_btn = ttk.Button(ds_frm, text='Open Folder', command=_func, bootstyle=LINK,
                              image='opened-folder', compound=LEFT)
        open_btn.grid(row=5, column=0, columnspan=2, sticky=W)

        status_cf = CollapsingFrame(left_panel)
        status_cf.pack(fill=BOTH, pady=1)

        status_frm = ttk.Frame(status_cf, padding=10)
        status_frm.columnconfigure(1, weight=1)
        status_cf.add(child=status_frm, title='Download Status', bootstyle=SECONDARY)

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

        right_panel = ttk.Frame(self, padding=(2, 0))
        right_panel.pack(side=RIGHT, fill=BOTH, expand=NO)
        # Stil nesnesini oluşturun
        self.style = ttk.Style()

        # Treeview başlık stilini ayarlayın
        self.style.configure(
            "Treeview.Heading",
            padding=(0, 11),  # Başlık yüksekliğini artırmak için padding'i ayarlayın
            font=("Arial", 13)  # Gerekirse font boyutunu ayarlayın
        )
        # İki satırlık bir düzen ile treeview ve log'u eşit boyda yapmak için grid kullandık.
        right_panel.columnconfigure(0, weight=1)
        right_panel.rowconfigure(0, weight=1)
        right_panel.rowconfigure(1, weight=1)

        tv = ttk.Treeview(right_panel, show='headings', height=16, style="Treeview")
        tv.configure(columns=(" ", "month", "content", "size", "Downloaded"))
        tv.column(" ", width=1,anchor=CENTER)
        tv.column("month", width=15, anchor=CENTER)
        tv.column("content", width=20, anchor=CENTER)
        tv.column("size", width=25, anchor=CENTER)
        tv.column("Downloaded", width=35, anchor=CENTER)

        # Create vertical scrollbar
        vsb = ttk.Scrollbar(right_panel, orient="vertical", command=tv.yview)
        tv.configure(yscrollcommand=vsb.set)

        # Place Treeview and scrollbar in the grid
        tv.grid(row=0, column=0, sticky='nsew', pady=1)
        vsb.grid(row=0, column=1, sticky='ns')
        buttonbar2 = ttk.Frame(self, style='primary.TFrame')
        buttonbar2.pack(fill=X, pady=1, side=BOTTOM)
        # Define a custom set method for the scrollbar
        def _scrollbar_set(first, last):
            vsb.set(first, last)
            self.position_checkbuttons()

        # Configure the treeview to use the custom set method
        tv.configure(yscrollcommand=_scrollbar_set)

        for col in tv["columns"]:
            tv.heading(col, text=col.capitalize(), anchor=CENTER)

        # Treeview grid yerleşimi
        tv.grid(row=0, column=0, sticky='nsew', pady=1)
        self.tree = tv

        # scrolling text output (log)
        scroll_cf = CollapsingFrame(right_panel)
        scroll_cf.grid(row=1, column=0, columnspan=2, sticky='nsew')


        output_container = ttk.Frame(scroll_cf, padding=0)
        _value = 'Log: Ready.'
        self.setvar('scroll-message', _value)

        # Create a frame to hold the Text widget and scrollbar
        console_frame = ttk.Frame(output_container)
        console_frame.pack(fill=BOTH, expand=NO)
        console_frame.columnconfigure(0, weight=1)
        console_frame.rowconfigure(0, weight=1)

        st = ttk.Text(console_frame, wrap='word', state='disabled', height=15)
        st.grid(row=0, column=0, sticky='nsew')
        self.console_text = st

        # Create the vertical scrollbar
        console_vsb = ttk.Scrollbar(console_frame, orient='vertical', command=st.yview)
        console_vsb.grid(row=0, column=1, sticky='ns')

        # Configure the Text widget to use the scrollbar
        st.configure(yscrollcommand=console_vsb.set)

        # Add the output_container to the collapsible frame
        scroll_cf.add(output_container, textvariable='scroll-message')

        # Bind the scroll events to update the Checkbutton positions
        self.tree.bind("<Configure>", lambda e: self.position_checkbuttons())
        self.tree.bind("<Motion>", lambda e: self.position_checkbuttons())
        self.tree.bind("<ButtonRelease-1>", lambda e: self.position_checkbuttons())
        self.tree.bind("<<TreeviewSelect>>", lambda e: self.position_checkbuttons())
        right_panel.bind("<Configure>", lambda e: self.position_checkbuttons())

        self.log_to_console("Welcome to the Discogs Data Downloader", "INFO")
        self.log_to_console("The application is fetching data automatically, please wait...", "INFO")
        self.after(100, self.start_scraping)

    def refresh_data(self):
        if self.data_df.empty:
            self.log_to_console("No data to refresh. Fetch data first.", "WARNING")
        else:
            self.populate_table(self.data_df)
            self.log_to_console("Data refreshed.", "INFO")

    def populate_table(self, data_df):
        # Destroy all existing Checkbuttons
        for cb in self.checkbuttons.values():
            cb.destroy()
        self.check_vars.clear()
        self.checkbuttons.clear()

        # Clear existing rows in the Treeview
        for row in self.tree.get_children():
            self.tree.delete(row)

        # Prepare color map for alternating row colors
        unique_months = data_df["month"].unique()
        color_map = {}
        for i, month in enumerate(unique_months):
            color_map[month] = "month1" if i % 2 == 0 else "month2"

        self.tree.tag_configure("month1", background="#343a40", foreground="#f8f9fa")
        self.tree.tag_configure("month2", background="#495057", foreground="#f8f9fa")

        # Add rows to the Treeview and create Checkbuttons
        for i, (_, row) in enumerate(data_df.iterrows()):
            # Insert row into Treeview
            tag = color_map.get(row["month"], "month1")
            values = ["", row["month"], row["content"], row["size"], row["Downloaded"]]
            item_id = self.tree.insert("", "end", values=values, tags=(tag,))

            # Create Checkbutton only if the row was successfully added
            if item_id:
                var = ttk.IntVar(value=0)
                cb = ttk.Checkbutton(self.tree, variable=var)
                self.check_vars[item_id] = var
                self.checkbuttons[item_id] = cb
            else:
                print(f"Skipped creating Checkbutton for row {i} due to invalid item_id")

        # Reposition Checkbuttons for visible rows
        self.position_checkbuttons()


        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.setvar('lastupdate', f"→ {now}")

    def position_checkbuttons(self):
        self.update_idletasks()

        if not self.checkbuttons:
            return

        # Get the visible rows in the treeview
        first_visible = self.tree.winfo_rooty()
        last_visible = first_visible + self.tree.winfo_height()

        for item_id, cb in list(self.checkbuttons.items()):
            bbox = self.tree.bbox(item_id, column=0)
            if not bbox:
                # If no bounding box, hide the checkbutton
                cb.place_forget()
                continue

            x, y, width, height = bbox
            cb_width = cb.winfo_reqwidth()
            cb_height = cb.winfo_reqheight()

            # Calculate checkbox position
            cb_x = x + (width - cb_width) // 2
            cb_y = y + (height - cb_height) // 2

            # Check if the item is actually visible in the treeview viewport
            item_top = self.tree.winfo_rooty() + y
            item_bottom = item_top + height

            if first_visible <= item_bottom and item_top <= last_visible:
                # Item is visible, place the checkbutton
                cb.place(in_=self.tree, x=cb_x, y=cb_y + 1, width=height - 2, height=height - 2)
            else:
                # Item is not visible, hide the checkbutton
                cb.place_forget()

    def delete_selected(self):
        """Delete the selected file(s) from the local disk and update the table."""
        # Get selected items
        checked_items = [item for item, var in self.check_vars.items() if var.get() == 1]
        if not checked_items:
            messagebox.showwarning("Warning", "No file selected!")
            return

        # Ask for confirmation before deleting
        confirm = messagebox.askyesno("Confirm Deletion", "Are you sure you want to delete the selected file(s)?")
        if not confirm:
            return  # Exit if the user cancels

        deleted_files = []
        failed_files = []

        # Iterate over selected items and delete files
        for item in checked_items:
            values = self.tree.item(item, "values")
            month_val = values[1]
            content_val = values[2]
            size_val = values[3]
            downloaded_val = values[4]

            # Find the corresponding row in the DataFrame
            row_data = self.data_df[
                (self.data_df["month"] == month_val) &
                (self.data_df["content"] == content_val) &
                (self.data_df["size"] == size_val) &
                (self.data_df["Downloaded"] == downloaded_val)
                ]
            if not row_data.empty:
                url = row_data["URL"].values[0]
                folder_name = row_data["month"].values[0]
                filename = os.path.basename(url)
                file_path = Path.home() / "Downloads" / "Discogs" / "Datasets" / folder_name / filename

                # Attempt to delete the file
                try:
                    if file_path.exists():
                        file_path.unlink()  # Delete the file
                        deleted_files.append(file_path)
                        self.data_df.loc[self.data_df["URL"] == url, "Downloaded"] = "✖"
                    else:
                        failed_files.append(file_path)
                except Exception as e:
                    self.log_to_console(f"Error deleting {file_path}: {e}", "ERROR")
                    failed_files.append(file_path)

        # Update the UI and log the results
        if deleted_files:
            self.log_to_console(f"Deleted files: {', '.join(map(str, deleted_files))}", "INFO")
        if failed_files:
            self.log_to_console(f"Failed to delete files: {', '.join(map(str, failed_files))}", "WARNING")

        # Refresh the table
        self.populate_table(self.data_df)

    def log_to_console(self, message, message_type="INFO"):
        # Enable the text widget to insert new text
        self.console_text.config(state='normal')

        # Format the message with timestamp and message type
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"→ [{timestamp}] [{message_type.upper()}]: {message}\n"

        # Insert the formatted message into the console text widget
        self.console_text.insert('end', formatted_message)

        # Disable the text widget to prevent editing
        self.console_text.config(state='disabled')

        # Scroll to the end of the text widget
        self.console_text.see('end')

        # Update the header with just the message content
        message_content = message.strip()

        # Truncate if necessary
        max_header_length = 80
        if len(message_content) > max_header_length:
            message_content = message_content[:max_header_length] + '...'

        # Update the 'scroll-message' variable
        self.setvar('scroll-message', f"Log: {message_content}")

    def mark_downloaded_files(self, data_df):
        downloads_dir = Path.home() / "Downloads" / "Discogs" / "Datasets"
        downloaded_status = []
        for _, row in data_df.iterrows():
            folder_name = str(row["month"])
            filename = os.path.basename(row["URL"])
            file_path = downloads_dir / folder_name / filename
            if file_path.exists():
                downloaded_status.append("✔")
            else:
                downloaded_status.append("✖")
        data_df["Downloaded"] = downloaded_status
        return data_df

    def download_file(self, url, filename, folder_name):
        try:
            self.setvar('prog-message', 'Downloading...')
            downloads_dir = Path.home() / "Downloads" / "Discogs" / "Datasets"
            target_dir = downloads_dir / folder_name
            target_dir.mkdir(parents=True, exist_ok=True)
            file_path = target_dir / filename

            response = requests.get(url, stream=True)
            total_size = int(response.headers.get('content-length', 0))
            block_size = 1024
            self.pb["value"] = 0
            self.pb["maximum"] = total_size

            start_time = datetime.now()
            self.setvar('prog-time-started', f'Started at: {start_time.strftime("%Y-%m-%d %H:%M:%S")}')
            downloaded_size = 0

            with open(file_path, "wb") as file:
                for data in response.iter_content(block_size):
                    if self.stop_flag:  # Check if stop button is pressed
                        self.setvar('prog-message', 'Idle...')
                        self.log_to_console("Download Stopped", "WARNING")
                        file.close()  # Close the file before deletion
                        if file_path.exists():
                            os.remove(file_path)  # Delete incomplete file
                            self.log_to_console(f"Incomplete file {file_path} deleted.", "WARNING")
                        return
                    file.write(data)
                    downloaded_size += len(data)
                    self.pb["value"] = downloaded_size
                    elapsed = (datetime.now() - start_time).total_seconds()

                    if elapsed > 0:
                        speed = (downloaded_size / elapsed) / (1024 * 1024)
                    else:
                        speed = 0.0
                        # Elapsed zamanı dönüştür
                    elapsed_minutes = int(elapsed) // 60
                    elapsed_seconds = int(elapsed) % 60
                    self.setvar('prog-speed', f'Speed: {speed:.2f} MB/s')
                    self.setvar('prog-time-elapsed', f'Elapsed: {elapsed_minutes} min {elapsed_seconds} sec')

                    if total_size > 0 and downloaded_size > 0:
                        percentage = (downloaded_size / total_size) * 100
                        left = int(
                            (total_size - downloaded_size) / (downloaded_size / elapsed)) if downloaded_size > 0 else 0
                        # Left zamanı dönüştür
                        left_minutes = left // 60
                        left_seconds = left % 60
                        self.setvar('prog-time-left', f'Left: {left_minutes} min {left_seconds} sec')
                        self.setvar('prog-message', f'Downloading {filename}: {percentage:.2f}%')
                        self.setvar('current-file-msg', f'Current file: {file_path}')

            self.setvar('prog-message', 'Idle...')
            self.log_to_console(f"{filename} successfully downloaded: {file_path}", "INFO")

            self.data_df.loc[self.data_df["URL"] == url, "Downloaded"] = "✔"
            self.populate_table(self.data_df)

        except Exception as e:
            self.log_to_console(f"Error: {e}", "ERROR")
            if file_path.exists():
                os.remove(file_path)  # Ensure incomplete files are deleted
                self.log_to_console(f"Incomplete file {file_path} deleted.", "WARNING")

    def start_download(self, url, filename, last_modified):
        self.stop_flag = False
        folder_name = last_modified.strftime("%Y-%m")
        Thread(target=self.download_file, args=(url, filename, folder_name), daemon=True).start()

    def stop_download(self):
        """Stop the ongoing download and delete incomplete files."""
        self.stop_flag = True
        self.log_to_console("Stopping the download and cleaning up...", "WARNING")

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
        Thread(target=self._scrape_data, daemon=True).start()

    def _scrape_data(self):
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
                data_df["month"] = data_df["last_modified"].dt.to_period("M").astype(str)
                content_order = {"artists": 1, "labels": 2, "masters": 3, "releases": 4}
                data_df = data_df[data_df["content"] != "checksum"]
                data_df["content_order"] = data_df["content"].map(content_order)
                data_df = data_df.sort_values(by=["month", "content_order"], ascending=[False, True])
                data_df.drop(columns=["content_order"], inplace=True)
                data_df = self.mark_downloaded_files(data_df)
                self.data_df = data_df
                self.populate_table(data_df)
                self.save_to_file()
                self.log_to_console("Scraping completed. Data saved automatically.", "INFO")
            else:
                self.log_to_console("No data found.", "WARNING")
        except Exception as e:
            self.log_to_console(f"Error: {e}", "ERROR")
        finally:
            driver.quit()


def main():
    empty_df = pd.DataFrame(columns=["month", "content", "size", "last_modified", "key", "URL", "Downloaded"])
    app = ttk.Window("Discogs Data Downloader", themename="darkly")
    # Customize the Treeview header style
    primary_color = app.style.colors.primary
    style = ttk.Style()
    style.configure("Treeview.Heading", background=primary_color,foreground="white", font=("Helvetica", 10, "bold"))

    DiscogsDownloaderUI(app, empty_df)
    # Ekran boyutunu ve konumunu ayarla
    window_width = 600
    window_height = 800

    # Ekran çözünürlüğünü al
    screen_width = app.winfo_screenwidth()
    screen_height = app.winfo_screenheight()

    # Pencereyi ekranın ortasına konumlandır
    center_x = int((screen_width - window_width) / 2)
    center_y = int((screen_height - window_height) / 2)

    # Pencere geometrisini ayarla
    app.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
    app.resizable(False,True)


    app.mainloop()

if __name__ == "__main__":
    main()
