<h1  align="center">🎵 Discogs Data Processor GUI Project 💿</h1>

<p align="center">
  <img src="assets/logo.png" alt="Discogs Data Processor Logo" width="200"/>
</p>

A powerful desktop application for efficiently managing Discogs data dumps. Download, extract, convert to CSV, and create custom cover arts via a modern interface.

## ✨ Key Features

- 🚀 Multi-threaded Downloads (8 threads)
- 📦 Smart `.gz` Extraction
- 🔄 Efficient XML → CSV conversion
- 📊 Real-time Progress (speed, ETA)
- 🎨 Dark Themed Modern UI
- 📝 Detailed Logging (color-coded)
- 💾 Custom Download Location
- 🖼️ Cover Art Generator
- ⚙️ Auto Mode (Download → Extract → Convert)

## 🖼️ Preview

<p align="center">
  <img src="img/UI.gif" alt="Application Interface"/>
</p>

## 🚀 Getting Started

### Prerequisites
- Python 3.7+
- `ttkbootstrap`, `pandas`, `requests`

### Installation

```bash
git clone https://github.com/ofurkancoban/discogs-data-processor.git
cd discogs-data-processor
pip install -r requirements.txt
python main.py
```

## 📖 How to Use

### Setup
- Click **Settings** to choose a download folder (default: `~/Downloads/Discogs`)

### Main Operations

1️⃣ **Fetch Data**: auto on startup or use **Fetch**  
2️⃣ **Download**: select files & click **Download**  
3️⃣ **Extract**: convert `.gz` to `.xml`  
4️⃣ **Convert**: convert `.xml` to `.csv`  
5️⃣ **Cover Art**: image + year/month → output  
6️⃣ **Manage Files**: delete, status, disk size

### ⚙️ Auto Mode

- Enable **Auto Mode**  
- Select rows → click **Download**  
- Automatically runs: Download → Extract → Convert  
- Perfect for batch automation!

## 📁 Folder Structure

```
Discogs/
├── Datasets/
│   ├── YYYY-MM/
│   │   ├── discogs_YYYY-MM-DD_type.xml.gz
│   │   ├── discogs_YYYY-MM-DD_type.xml
│   │   └── discogs_YYYY-MM-DD_type.csv
│   └── ...
├── Cover Arts/
└── discogs_data.csv
└── discogs_data.log
```

## 🔍 Technical Details

### Download Process
- Multi-threaded downloading (8 threads)
- Automatic fallback to single-thread
- Built-in retry mechanism
- Real-time progress tracking

### XML Processing
- Memory-efficient streaming parser
- Two-pass conversion:
  1. Column discovery
  2. Data extraction
- Chunking for large files

### Logging System
- Timestamp-based logging
- Color-coded messages
- Both UI and file logging
- Detailed operation tracking

## 🛠️ Tech Stack

- Python 3.7+
- `ttkbootstrap` (UI)
- `pandas`, `requests`

## 👤 Author

**Furkan Coban**
- LinkedIn: [ofurkancoban](https://www.linkedin.com/in/ofurkancoban/)
- GitHub: [ofurkancoban](https://github.com/ofurkancoban)
- Kaggle: [ofurkancoban](https://www.kaggle.com/ofurkancoban)


## 🙏 Thanks To

- Discogs (Data dumps)  
- ttkbootstrap (UI framework)  
- Icons8 (Icons)

## 📞 Support

- Check logs inside the app  
- Open an issue on GitHub  
- Reach out on LinkedIn

<p align="center">
  Made with ❤️ by ofurkancoban
</p>