<h1  align="center">🎵 Discogs Data Processor GUI Project 💿</h1>

<p align="center">
  <img src="assets/logo.png" alt="Discogs Data Processor Logo" width="200"/>
</p>

A powerful desktop application for efficiently managing Discogs data dumps. Download, extract, and convert Discogs datasets to CSV format with an intuitive user interface.

## ✨ Key Features

- 🚀 **Multi-threaded Downloads**: Utilizes 8 parallel threads for faster downloads
- 📦 **Smart Extraction**: Automatic .gz file extraction with progress tracking
- 🔄 **Efficient Conversion**: Streams XML to CSV with memory-efficient processing
- 📊 **Real-time Progress**: Live tracking of all operations with speed and time estimates
- 🎨 **Modern UI**: Clean, dark-themed interface with intuitive controls
- 📝 **Detailed Logging**: Comprehensive logging system with color-coded messages
- 💾 **Flexible Storage**: Customizable download location and organized file structure

## 🖼️ Application Preview

<p align="center">
  <img src="img/UI.gif" alt="Application Interface"/>
</p>

## 🚀 Getting Started

### Prerequisites

- Python 3.7 or higher
- Required Python packages:
```python
ttkbootstrap
pandas
requests
```

### Installation

1. Clone the repository:
```bash
git clone https://github.com/ofurkancoban/discogs-data-processor.git
cd discogs-data-processor
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Run the application:
```bash
python main.py
```

## 📖 How to Use

### Initial Setup
1. Launch the application
2. Click the Settings button to set your preferred download folder
   - Default: `~/Downloads/Discogs`
   - A Discogs folder will be automatically created

### Basic Operations

#### 1️⃣ Fetching Data
- Data is automatically fetched on startup
- Use "Fetch Data" button for manual updates
- View available Discogs datasets in the main table

#### 2️⃣ Downloading Files
- Select desired files using checkboxes
- Click "Download" to start multi-threaded download
- Monitor progress with real-time speed and time estimates

#### 3️⃣ Extracting Files
- Select downloaded files (.gz)
- Click "Extract" to convert to XML format
- Progress bar shows extraction status

#### 4️⃣ Converting to CSV
- Select extracted files (.xml)
- Click "Convert" for CSV conversion
- Uses streaming for memory efficiency

#### 5️⃣ Managing Files
- Delete unwanted files with "Delete" button
- View file status with ✔/✖ indicators
- Track total downloaded size

## 📁 File Organization

```
Discogs/
├── Datasets/
│   ├── YYYY-MM/
│   │   ├── discogs_YYYY-MM-DD_type.xml.gz
│   │   ├── discogs_YYYY-MM-DD_type.xml
│   │   └── discogs_YYYY-MM-DD_type.csv
│   └── ...
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

## 🛠️ Development

### Built With
- Python 3.7+
- ttkbootstrap for UI
- pandas for data processing
- requests for downloads

### Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

## 👤 Author

**Furkan Coban**
- LinkedIn: [ofurkancoban](https://www.linkedin.com/in/ofurkancoban/)
- GitHub: [ofurkancoban](https://github.com/ofurkancoban)
- Kaggle: [ofurkancoban](https://www.kaggle.com/ofurkancoban)

## 🙏 Acknowledgments

- [Discogs](https://www.discogs.com/) for providing data dumps
- [ttkbootstrap](https://ttkbootstrap.readthedocs.io/) for UI components
- [Icons8](https://icons8.com/) for application icons

## 📞 Support

If you encounter any issues or have questions:
1. Check the detailed logs in the application
2. Open an issue on GitHub
3. Contact through LinkedIn

---

<p align="center">
  Made with ❤️ by ofurkancoban
</p>
