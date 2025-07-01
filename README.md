# Blitzortung WebSocket Client

This Python script connects to the Blitzortung WebSocket service, collects lightning data, decodes it using LZW compression, and saves it to a CSV file.

## Features

- Connects to `wss://ws1.blitzortung.org/`
- Sends authentication message `{"a": 111}` on connection
- Collects and processes incoming data
- Implements LZW decompression algorithm
- Saves data to CSV files with timestamps
- Comprehensive logging
- Graceful error handling

## Installation

1. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

Run the script:
```bash
python blitzortung_client.py
```

The script will:
1. Connect to the WebSocket
2. Send the authentication message
3. Start collecting data
4. Save data to `blitzortung_data.csv`
5. Create a timestamped backup file

Press `Ctrl+C` to stop data collection and save the results.

## Output

The script generates two CSV files:
- `blitzortung_data.csv` - Main data file
- `blitzortung_data_YYYYMMDD_HHMMSS.csv` - Timestamped backup

## Data Structure

The CSV will contain columns based on the received data structure, typically including:
- `timestamp` - When the data was received
- `data_type` - Type of data (json/raw)
- Additional columns based on the actual data received from Blitzortung

## Logging

The script provides detailed logging information including:
- Connection status
- Messages sent/received
- Data processing status
- Error information
