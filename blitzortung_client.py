import asyncio
import websockets
import json
import csv
import pandas as pd
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def lzw_decode(s):
    """
    Python implementation of the LZW decode function
    Translated from the JavaScript version provided
    """
    if not s:
        return ""
    
    # Convert string to list of characters
    data = list(str(s))
    curr_char = data[0]
    old_phrase = curr_char
    out = [curr_char]
    code = 256
    dict_decode = {}
    
    for i in range(1, len(data)):
        curr_code = ord(data[i])
        
        if curr_code < 256:
            phrase = data[i]
        else:
            if curr_code in dict_decode:
                phrase = dict_decode[curr_code]
            else:
                phrase = old_phrase + curr_char
        
        out.append(phrase)
        curr_char = phrase[0] if phrase else ""
        dict_decode[code] = old_phrase + curr_char
        code += 1
        old_phrase = phrase
    
    return "".join(out)

class BlitzortungClient:
    def __init__(self, csv_filename="blitzortung_data.csv"):
        self.csv_filename = csv_filename
        self.data_buffer = []
        self.websocket = None
        
    async def connect_and_collect(self):
        """Connect to WebSocket, send message, and collect data"""
        uri = "wss://ws1.blitzortung.org/"
        
        try:
            logger.info(f"Connecting to {uri}")
            async with websockets.connect(uri) as websocket:
                self.websocket = websocket
                logger.info("Connected successfully")
                
                # Send the required message
                message = {"a": 111}
                await websocket.send(json.dumps(message))
                logger.info(f"Sent message: {message}")
                
                # Collect data
                await self.collect_data()
                
        except websockets.exceptions.ConnectionClosed:
            logger.info("Connection closed")
        except Exception as e:
            logger.error(f"Error: {e}")
        finally:
            await self.save_to_csv()
    
    async def collect_data(self):
        """Collect and process incoming data"""
        logger.info("Starting data collection...")
        
        try:
            async for message in self.websocket:
                try:
                    # Log raw message
                    logger.info(f"Received raw message: {message[:100]}..." if len(message) > 100 else f"Received raw message: {message}")
                    
                    # Try to decode if it's LZW compressed
                    decoded_message = message
                    try:
                        decoded_message = lzw_decode(message)
                        logger.info(f"LZW decoded message: {decoded_message[:100]}..." if len(decoded_message) > 100 else f"LZW decoded message: {decoded_message}")
                    except Exception as decode_error:
                        logger.warning(f"LZW decode failed, using original message: {decode_error}")
                    
                    # Try to parse as JSON
                    try:
                        json_data = json.loads(decoded_message)
                        processed_data = self.process_json_data(json_data)
                    except json.JSONDecodeError:
                        # If not JSON, treat as raw data
                        processed_data = {
                            'timestamp': datetime.now().isoformat(),
                            'raw_data': decoded_message,
                            'data_type': 'raw'
                        }
                    
                    self.data_buffer.append(processed_data)
                    logger.info(f"Processed data entry: {processed_data}")
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info("WebSocket connection closed")
        except KeyboardInterrupt:
            logger.info("Data collection interrupted by user")
    
    def process_json_data(self, json_data):
        """Process JSON data and extract relevant fields"""
        processed = {
            'timestamp': datetime.now().isoformat(),
            'data_type': 'json'
        }
        
        # Add all JSON fields to the processed data
        if isinstance(json_data, dict):
            for key, value in json_data.items():
                processed[key] = value
        elif isinstance(json_data, list):
            processed['list_data'] = json.dumps(json_data)
        else:
            processed['json_data'] = str(json_data)
            
        return processed
    
    async def save_to_csv(self):
        """Save collected data to CSV file"""
        if not self.data_buffer:
            logger.info("No data to save")
            return
            
        logger.info(f"Saving {len(self.data_buffer)} records to {self.csv_filename}")
        
        try:
            # Convert to DataFrame for easier CSV handling
            df = pd.DataFrame(self.data_buffer)
            df.to_csv(self.csv_filename, index=False)
            logger.info(f"Data saved successfully to {self.csv_filename}")
            
            # Also save as backup with timestamp
            backup_filename = f"blitzortung_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(backup_filename, index=False)
            logger.info(f"Backup saved as {backup_filename}")
            
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")

async def main():
    """Main function to run the Blitzortung client"""
    client = BlitzortungClient()
    
    try:
        await client.connect_and_collect()
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {e}")

if __name__ == "__main__":
    print("Blitzortung WebSocket Client")
    print("Connecting to wss://ws1.blitzortung.org/")
    print("Press Ctrl+C to stop data collection and save to CSV")
    print("-" * 50)
    
    asyncio.run(main())
