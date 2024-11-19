import asyncio
from datetime import datetime, timedelta
from alpaca.data.live import CryptoDataStream

# Alpaca API keys (replace with your actual keys)
API_KEY = 'PKVGPLPANKIC48R0NBWG'
API_SECRET = 'urF5LGJRjDj235Tw0MFcjEuRr3TRGUBlbOCsAAyW'

# Initialize the CryptoDataStream client
stream = CryptoDataStream('PKVGPLPANKIC48R0NBWG','urF5LGJRjDj235Tw0MFcjEuRr3TRGUBlbOCsAAyW')

# Set the end time to 5 minutes from now
end_time = datetime.utcnow() + timedelta(minutes=5)

# Define the async callback function to handle incoming crypto trade data
async def on_crypto_trade(data):
    # Print incoming trade data
    print(f"Crypto trade data received: {data}")

# Subscribe to trade updates for a specific cryptocurrency pair, e.g., "BTC/USD"
crypto_pair = "BTC/USD"
stream.subscribe_trades(on_crypto_trade, crypto_pair)

# Function to run the streaming client for 5 minutes
async def start_stream():
    try:
        print("Starting crypto data stream...")
        await stream.run()  # This blocks until the stream is stopped
    except Exception as e:
        print(f"Error in stream: {e}")

# Function to stop the stream after 5 minutes
async def stop_stream_after_duration():
    while datetime.utcnow() < end_time:
        await asyncio.sleep(1)  # Check every second
    print("5 minutes have passed. Stopping the stream...")
    await stream.stop()  # Stop the streaming client

# Main function to manage the tasks
async def main():
    # Schedule both start_stream and stop_stream_after_duration to run concurrently
    tasks = [
        asyncio.create_task(start_stream()),
        asyncio.create_task(stop_stream_after_duration())
    ]
    await asyncio.gather(*tasks)  # Wait for both tasks to complete
    print("Stream stopped successfully.")

# Run the main function in an asyncio event loop
asyncio.run(main())