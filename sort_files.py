import os
import csv
from datetime import datetime
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

def sort_csv_by_time(directory_path):
    # Create a 'sorted' directory if it doesn't exist
    sorted_dir = os.path.join(directory_path, "sorted")
    if not os.path.exists(sorted_dir):
        os.makedirs(sorted_dir)

    with ThreadPoolExecutor() as executor:  # Use ThreadPoolExecutor for concurrent processing
        futures = []
        
        for filename in os.listdir(directory_path):
            if filename.endswith(".csv"):  # Process only CSV files
                file_path = os.path.join(directory_path, filename)
                futures.append(executor.submit(process_file, file_path, sorted_dir))
        
        for future in as_completed(futures):
            future.result()  # Wait for all tasks to finish

def process_file(file_path, sorted_dir):
    # Variables to store header and rows
    header = None
    chunk_files = []  # List of temporary files for sorted chunks

    try:
        # Open the CSV file and process in chunks
        with open(file_path, "r", newline='') as f:
            reader = csv.reader(f)
            rows = []
            for line in reader:
                # Skip lines starting with '#' or empty lines
                if not line or line[0].startswith('#'):
                    continue
                
                # Detect header row
                if header is None and "Time" in line:
                    header = line
                    time_index = header.index('Time')
                    continue
                
                # Skip rows until header is found
                if header is None:
                    continue

                # Process rows with valid time
                try:
                    if line[time_index]:
                        # Attempt to parse time and append to rows
                        datetime.strptime(line[time_index], "%H:%M:%S.%f")
                        rows.append(line)  # Only append valid rows
                    else:
                        print(f"Skipping row with empty time field in {file_path}: {line}")
                except ValueError:
                    print(f"Skipping row with invalid time format in {file_path}: {line}")
            
                # Process in chunks
                if len(rows) >= 100000:  # Process in chunks of 100,000 rows
                    chunk_file = process_chunk(rows, time_index)
                    chunk_files.append(chunk_file)
                    rows = []
            
            # If there are remaining rows after the last chunk
            if rows:
                chunk_file = process_chunk(rows, time_index)
                chunk_files.append(chunk_file)

        # Now merge all the chunks into one sorted file
        sorted_file_path = os.path.join(sorted_dir, f"{os.path.splitext(os.path.basename(file_path))[0]}_sorted.csv")
        with open(sorted_file_path, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)  # Write header
            for chunk_file in chunk_files:
                with open(chunk_file, "r", newline='') as temp_f:
                    reader = csv.reader(temp_f)
                    for row in reader:
                        writer.writerow(row)

        # Clean up temporary files
        for chunk_file in chunk_files:
            os.remove(chunk_file)

        print(f"Sorted file created: {sorted_file_path}")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

def process_chunk(rows, time_index):
    # Sort the rows by the 'Time' field (this is where optimization happens)
    rows.sort(key=lambda row: datetime.strptime(row[time_index], "%H:%M:%S.%f"))
    
    # Write the sorted rows to a temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='')
    with open(temp_file.name, "w", newline='') as temp_f:
        writer = csv.writer(temp_f)
        writer.writerows(rows)
    
    return temp_file.name

if __name__ == "__main__":
    # Specify your directory path
    sort_csv_by_time("./data/trading_data")
