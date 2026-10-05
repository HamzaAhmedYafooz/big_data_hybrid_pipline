import csv
import argparse
import os

from tqdm import tqdm


def generate_sample(input_path, output_path, num_rows=100_000):
    print(" Beginning small sample extraction...", flush=True)
    try:
        with open(input_path, 'r', encoding='utf-8-sig', errors='ignore') as infile, \
            open(output_path, 'w', encoding='utf-8', newline='') as outfile:
            
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            header = next(reader)
            writer.writerow(header)
            
            count = 0
            print(f"--> Starting extracting {num_rows}")
            for row in tqdm(reader, total= num_rows, desc= "--> Extracting", unit = "record"):
                writer.writerow(row)
                count += 1
                if count >= num_rows:
                    break
        print(f"✅ ","-"*40, flush=True)
        print(f"✅ Sample created at: {output_path}", flush=True)
        print(f"✅ Success! Extracted {count} rows.", flush=True)
        print(f"✅ ","-"*40, flush=True)
        return True
    except Exception as e:
        print(f"❌ Error occurred while extracting the sample: {e}", flush=True)
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create a small reproducible sample from a huge CSV file.')
    parser.add_argument('--input', type=str, required=True, help='Input huge CSV file path') # 
    parser.add_argument('--rows', type=int, default=100000, help='Number of rows for the sample')
    parser.add_argument('--output', type=str, default='data/orders_small_sample.csv', help='Output sample CSV file path')
    
    args = parser.parse_args()
    
    # Ensure data dir exists
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    generate_sample(args.input, args.output, args.rows)