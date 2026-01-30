import argparse
import os
import sys
from mongo_file_manager import MongoFileManager

def main():
    parser = argparse.ArgumentParser(description="MongoDB File Manager CLI")
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")

    # Common arguments
    parser.add_argument("--db-url", default="mongodb+srv://Albert:Uday.2244@uday.ukeabds.mongodb.net/", help="MongoDB Connection URL")
    parser.add_argument("--db-name", default="test_db", help="MongoDB Database Name")
    parser.add_argument("--threshold", type=int, default=15, help="GridFS Threshold in MB")

    # Upload Command
    upload_parser = subparsers.add_parser("upload", help="Upload a file")
    upload_parser.add_argument("file", help="Path to the file to upload")

    # Download Command
    download_parser = subparsers.add_parser("download", help="Download a file")
    download_parser.add_argument("filename", help="Name of the file to download")
    download_parser.add_argument("--destination", default="./downloaded", help="Destination directory (default: current dir)")

    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        sys.exit(1)

    try:
        manager = MongoFileManager(
            db_url=args.db_url,
            db_name=args.db_name,
            gridfs_threshold_mb=args.threshold
        )

        if args.action == "upload":
            if not os.path.exists(args.file):
                print(f"Error: File '{args.file}' not found.")
                sys.exit(1)
            
            print(f"Uploading '{args.file}'...")
            file_id, storage_type = manager.upload_file(args.file)
            print(f"Success! Uploaded via {storage_type}. ID: {file_id}")

        elif args.action == "download":
            print(f"Downloading '{args.filename}' to '{args.destination}'...")
            output_path = manager.download_file(args.filename, args.destination)
            print(f"Success! File saved to: {os.path.abspath(output_path)}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
