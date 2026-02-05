import os
import hashlib
import pymongo
import gridfs
from bson.binary import Binary
from datetime import datetime

class MongoFileManager:
    def __init__(self, db_url='mongodb://localhost:27017', db_name='fileStorageDemo', gridfs_threshold_mb=15):
        self.client = pymongo.MongoClient(db_url)
        self.db = self.client[db_name]
        self.gridfs_threshold_bytes = gridfs_threshold_mb * 1024 * 1024
        self.fs = gridfs.GridFSBucket(self.db)
        self.bson_collection = self.db['files_bson']
        
        # Ensure indexes for faster lookups
        self.bson_collection.create_index("name")
        self.bson_collection.create_index("checksum")

    def calculate_checksum(self, file_path):
        """Calculates SHA-256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def calculate_checksum_from_bytes(self, data):
        """Calculates SHA-256 checksum from bytes."""
        return hashlib.sha256(data).hexdigest()

    def upload_file(self, file_path, custom_filename=None):
        """
        Uploads a file to MongoDB. 
        Decides intelligently between BSON and GridFS based on size.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        filename = custom_filename or os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        checksum = self.calculate_checksum(file_path)

        metadata = {
            "original_name": filename,
            "upload_date": datetime.now(),
            "checksum": checksum,
            "file_size": file_size
        }

        # Check if file with same checksum already exists (deduplication check - optional, but good for efficiency)
        # For this specific task, we will just proceed to upload, but strictly ensuring data integrity.
        duplicate_found = False
        if self.bson_collection.find_one({"metadata.checksum": checksum}):
            print(f"[INFO] File with checksum {checksum} already exists in BSON collection.")
            duplicate_found = True
        
        # Check GridFS if not found in BSON (or check both, strictly speaking we just want to know if IT exists)
        if not duplicate_found:
             # Using find with limit 1 to check existence
             if self.fs.find({"metadata.checksum": checksum}).limit(1).try_next():
                 print(f"[INFO] File with checksum {checksum} already exists in GridFS.")

        if file_size >= self.gridfs_threshold_bytes:
            # Use GridFS
            print(f"[UPLOAD] Size {file_size/1024/1024:.2f}MB > Threshold. Using GridFS.")
            with open(file_path, 'rb') as f:
                file_id = self.fs.upload_from_stream(
                    filename,
                    f,
                    metadata=metadata
                )
            storage_type = "GridFS"
        else:
            # Use BSON Document
            print(f"[UPLOAD] Size {file_size/1024/1024:.2f}MB <= Threshold. Using BSON Collection.")
            with open(file_path, 'rb') as f:
                data = f.read()
            
            doc = {
                "name": filename,
                "data": Binary(data),
                "metadata": metadata
            }
            result = self.bson_collection.insert_one(doc)
            file_id = result.inserted_id
            storage_type = "BSON"

        print(f"[SUCCESS] Uploaded '{filename}' via {storage_type}. ID: {file_id}. Checksum: {checksum}")
        return file_id, storage_type

    def download_file(self, filename, output_dir):
        """
        Retrieves a file by name. Checks 'files_bson' first, then GridFS.
        Verifies checksum after download.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output_path = os.path.join(output_dir, filename)
        
        # 1. Try finding in BSON collection
        doc = self.bson_collection.find_one({"name": filename})
        stored_checksum = None
        
        if doc:
            print(f"[DOWNLOAD] Found '{filename}' in BSON collection.")
            with open(output_path, 'wb') as f:
                f.write(doc['data'])
            stored_checksum = doc['metadata']['checksum']
        
        else:
            # 2. Try finding in GridFS
            # GridFS find returns a cursor, need to iterate
            grid_out_cursor = self.fs.find({"filename": filename}).sort("uploadDate", -1).limit(1)
            try:
                grid_out = grid_out_cursor.next()
                print(f"[DOWNLOAD] Found '{filename}' in GridFS.")
                
                with open(output_path, 'wb') as f:
                    self.fs.download_to_stream(grid_out._id, f)
                
                stored_checksum = grid_out.metadata['checksum']
                
            except StopIteration:
                raise FileNotFoundError(f"File '{filename}' not found in MongoDB (neither BSON nor GridFS).")

        # 3. Verify Integrity
        downloaded_checksum = self.calculate_checksum(output_path)
        
        if downloaded_checksum == stored_checksum:
            print(f"[VERIFIED] Checksum matched: {downloaded_checksum}")
            return output_path
        else:
            print(f"[ERROR] Checksum Mismatch! Stored: {stored_checksum}, Downloaded: {downloaded_checksum}")
            # Security measure: delete corrupted file
            if os.path.exists(output_path):
                os.remove(output_path)
            raise ValueError("Data integrity verification failed! Downloaded file is corrupted.")