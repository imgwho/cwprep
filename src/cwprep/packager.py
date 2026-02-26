import json
import zipfile
import os
import shutil

class TFLPackager:
    @staticmethod
    def save_to_folder(folder_path, flow, display, meta, data_files=None):
        """Write generated JSON to folder
        
        Args:
            folder_path: Output folder path
            flow: Flow JSON object
            display: Display settings JSON object
            meta: Maestro metadata JSON object
            data_files: Optional dict mapping connection_id to list of source
                        file paths. Files are copied to Data/{connection_id}/.
                        Example: {"abc-123": ["./orders.xlsx"]}
        """
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
        os.makedirs(folder_path)
        
        # Write flow (no extension)
        with open(os.path.join(folder_path, "flow"), "w", encoding="utf-8") as f:
            json.dump(flow, f, indent=2, ensure_ascii=False)
            
        # Write displaySettings (no extension)
        with open(os.path.join(folder_path, "displaySettings"), "w", encoding="utf-8") as f:
            json.dump(display, f, indent=2, ensure_ascii=False)
            
        # Write maestroMetadata (no extension)
        with open(os.path.join(folder_path, "maestroMetadata"), "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)

        # Copy data files into Data/{connection_id}/ directories
        if data_files:
            data_dir = os.path.join(folder_path, "Data")
            for conn_id, file_paths in data_files.items():
                conn_dir = os.path.join(data_dir, conn_id)
                os.makedirs(conn_dir, exist_ok=True)
                for src_path in file_paths:
                    dst_path = os.path.join(conn_dir, os.path.basename(src_path))
                    shutil.copy2(src_path, dst_path)

    @staticmethod
    def pack_zip(folder_path, output_tfl_path):
        """Pack folder as .tfl (ZIP)"""
        with zipfile.ZipFile(output_tfl_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, folder_path)
                    zipf.write(file_path, arcname)
        print(f"Successfully created: {output_tfl_path}")

    @staticmethod
    def pack_tflx(folder_path, output_tflx_path):
        """Pack folder as .tflx (ZIP with embedded data files)
        
        Same as pack_zip but specifically for .tflx output.
        The folder should contain a Data/ directory with embedded files.
        
        Args:
            folder_path: Folder containing flow, displaySettings,
                         maestroMetadata, and optionally Data/
            output_tflx_path: Output .tflx file path
        """
        with zipfile.ZipFile(output_tflx_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, folder_path)
                    zipf.write(file_path, arcname)
        print(f"Successfully created: {output_tflx_path}")
