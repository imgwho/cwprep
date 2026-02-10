import json
import zipfile
import os
import shutil

class TFLPackager:
    @staticmethod
    def save_to_folder(folder_path, flow, display, meta):
        """Write generated JSON to folder"""
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
