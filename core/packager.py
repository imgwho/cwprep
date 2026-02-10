import json
import zipfile
import os
import shutil

class TFLPackager:
    @staticmethod
    def save_to_folder(folder_path, flow, display, meta):
        """将生成的 JSON 写入文件夹"""
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
        os.makedirs(folder_path)
        
        # 写入 flow (无后缀)
        with open(os.path.join(folder_path, "flow"), "w", encoding="utf-8") as f:
            json.dump(flow, f, indent=2, ensure_ascii=False)
            
        # 写入 displaySettings (无后缀)
        with open(os.path.join(folder_path, "displaySettings"), "w", encoding="utf-8") as f:
            json.dump(display, f, indent=2, ensure_ascii=False)
            
        # 写入 maestroMetadata (无后缀)
        with open(os.path.join(folder_path, "maestroMetadata"), "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)

    @staticmethod
    def pack_zip(folder_path, output_tfl_path):
        """将文件夹打包为 .tfl (ZIP)"""
        with zipfile.ZipFile(output_tfl_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, folder_path)
                    zipf.write(file_path, arcname)
        print(f"Successfully created: {output_tfl_path}")
