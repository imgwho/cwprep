import json
import os
import shutil
import uuid
import zipfile
from datetime import datetime


class TFLPackager:
    @staticmethod
    def _build_backup_path(path):
        """Build a unique timestamped backup path next to an existing file/dir."""
        parent = os.path.dirname(path) or "."
        name = os.path.basename(path)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        backup_path = os.path.join(parent, f"{name}.bak-{timestamp}")
        suffix = 1

        while os.path.exists(backup_path):
            backup_path = os.path.join(parent, f"{name}.bak-{timestamp}-{suffix}")
            suffix += 1

        return backup_path

    @staticmethod
    def _backup_existing_path(path):
        """Move an existing file/dir aside before writing a replacement."""
        if not os.path.exists(path):
            return None

        backup_path = TFLPackager._build_backup_path(path)
        shutil.move(path, backup_path)
        return backup_path

    @staticmethod
    def _create_temp_workspace(parent_dir):
        """Create a unique temporary workspace near the final output."""
        os.makedirs(parent_dir, exist_ok=True)

        while True:
            token = uuid.uuid4().hex
            temp_root = os.path.join(parent_dir, f"cwprep_build_{token}")
            if os.path.exists(temp_root):
                continue
            os.makedirs(temp_root)
            return temp_root, os.path.join(temp_root, "flow")

    @staticmethod
    def _pack_archive(folder_path, output_path, keep_folder=False):
        """Pack an exploded flow folder into a final archive file."""
        folder_path = os.path.abspath(folder_path)
        output_path = os.path.abspath(output_path)

        if not os.path.isdir(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")

        try:
            is_nested_output = os.path.commonpath([folder_path, output_path]) == folder_path
        except ValueError:
            # Different drives on Windows are fine; skip the containment check there.
            is_nested_output = False

        if is_nested_output:
            raise ValueError("output_path must be outside folder_path.")

        output_dir = os.path.dirname(output_path) or "."
        os.makedirs(output_dir, exist_ok=True)

        suffix = os.path.splitext(output_path)[1]
        temp_archive = None
        success = False

        try:
            while True:
                token = uuid.uuid4().hex
                candidate = os.path.join(output_dir, f"cwprep_output_{token}{suffix}")
                if not os.path.exists(candidate):
                    temp_archive = candidate
                    break

            with zipfile.ZipFile(temp_archive, "w", zipfile.ZIP_DEFLATED) as zipf:
                for root, _dirs, files in os.walk(folder_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, folder_path)
                        zipf.write(file_path, arcname)

            TFLPackager._backup_existing_path(output_path)
            os.replace(temp_archive, output_path)
            success = True
        finally:
            if temp_archive and os.path.exists(temp_archive):
                os.unlink(temp_archive)
            if success and not keep_folder:
                shutil.rmtree(folder_path, ignore_errors=True)

        print(f"Successfully created: {output_path}")
        return output_path

    @staticmethod
    def save_to_folder(folder_path, flow, display, meta, data_files=None):
        """Write generated JSON to folder.

        Args:
            folder_path: Output folder path.
            flow: Flow JSON object.
            display: Display settings JSON object.
            meta: Maestro metadata JSON object.
            data_files: Optional dict mapping connection_id to list of source
                        file paths. Files are copied to Data/{connection_id}/.
                        Example: {"abc-123": ["./orders.xlsx"]}
        """
        if os.path.exists(folder_path):
            TFLPackager._backup_existing_path(folder_path)
        os.makedirs(folder_path)

        with open(os.path.join(folder_path, "flow"), "w", encoding="utf-8") as f:
            json.dump(flow, f, indent=2, ensure_ascii=False)

        with open(os.path.join(folder_path, "displaySettings"), "w", encoding="utf-8") as f:
            json.dump(display, f, indent=2, ensure_ascii=False)

        with open(os.path.join(folder_path, "maestroMetadata"), "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)

        if data_files:
            data_dir = os.path.join(folder_path, "Data")
            for conn_id, file_paths in data_files.items():
                conn_dir = os.path.join(data_dir, conn_id)
                os.makedirs(conn_dir, exist_ok=True)
                for src_path in file_paths:
                    dst_path = os.path.join(conn_dir, os.path.basename(src_path))
                    shutil.copy2(src_path, dst_path)

    @staticmethod
    def save_tfl(output_tfl_path, flow, display, meta):
        """Build a .tfl archive without leaving an exploded folder behind."""
        output_path = os.path.abspath(output_tfl_path)
        temp_root, temp_folder = TFLPackager._create_temp_workspace(
            os.path.dirname(output_path) or "."
        )
        try:
            TFLPackager.save_to_folder(temp_folder, flow, display, meta)
            return TFLPackager.pack_zip(temp_folder, output_path)
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)

    @staticmethod
    def save_tflx(output_tflx_path, flow, display, meta, data_files=None):
        """Build a .tflx archive without leaving an exploded folder behind."""
        output_path = os.path.abspath(output_tflx_path)
        temp_root, temp_folder = TFLPackager._create_temp_workspace(
            os.path.dirname(output_path) or "."
        )
        try:
            TFLPackager.save_to_folder(
                temp_folder,
                flow,
                display,
                meta,
                data_files=data_files,
            )
            return TFLPackager.pack_tflx(temp_folder, output_path)
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)

    @staticmethod
    def pack_zip(folder_path, output_tfl_path, keep_folder=False):
        """Pack folder as .tfl and remove the source folder by default."""
        return TFLPackager._pack_archive(
            folder_path,
            output_tfl_path,
            keep_folder=keep_folder,
        )

    @staticmethod
    def pack_tflx(folder_path, output_tflx_path, keep_folder=False):
        """Pack folder as .tflx and remove the source folder by default.

        Same as pack_zip but specifically for .tflx output. The folder should
        contain a Data/ directory with embedded files when packaging file-based
        flows.
        """
        return TFLPackager._pack_archive(
            folder_path,
            output_tflx_path,
            keep_folder=keep_folder,
        )
