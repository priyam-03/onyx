import os
from typing import Any

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.configs.constants import FileOrigin
from onyx.connectors.file.connector import _process_file
from onyx.connectors.interfaces import GenerateDocumentsOutput
from onyx.connectors.interfaces import LoadConnector
from onyx.connectors.models import Document
from onyx.file_processing.extract_file_text import get_file_ext
from onyx.file_processing.extract_file_text import is_accepted_file_ext
from onyx.file_processing.extract_file_text import OnyxExtensionType
from onyx.file_store.file_store import get_default_file_store
from onyx.utils.logger import setup_logger

logger = setup_logger()


def _should_process_file(file_path: str) -> bool:
    """
    Skip directories and hidden files (starting with .).
    Similar to the logic in upload_files for file connector.
    """
    normalized_path = os.path.normpath(file_path)
    # Skip hidden files and directories
    return not any(part.startswith(".") for part in normalized_path.split(os.sep))


def _scan_folder_recursively(folder_path: str) -> list[tuple[str, str]]:
    """
    Recursively scan a folder and return list of (file_path, file_name) tuples.
    
    Args:
        folder_path: Path to the folder to scan
        
    Returns:
        List of tuples (full_path, relative_path) for all files in the folder
    """
    files = []
    
    if not os.path.exists(folder_path):
        logger.error(f"Folder path does not exist: {folder_path}")
        return files
    
    if not os.path.isdir(folder_path):
        logger.error(f"Path is not a directory: {folder_path}")
        return files
    
    try:
        # Recursively walk through all files in the folder
        for root, dirs, filenames in os.walk(folder_path):
            # Skip hidden directories
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            
            for filename in filenames:
                # Skip hidden files
                if filename.startswith("."):
                    continue
                
                full_path = os.path.join(root, filename)
                relative_path = os.path.relpath(full_path, folder_path)
                
                # Check if we should process this file
                if not _should_process_file(relative_path):
                    continue
                
                # Check if file has accepted extension
                extension = get_file_ext(filename)
                if not is_accepted_file_ext(extension, OnyxExtensionType.All):
                    logger.debug(
                        f"Skipping file '{full_path}' with unrecognized extension '{extension}'"
                    )
                    continue
                
                files.append((full_path, relative_path))
        
        logger.info(f"Scanned folder '{folder_path}' and found {len(files)} files")
        return files
    
    except Exception as e:
        logger.error(f"Error scanning folder '{folder_path}': {e}")
        raise


def _upload_files_from_folder(
    folder_path: str, file_store
) -> tuple[list[str], list[str], dict[str, Any]]:
    """
    Scan folder and upload all files to file store.
    
    Returns:
        Tuple of (file_ids, file_names, metadata_dict)
    """
    file_ids = []
    file_names = []
    metadata = {}
    
    files_to_upload = _scan_folder_recursively(folder_path)
    
    for full_path, relative_path in files_to_upload:
        try:
            with open(full_path, "rb") as file_handle:
                import mimetypes
                
                mime_type, _ = mimetypes.guess_type(full_path)
                if mime_type is None:
                    mime_type = "application/octet-stream"
                
                file_id = file_store.save_file(
                    content=file_handle,
                    display_name=os.path.basename(relative_path),
                    file_origin=FileOrigin.CONNECTOR,
                    file_type=mime_type,
                )
                
                file_ids.append(file_id)
                file_names.append(os.path.basename(relative_path))
                # Store relative path as metadata for potential future use
                metadata[os.path.basename(relative_path)] = {
                    "relative_path": relative_path,
                    "full_path": full_path,
                }
                
                logger.debug(f"Uploaded file '{relative_path}' with ID '{file_id}'")
        
        except Exception as e:
            logger.error(f"Failed to upload file '{full_path}': {e}")
            continue
    
    return file_ids, file_names, metadata


class FolderConnector(LoadConnector):
    """
    Connector that reads files from a local folder path and yields Documents.
    Similar to LocalFileConnector but accepts a folder path instead of file IDs.
    
    The folder is scanned recursively on each indexing run, and all files are
    uploaded to the file store before processing.
    """

    def __init__(
        self,
        folder_path: str,
        batch_size: int = INDEX_BATCH_SIZE,
    ) -> None:
        self.folder_path = folder_path
        self.batch_size = batch_size
        self.pdf_pass: str | None = None

    def _upload_folder_files(self) -> tuple[list[str], list[str], dict[str, Any]]:
        """Scan folder and upload all files to file store."""
        file_store = get_default_file_store()
        return _upload_files_from_folder(self.folder_path, file_store)

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        self.pdf_pass = credentials.get("pdf_password")
        return None

    def load_from_state(self) -> GenerateDocumentsOutput:
        """
        Scans the folder, uploads files to file store, then iterates over each file,
        fetches from file store, tries to parse text or images, and yields Document batches.
        """
        # Scan folder and upload all files to file store
        file_locations, file_names, metadata = self._upload_folder_files()
        
        if not file_locations:
            logger.warning(
                f"No files found or uploaded from folder '{self.folder_path}'"
            )
            return
        
        documents: list[Document] = []

        for file_id, file_name in zip(file_locations, file_names):
            file_store = get_default_file_store()
            file_record = file_store.read_file_record(file_id=file_id)
            if not file_record:
                logger.warning(f"No file record found for '{file_id}' in PG; skipping.")
                continue

            file_metadata = metadata.get(file_record.display_name, {})
            if "connector_type" not in file_metadata:
                file_metadata["connector_type"] = DocumentSource.FOLDER.value
            file_io = file_store.read_file(file_id=file_id, mode="b")
            new_docs = _process_file(
                file_id=file_id,
                file_name=file_record.display_name,
                file=file_io,
                metadata=file_metadata,
                pdf_pass=self.pdf_pass,
                file_type=file_record.file_type,
            )
            documents.extend(new_docs)

            if len(documents) >= self.batch_size:
                yield documents
                documents = []

        if documents:
            yield documents

