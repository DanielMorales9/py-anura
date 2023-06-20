import json
from typing import Dict

from constants import TEST_META

from anura.metadata import TableMetadata


def setup_metadata(tmp_path, metadata: Dict = None) -> TableMetadata:
    metadata = metadata or TEST_META
    meta_data_path = tmp_path / "metadata.json"
    meta_data_path.write_text(json.dumps(metadata))
    return TableMetadata(tmp_path)
