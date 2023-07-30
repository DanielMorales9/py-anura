import json
from typing import Dict

from constants import TEST_META


def setup_metadata(tmp_path, metadata: Dict = None):
    metadata = metadata or TEST_META
    meta_data_path = tmp_path / "metadata.json"
    meta_data_path.write_text(json.dumps(metadata))
