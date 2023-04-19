from typing import Dict, Optional


class LSMTree:
    def __init__(self) -> None:
        self.__mem_table: Dict[str, str] = {}

    def get(self, key: str) -> Optional[str]:
        if key in self.__mem_table:
            return self.__mem_table[key]
        return None

    def put(self, key: str, value: str) -> None:
        self.__mem_table[key] = value

    def delete(self, key: str) -> None:
        del self.__mem_table[key]
