from typing import Dict, Optional


class MemTable:
    def __init__(self) -> None:
        self.__mem_table: Dict[str, str] = {}

    def __contains__(self, key: str) -> bool:
        return key in self.__mem_table

    def __getitem__(self, key: str) -> str:
        return self.__mem_table[key]

    def __setitem__(self, key: str, value: str) -> None:
        self.__mem_table[key] = value

    def __delitem__(self, key: str) -> None:
        del self.__mem_table[key]


class LSMTree:
    def __init__(self) -> None:
        self.__mem_table = MemTable()

    def get(self, key: str) -> Optional[str]:
        if key in self.__mem_table:
            return self.__mem_table[key]
        return None

    def put(self, key: str, value: str) -> None:
        self.__mem_table[key] = value

    def delete(self, key: str) -> None:
        del self.__mem_table[key]
