from abc import ABC, abstractmethod
from typing import Any

class DataProcessor(ABC):
    def __init__(self) -> None:
        self.values: list[tuple[int, str]] = []

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        return self.values.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(
            isinstance(value, (int, float))
            for value in data
            )
        else:
            return False

    def ingest(self, data: Any) -> None:
        if self.validate(data) == False:
            print("Got exception: Improper numeric data")
            return
        start_n = len(self.values) + 1
        if isinstance(data, list):
            for n, dn in enumerate(data, start=start_n):
                self.values.append((n, str(dn)))
        else:
            self.values.append((start_n,str(data)))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
                    return True
        if isinstance(data, list):
            return all(
            isinstance(item, str)
            for item in data
            )
        else:
            return False

    def ingest(self, data: Any) -> None:
        if self.validate(data) == False:
            print("Got exception: Improper text data")
            return
        start_n = len(self.values) + 1
        if isinstance(data, list):
            for n, text in enumerate(data, start=start_n):
                self.values.append((n, text))
        else:
            self.values.append((start_n,data))

def main() -> None:
    print("=== Code Nexus - Data Processor ===")
    processor1 = NumericProcessor()
    result1 = processor1.validate("hello")
    result2 = processor1.validate([876,5678])
    print(result1, result2)
    processor1.ingest([876,5678,7])
    for _ in range(3):
        numeric_values = processor1.output()
        print(f"Numeric value:{numeric_values[0]}:{numeric_values[1]}")
    processor2 = TextProcessor()
    result3 = processor2.validate(["abc",321])
    print(result3)
    processor2.ingest(["Hello","good morning"])
    text_values = processor2.output()
    print(f"Text value:{text_values[0]}:{text_values[1]}")
    text_values = processor2.output()
    print(f"Text value:{text_values[0]}:{text_values[1]}")


if __name__ == "__main__":
    main()
