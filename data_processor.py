from abc import ABC, abstractmethod
from typing import Any

class DataProcessor(ABC):
    def __init__(self) -> None:
        self.values: list[tuple[int, str]] = []
        self.counter = 0

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
        if isinstance(data, list):
            for value in data:
                self.values.append((self.counter, str(value)))
                self.counter += 1
        else:
            self.values.append((self.counter, str(data)))
            self.counter += 1


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
        if isinstance(data, list):
            for text in data:
                self.values.append((self.counter, text))
                self.counter += 1
            else:
                self.values.append((self.counter, data))
                self.counter += 1

class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict) and set(data.keys()) == {"log_level", "log_message"}:
            return True
        if isinstance(data, list):
            return all(
            isinstance(item, dict) and set(item.keys()) == {"log_level", "log_message"}
            for item in data
            )
        else:
            return False

    def ingest(self, data: Any) -> None:
        if self.validate(data) == False:
            print("Got exception: Improper log data")
            return
        start_n = len(self.values)
        if isinstance(data, list):
            for n, log in enumerate(data, start=start_n):
                value1 = log["log_level"]
                value2 = log["log_message"]
                text = f"{value1}: {value2}"
                self.values.append((n, text))
        else:
            value1 = data["log_level"]
            value2 = data["log_message"]
            text = f"{value1}: {value2}"
            self.values.append((start_n,text))

def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    processor1 = NumericProcessor()
    result1 = processor1.validate(42)
    print(f"Trying to validate input '42': {result1}")
    result2 = processor1.validate("Hello")
    print(f"Trying to validate input 'Hello': {result2}")
    print("Test invalid ingestion of string 'foo' without prior validation")
    processor1.ingest("foo")
    data1 = [1, 2, 3, 4, 5]
    print(f"Processing data: {data1}")
    processor1.ingest(data1)
    print("Extracting 3 values...")
    for _ in range(3):
        result3 = processor1.output()
        print(f"Numeric value {result3[0]}: {result3[1]}")
    print()
    print("Testing Text Processor...")
    processor2 = TextProcessor()
    result4 = processor2.validate(42)
    print(f"Trying to validate input '42': {result4}")
    data2 = ['Hello', 'Nexus', 'World']
    print(f"Processing data: {data2}")
    processor2.ingest(data2)
    print("Extracting 1 value")
    text_values = processor2.output()
    print(f"Text value {text_values[0]}: {text_values[1]}\n")
    print("Testing Log Processor...")
    processor3 = LogProcessor()
    data3 = 'Hello'
    result5 = processor3.validate(data3)
    print(f"Trying to validate input '{data3}': {result5}")
    data4 = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'}, {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    print(f"Processing data: {data4}")  
    processor3.ingest(data4)
    print("Extracting 2 values")
    for _ in range (2):
        log_values = processor3.output()
        print(f"Log entry {log_values[0]}:{log_values[1]}")


if __name__ == "__main__":
    main()
