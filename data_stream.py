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

    def get_stats(self) -> tuple[int, int]:
        return self.counter, len(self.values)


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
        start_n = len(self.values)
        if isinstance(data, list):
            for n, dn in enumerate(data, start=start_n):
                self.values.append((n, str(dn)))
                self.counter += 1
        else:
            self.values.append((start_n,str(data)))
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
        start_n = len(self.values)
        if isinstance(data, list):
            for n, text in enumerate(data, start=start_n):
                self.values.append((n, text))
                self.counter += 1
        else:
            self.values.append((start_n,data))
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
                self.counter += 1
        else:
            value1 = data["log_level"]
            value2 = data["log_message"]
            text = f"{value1}: {value2}"
            self.values.append((start_n,text))
            self.counter += 1


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []


    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for data in stream:
            for processor in self.processors:
                if processor.validate(data):
                    processor.ingest(data)
                    break
            else:
                print(f"DataStream Error - Can't process element in stream: {data}")

    def print_processor_stats(self) -> None:
        print("== DataStream statics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for processor in self.processors:
            processed, remain = processor.get_stats()
            processor_name = type(processor).__name__
            print(f"{processor_name}: total {processed} items processed, remaining {remain} on processor")


def main() -> None:
    print("== Code Nexus- Data Stream ===\n")
    print("Initialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processor_stats()
    print()
    print("Registering Numeric Processor")
    data_stream.register_processor(NumericProcessor())
    sample_stream = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {
                'log_level': 'WARNING',
                'log_message': 'Telnet access! Use ssh instead'
            },
            {
                'log_level': 'INFO',
                'log_message': 'User wil is connected'
            }
        ],
        42,
        ['Hi', 'five']
    ]
    print(f"send first batch of data on stream: {sample_stream}")
    data_stream.process_stream(sample_stream)
    data_stream.print_processor_stats()
    print("Registering other data processors")
    data_stream.register_processor(TextProcessor())
    data_stream.register_processor(LogProcessor())
    print("Send the same batch again")
    data_stream.process_stream(sample_stream)
    data_stream.print_processor_stats()
    print("\nConsume some elements from the data processors: Numeric 3, Text 2, Log 1")
    for _ in range(3):
        data_stream.processors[0].output()
    for _ in range(2):
        data_stream.processors[1].output()
    data_stream.processors[2].output()
    data_stream.print_processor_stats()


if __name__ == "__main__":
    main()