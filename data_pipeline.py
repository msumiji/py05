from abc import ABC, abstractmethod
from typing import Any
from typing import Protocol

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
        # start_n = self.counter
        # if isinstance(data, list):
        #     for n, dn in enumerate(data, start=start_n):
        #         self.values.append((n, str(dn)))
        #         self.counter += 1
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
        if isinstance(data, list):
            for log in data:
                value1 = log["log_level"]
                value2 = log["log_message"]
                text = f"{value1}: {value2}"
                self.values.append((self.counter, text))
                self.counter += 1
        else:
            value1 = data["log_level"]
            value2 = data["log_message"]
            text = f"{value1}: {value2}"
            self.values.append((self.counter,text))
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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self.processors:
            data: list[tuple[int, str]] = []
            for _ in range(nb):
                if processor.values:
                    data.append(processor.output())
            plugin.process_output(data)

    def print_processor_stats(self) -> None:
        print("== DataStream statics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for processor in self.processors:
            processed, remain = processor.get_stats()
            processor_name = type(processor).__name__
            print(f"{processor_name}: total {processed} items processed, remaining {remain} on processor")

class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass

class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        print(",".join(row[1] for row in data))

class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        items = [
            f'"item_{number}": "{value}"'
            for number, value in data
        ]
        print("{" + ", ".join(items) + "}")

def main() -> None:
    print("=== Code Nexus - Data Pipeline ===\n")
    print("Initialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processor_stats()
    print()
    print("Registering Processors")
    data_stream.register_processor(NumericProcessor())
    data_stream.register_processor(TextProcessor())
    data_stream.register_processor(LogProcessor())
    sample_stream1 = [
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
    print(f"send first batch of data on stream: {sample_stream1}")
    data_stream.process_stream(sample_stream1)
    data_stream.print_processor_stats()
    print("Send 3 processed data from each processor to a CSV plugin")
    data_stream.output_pipeline(3, CSVPlugin())
    print()
    data_stream.print_processor_stats()
    sample_stream2 =  [
        21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'], 
        [
            {'log_level': 'ERROR', 'log_message': '500 server crash'},
            {'log_level': 'NOTICE', 'log_message': 'Certificate expires in 10 days'}
        ],
        [32, 42, 64, 84, 128, 168], 'World hello'
    ]
    print(f"send another batch of data: {sample_stream2}\n")
    data_stream.process_stream(sample_stream2)
    data_stream.print_processor_stats()
    print()
    print("Send 5 processed data from each processor to a JSON plugin:")
    data_stream.output_pipeline(5, JSONPlugin())
    data_stream.print_processor_stats()


if __name__ == "__main__":
    main()