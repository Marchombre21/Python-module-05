# ****************************************************************************#
#                                                                             #
#                                                         :::      ::::::::   #
#    nexus_pipeline.py                                  :+:      :+:    :+:   #
#                                                     +:+ +:+         +:+     #
#    By: bfitte <bfitte@student.42lyon.fr>          +#+  +:+       +#+        #
#                                                 +#+#+#+#+#+   +#+           #
#    Created: 2026/01/20 07:53:48 by bfitte            #+#    #+#             #
#    Updated: 2026/01/20 07:53:49 by bfitte           ###   ########lyon.fr   #
#                                                                             #
# ****************************************************************************#

from abc import ABC, abstractmethod
from typing import Any, Protocol, Iterable
import json
import time


class DataError(Exception):
    def __init__(self, details: str | None = None):
        message = details
        super().__init__(message)


class JSONError(DataError):
    pass


class CSVError(DataError):
    pass


class StreamError(DataError):
    pass


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...

    def prove_link(self) -> str:
        ...


class ProcessingPipeline(ABC):
    def __init__(self, id: str):
        self.__stages: list[ProcessingStage] = []
        self.__id = id

    @abstractmethod
    def process(self, data: Any) -> Any:
        res = data
        try:
            for stage in self.__stages:
                res = stage.process(res)
            return res
        except DataError as e:
            raise DataError(e)

    def add_stage(self, stage: ProcessingStage) -> None:
        self.__stages.append(stage)

    def link_pipeline(self) -> str:
        return self.__id

    def test_prove(self) -> None:
        """Just a function to prove the stages have the same interface
        """
        print("Data flow: ", end="")
        for i, stage in enumerate(self.__stages):
            print(stage.prove_link(), end="")
            if i + 1 < len(self.__stages):
                print(" -> ", end="")
        print()


class InputStage:
    def process(self, data: Any) -> Any:
        """Check which type of data is passed and adapts its behavior

        Args:
            data (Any): Data to be treated

        Raises:
            JSONError: Raise an error about json format
            CSVError: Raise an error about CSV file content

        Returns:
            Any: A dictionnary which contain usefull datas for the next stage
        """
        if isinstance(data, str) and ":" in data:
            print(f"Input: {data}")
            try:
                raws = json.loads(data)
                return raws
            except json.JSONDecodeError:
                raise JSONError("Error detected in Stage 1: "
                                "JSON file isn't in the correct format")
        elif isinstance(data, str) and "," in data:
            print(f"Input: {data}")
            raws = data.split(",")
            if len(raws) != 3:
                raise CSVError("Error detected in Stage 1: "
                               "CSV file must have exactly 3 values.")
            return {"user": raws[0].strip(), "action": raws[1].strip(),
                    "number": raws[2].strip()}
        elif isinstance(data, Iterable):
            print("Input: Real-time sensor stream")
            res = {}
            for i, element in enumerate(data):
                res[i] = element
            return res

    def prove_link(self) -> str:
        return self.__class__.__name__


class TransformStage:
    def process(self, data: Any) -> Any:
        """Check which type of datas it's and adapts behavior.

        Args:
            data (Any): Data to be treated

        Raises:
            JSONError: Raise an error about json dict content
            StreamError: Raise an error about stream dict content

        Returns:
            Any: A dictionnary which contain usefull datas for the next stage
        """
        if data.get("sensor"):
            print("Transform: Enriched with metadata and validation")
            if data.get("sensor") == "temp":
                temp = int(data.get("value"))
                if 20 < temp < 30:
                    return {"value": temp, "result": "Normal"}
                else:
                    return {"value": temp, "result": "Critical"}
            else:
                raise JSONError("Error detected in Stage 2: "
                                "Unknow sensor type")
        elif data.get("user"):
            print("Transform: Parsed and structured data")
            try:
                return {"activity": data.get("action"),
                        "number": data.get("number")}
            except Exception as e:
                print(f"Error detected in Stage 2: {e}")
        elif data.get(1):
            print("Transform: Aggregated and filtered")
            if isinstance(data.get(1), int):
                return {"number": len(data),
                        "avg": round(float(sum(data.values()) / len(data)), 2)}
            else:
                raise StreamError("Error detected in Stage 2: "
                                  "Streams datas must be int (for the moment)")

    def prove_link(self) -> str:
        return self.__class__.__name__


class OutputStage:
    def process(self, data: Any) -> Any:
        """Format a string with the values of the dict passed

        Args:
            data (Any): A dictionnary containing usefull datas of the process

        Returns:
            Any: A string to summarize what happened during process
        """
        if data and data.get("value"):
            return f"Processed temperature reading: {data.get('value')}°C"\
                   f" ({data.get('result')} range)"
        elif data and data.get("activity"):
            return f"User activity logged ({data.get('activity')}):"\
                   f" {data.get('number')} action(s) processed"
        elif data and data.get("avg"):
            return f" Stream summary: {data.get('number')} readings,"\
                   f" avg: {data.get('avg')}°C"

    def prove_link(self) -> str:
        return self.__class__.__name__


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> str | Any:
        print("\nProcessing JSON data through pipeline...")
        return super().process(data)


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> str | Any:
        print("\nProcessing CSV data through pipeline...")
        return super().process(data)


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> str | Any:
        print("\nProcessing Stream data through pipeline...")
        return super().process(data)


class NexusManager:

    def __init__(self) -> None:
        self.pipelines: list[ProcessingPipeline] = []
        self.number_record: int = 0
        self.execution_time: float = 0
        self.number_datas: int = 0
        self.efficiency: int = 0
        print("Creating Data Processing Pipeline...")
        print("Stage 1: Input validation and parsing")
        print("Stage 2: Data transformation and enrichment")
        print("Stage 3: Output formatting and delivery")
        self.add_pipeline(JSONAdapter("JSON_001"))
        self.add_pipeline(CSVAdapter("CSV_001"))
        self.add_pipeline(StreamAdapter("STREAM_001"))

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def process_data(self, datas: Any) -> None:
        """Check on which adapter the datas should be send

        Args:
            datas (Any): A list of differents datas to be processed
        """
        start_time = time.time()
        for data in datas:
            adapter = None
            if isinstance(data, str) and ":" in data:
                adapter = self.pipelines[0]
            elif isinstance(data, str) and "," in data:
                adapter = self.pipelines[1]
            elif isinstance(data, Iterable):
                adapter = self.pipelines[2]
            if adapter:
                try:
                    print("Output:", adapter.process(data))
                    self.number_record += 1
                except DataError as e:
                    print(e)
                    print("Recovery initiated: Switching to backup processor")
                    print("Recovery successful: Pipeline restored,"
                          " processing resumed")
        end_time = time.time()
        self.execution_time += round(float(end_time - start_time), 2)
        self.number_datas += len(datas)
        self.efficiency = int((self.number_record * 100) / self.number_datas)

    def prove_chaining(self) -> None:
        """Call the same function in all adapters in a loop to prove that they
        share the same interface.
        Then do the same with the stages
        """
        for i, pipeline in enumerate(self.pipelines):
            print(pipeline.link_pipeline(), end="")
            if i + 1 < len(self.pipelines):
                print(" -> ", end="")
        print()
        self.pipelines[0].test_prove()
        print(f"\nChain result: {self.number_record} records processed"
              " through 3-stage pipeline")
        print(f"Performance: {self.efficiency}% efficiency,"
              f" {self.execution_time}s total processing time")


def main():
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    datas: list[Any] = [
        '{"sensor": "temp", "value": 23.5, "unit": "C"}',
        "Bruno, login, 4",
        [5, 6, 7, 1, 4, 9],
        "Cedric, error, 1",
        '{"sensor": "pastemp", "value": 30.2, "unit": "C"}',
        [36, 78, 4, 56, 9, 10],
        '{"sensor": "temp", "value": 32.4, "unit": "C"}',
        [7, 85, 52, 4, 64, 25]
    ]
    print("Initializing Nexus Manager...")
    print("Pipeline capacity: 1000 streams/second\n")
    nexus = NexusManager()
    print("\n=== Multi-Format Data Processing ===")
    nexus.process_data(datas)
    print("\n=== Pipeline Chaining Demo ===")
    nexus.prove_chaining()
    print("\nNexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()
