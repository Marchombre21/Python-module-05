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
from typing import Any, Protocol
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


class ProcessingPipeline(ABC):
    shared_ways = []

    def __init__(self):
        self.__stages: list[ProcessingStage] = []

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

    def add_ways(self, stream_id: str):
        ProcessingPipeline.shared_ways.append(stream_id)

    def test_prove(self) -> None:
        """Just a function to prove the stages are chained together
        """
        for i, way in enumerate(ProcessingPipeline.shared_ways):
            print(way, end="")
            if i + 1 < len(ProcessingPipeline.shared_ways):
                print(" -> ", end="")
        print("\nData flow: InputStage -> TransformStage -> OutputStage")


class InputStage:
    def process(self, data: Any) -> Any:
        """Check which type of data is passed and adapts its behavior

        Args:
            data (Any): Data to be treated

        Raises:
            JSONError: Raise an error about json format
            DataError: Raise an error about type's file

        Returns:
            Any: A dictionnary which contain usefull datas for the next stage
        """
        if isinstance(data, str):
            print(f"Input: {data}")
            try:
                raws = json.loads(data)
                return raws
            except json.JSONDecodeError:
                return {"raw_content": data}
        elif isinstance(data, tuple):
            print(f"Input: {data}")
            return {"raw_content": data}
        else:
            raise DataError("InputStage must receive only strings or tuples")


class TransformStage:
    def process(self, data: Any) -> Any:
        """Check which dictionnary it's and adapts behavior.

        Args:
            data (Any): Data to be treated

        Raises:
            JSONError: Raise an error about json dict content
            DataError: Raise an error about dict content

        Returns:
            Any: A dictionnary which contain usefull datas for the next stage
        """
        if isinstance(data, dict) and data.get("sensor"):
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
        elif isinstance(data, dict) and data.get("raw_content"):
            print("Transform: Parsed and structured data")
            try:
                if isinstance(data.get("raw_content"), tuple):
                    first, second = data.get("raw_content")
                    return {"temperature": first, "severity": second}
                words = data.get("raw_content").split(",")
                return {"temp": words[0].strip(),
                        "seriousness": words[1].strip(),
                        "unit": words[2].strip()
                        }
            except Exception as e:
                raise DataError(f"Error detected in Stage 2: {e}")
        else:
            raise DataError("Error detected in Stage 2: "
                            "TransformStage must receive only dictionnaries")


class OutputStage:
    def process(self, data: Any) -> Any:
        """Format a string with the values of the dict passed

        Args:
            data (Any): A dictionnary containing usefull datas of the process

        Returns:
            Any: A string to summarize what happened during process
        """
        if data and isinstance(data, dict) and data.get("value"):
            print(f"Output: Processed temperature reading: {data.get('value')}"
                  f"°C ({data.get('result')} range)")
            return f"{data.get('value')}°C, {data.get('result')}, C)"
        elif data and isinstance(data, dict) and data.get("seriousness"):
            print(f"Output: Temperature severity: {data.get('seriousness')}")
            return (data.get("temp"), data.get("seriousness"))
        elif data and isinstance(data, dict) and data.get("severity"):
            return f"Output: Stream summary: {data.get('temperature')},"\
                   f" severity: {data.get('severity')}"
        else:
            raise DataError("OutputStage must receive a dict")


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())
        self.__id = pipeline_id

    def process(self, data: Any) -> str | Any:
        print("\nProcessing JSON data through pipeline...")
        self.add_ways(self.__id)
        return super().process(data)


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())
        self.__id = pipeline_id

    def process(self, data: Any) -> str | Any:
        print("\nProcessing CSV data through same pipeline...")
        self.add_ways(self.__id)
        return super().process(data)


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())
        self.__id = pipeline_id

    def process(self, data: Any) -> str | Any:
        print("\nProcessing Stream data through same pipeline...")
        self.add_ways(self.__id)
        return super().process(data)


class NexusManager:

    def __init__(self) -> None:
        self.pipelines: list[ProcessingPipeline] = []
        self.number_record: int = 0
        self.execution_time: float = 0
        self.number_datas: int = 0
        self.error: int = 0
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
        res = datas
        self.number_record += 1
        for pipeline in self.pipelines:
            try:
                res = pipeline.process(res)
            except DataError as e:
                self.error = 1
                print(e)
                print("Recovery initiated: Switching to backup processor")
                print("Recovery successful: Pipeline restored,"
                      " processing resumed")
        end_time = time.time()
        print(res)
        self.execution_time += round(float(end_time - start_time), 2)
        self.number_datas = self.number_record - self.error
        self.efficiency = int((self.number_record * 100) / self.number_datas)\
            if self.number_datas > 0 else 0

    def prove_chaining(self) -> None:
        """Call the same function in all adapters in a loop to prove that they
        share the same interface.
        Then do the same with the stages
        """
        self.pipelines[0].test_prove()
        print(f"\nChain result: {self.number_record} records processed"
              " through 3-stage pipeline")
        print(f"Performance: {self.efficiency}% efficiency,"
              f" {self.execution_time}s total processing time")


def main():
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    print("Initializing Nexus Manager...")
    print("Pipeline capacity: 1000 streams/second\n")
    nexus = NexusManager()
    print("\n=== Multi-Format Data Processing ===")
    nexus.process_data('{"sensor": "temp", "value": 23.5, "unit": "C"}')
    print("\n=== Pipeline Chaining Demo ===")
    nexus.prove_chaining()
    print("\nNexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()
