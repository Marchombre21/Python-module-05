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


class DataError(Exception):
    def __init__(self, details: str | None = None):
        message = f"Caugt an error: {details}"
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
            return e

    def add_stage(self, stage: ProcessingStage):
        self.__stages.append(stage)


class InputStage:
    def process(self, data: Any) -> Any:
        if isinstance(data, str) and ":" in data:
            print(f"Input: {data}")
            try:
                raws = json.loads(data)
                return raws
            except json.JSONDecodeError:
                raise JSONError("JSON file isn't in the correct format")
        elif isinstance(data, str) and "," in data:
            print(f"Input: {data}")
            try:
                raws = data.split(",")
                if len(raws) != 3:
                    raise CSVError("CSV file must have exactly 3 values.")
                return {"user": raws[0].strip(), "action": raws[1].strip(),
                        "number": raws[2].strip()}
            except DataError as e:
                print(e)
        elif isinstance(data, Iterable):
            print("Input: Real-time sensor stream")
            res = {}
            for i, element in enumerate(data):
                res[i] = element
            return res


class TransformStage:
    def process(self, data: Any) -> Any:
        if data.get("sensor"):
            print("Transform: Enriched with metadata and validation")
            if data.get("sensor") == "temp":
                temp = int(data.get("value"))
                if 20 < temp < 30:
                    return {"value": temp, "result": "Normal"}
                else:
                    return {"value": temp, "result": "Critical"}
            else:
                raise JSONError("Unknow sensor type")
        elif data.get("user"):
            print("Transform: Parsed and structured data")
            try:
                return {"activity": data.get("action"),
                        "number": data.get("number")}
            except Exception as e:
                print(e)
        elif data.get(1):
            print("Transform: Aggregated and filtered")
            if isinstance(data.get(1), int):
                return {"number": len(data),
                        "avg": round(float(sum(data.values()) / len(data)), 2)}
            else:
                raise StreamError("Streams datas must be int (for the moment)")


class OutputStage:
    def process(self, data: Any) -> Any:
        if data and data.get("value"):
            return f"Processed temperature reading: {data.get('value')}°C"\
                   f" ({data.get('result')} range)"
        elif data and data.get("activity"):
            return f"User activity logged ({data.get('activity')}):"\
                   f" {data.get('number')} action(s) processed"
        elif data and data.get("avg"):
            return f" Stream summary: {data.get('number')} readings,"\
                   f" avg: {data.get('avg')}°C"


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__()
        self.__id = pipeline_id
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> str | Any:
        print("\nProcessing JSON data through pipeline...")
        return super().process(data)


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__()
        self.__id = pipeline_id
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> str | Any:
        print("\nProcessing CSV data through pipeline...")
        return super().process(data)


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__()
        self.__id = pipeline_id
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> str | Any:
        print("\nProcessing Stream data through pipeline...")
        return super().process(data)


class NexusManager:
    pipelines: list[ProcessingPipeline] = []

    def __init__(self):
        print("Creating Data Processing Pipeline...")
        print("Stage 1: Input validation and parsing")
        print("Stage 2: Data transformation and enrichment")
        print("Stage 3: Output formatting and delivery")
        self.add_pipeline(JSONAdapter("JSON_001"))
        self.add_pipeline(CSVAdapter("CSV_001"))
        self.add_pipeline(StreamAdapter("STREAM_001"))

    def add_pipeline(self, pipeline: ProcessingPipeline):
        self.pipelines.append(pipeline)

    def process_data(self, datas: Any):
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
                except DataError as e:
                    print(e)


def main():
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    datas = [
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


if __name__ == "__main__":
    main()
