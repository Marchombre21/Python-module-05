# ****************************************************************************#
#                                                                             #
#                                                         :::      ::::::::   #
#    stream_processor.py                                :+:      :+:    :+:   #
#                                                     +:+ +:+         +:+     #
#    By: bfitte <bfitte@student.42lyon.fr>          +#+  +:+       +#+        #
#                                                 +#+#+#+#+#+   +#+           #
#    Created: 2026/01/17 10:50:54 by bfitte            #+#    #+#             #
#    Updated: 2026/01/17 10:50:55 by bfitte           ###   ########lyon.fr   #
#                                                                             #
# ****************************************************************************#

from abc import ABC, abstractmethod
from typing import Any


class DataError(Exception):
    def __init__(self, details: str = None):
        message = f"Caught an error: {details}\n"
        super().__init__(message)


class SensorError(DataError):
    pass


# class TextError(DataError):
#     pass


# class LogError(DataError):
#     pass


class DataStream(ABC):
    @abstractmethod
    def process_batch(self, data_batch: list[Any]) -> str:
        pass

    def filter_data(self, data_batch: list[Any],
                    criteria: str | None) -> list[Any]:
        return data_batch

    def get_stats(self) -> dict[str, str | int | float]:
        return {}


class SensorStream(DataStream):
    def process_batch(self, data_batch: list[tuple]) -> str:
        # Mettre systeme de calculs de valeurs de tuple (temp,
        # humidity et pressure)
        pass

    def filter_data(self, data_batch: list[Any],
                    criteria: str | None) -> list[tuple]:
        clean_datas = []
        for datas in data_batch:
            is_valid = True
            try:
                if not isinstance(datas, tuple):
                    is_valid = False
                    raise SensorError("All sensor datas must be tuples")
                for data in datas:
                    is_valid = False
                    if not isinstance(data, float):
                        raise SensorError("All sensor datas must be float")
            except SensorError as e:
                print(e)
            if is_valid:
                clean_datas.append(datas)
        if criteria and isinstance(criteria, str) and len(criteria) > 1:
            try:
                if criteria[0] == ">":
                    return [data for data in clean_datas if data[0] >
                            float(criteria[1:])]
                elif criteria[0] == "<":
                    return [data for data in clean_datas if data[0] <
                            float(criteria[1:])]
                else:
                    raise SensorError("Sensor filter must be in"
                                      " '<sup/inf> <float>' format")
            except (ValueError, DataError) as e:
                print(e)
        return super().filter_data(clean_datas, criteria)


# class TextProcessor(DataProcessor):
#     def process(self, data: str | list[str] | dict[Any, str]) -> str:
#         char_nb: int = 0
#         word_nb: int = 0
#         if type(data) is list:
#             for element in data:
#                 char_nb += len(element)
#                 word_nb += len(element.split())
#         elif type(data) is dict:
#             for element in data.values():
#                 char_nb += len(element)
#                 word_nb += len(element.split())
#         else:
#             char_nb = len(data)
#             word_nb = len(data.split(" "))
#         return f"text: {char_nb} character(s), {word_nb} words"

#     def validate(self, data:  str | list[str] | dict[Any, str]) -> bool:
#         if type(data) is list:
#             if len(data) < 1:
#                 raise TextError("List must contain at least one string")
#             for element in data:
#                 if type(element) is not str:
#                     raise TextError("The text data must be a string")
#         elif type(data) is dict:
#             if len(data) < 1:
#                 raise TextError("Dict must contain at least one string")
#             for element in data.values():
#                 if type(element) is not str:
#                     raise TextError("The text data must be a string")
#         elif type(data) is not str:
#             raise TextError("The text data must be a string")
#         return True


# class LogProcessor(DataProcessor):
#     def process(self, data: str) -> str:
#         return data

#     def validate(self, data: str) -> bool:
#         if type(data) is not str or ":" not in data:
#             raise LogError("A log data must have"
#                            " '<type error>: <error description>' format")
#         else:
#             words = data.split(":")
#             if not words[0].isupper():
#                 raise LogError("The type error must be uppercase")
#         return True

#     def format_output(self, result: str) -> str:
#         words = result.split(":")
#         begining = ""
#         result = words[1].strip()
#         if words[0] == "ERROR":
#             begining = "[ALERT] ERROR level detected:"
#         else:
#             begining = f"[{words[0]}] {words[0]} level detected:"
#         return f"{begining} {result}"


def main():
    pass
#     try:
#         sentences = [
#             "Initializing Numeric Processor...",
#             "Initializing Text Processor...",
#             "Initializing Log Processor..."
#         ]
#         checks = [
#             "Numeric data verified",
#             "Text data verified",
#             "Log entry verified"
#         ]
#         test1 = [
#             [1, 2, 3, 4, 5],
#             "Je suis Bruno",
#             "ERROR: Segfault!!"
#         ]
#         test2 = [
#             {1: 5, 2: 48, 3: 4, 4: 78, 5: 89},
#             ["Je", "suis", "bruno"],
#             "INFO: All is good."
#         ]
#         print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
#         numeric_proc = NumericProcessor()
#         text_proc = TextProcessor()
#         log_proc = LogProcessor()
#         procs: list[DataProcessor] = [numeric_proc, text_proc, log_proc]
#         for proc, data, sentence, valid in zip(procs, test1, sentences,
#                                                checks):
#             print(sentence)
#             print(f"Processing data: {data}")
#             try:
#                 if proc.validate(data):
#                     print(f"Validation {valid}")
#                     result: str = proc.format_output(proc.process(data))
#                     print(result)
#                     print()
#             except DataError as e:
#                 print(e)
#         print("=== Polymorphic Processing Demo ===\n")
#         print("Processing multiple data types through same interface...")
#         i = 1
#         for proc, data in zip(procs, test2):
#             try:
#                 if proc.validate(data):
#                     result: str = proc.format_output(proc.process(data))
#                     print(f"Result {i}: {result}")
#                     i += 1
#             except DataError as e:
#                 print(e)
#     except Exception as e:
#         print(e)


if __name__ == "__main__":
    main()
