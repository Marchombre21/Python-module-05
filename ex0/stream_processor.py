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


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        begining = "Processed"
        if ":" in result and result[:4] != "text":
            words = result.split(":")
            if words[0] == "ERROR":
                begining = "[ALERT] ERROR level detected:"
                result = words[1].strip()
            else:
                begining = f"[{words[0]}] {words[0]} level detected:"
                result = words[1].strip()
        return f"{begining} {result}"


class NumericProcessor(DataProcessor):
    def process(self, data: int | list[int] | dict[Any, int]) -> str:
        number: int = 0
        sum: int = 0
        if self.validate(data):
            if type(data) is list:
                for element in data:
                    number += 1
                    sum += element
            elif type(data) is dict:
                for element in data.values():
                    number += 1
                    sum += element
            else:
                number = 1
                sum = data
            return f"{number} numeric value(s), sum={sum},"\
                   f" avg={(sum / number):.2f}"

    def validate(self, data: int | list[int] | dict[Any, int]) -> bool:
        if type(data) is list:
            for element in data:
                if type(element) is not int:
                    return False
        elif type(data) is dict:
            for element in data.values():
                if type(element) is not int:
                    return False
        elif type(data) is not int:
            return False
        return True


class TextProcessor(DataProcessor):
    def process(self, data:  str | list[str] | dict[Any, str]) -> str:
        char_nb: int = 0
        word_nb: int = 0
        if self.validate(data):
            if type(data) is list:
                for element in data:
                    char_nb += len(element)
                    word_nb += len(element.split(" "))
            elif type(data) is dict:
                for element in data.values():
                    char_nb += len(element)
                    word_nb += len(element.split(" "))
            else:
                char_nb = len(data)
                word_nb = len(data.split(" "))
            return f"text: {char_nb} character(s), {word_nb} words"

    def validate(self, data:  str | list[str] | dict[Any, str]) -> bool:
        if type(data) is list:
            for element in data:
                if type(element) is not str:
                    return False
        elif type(data) is dict:
            for element in data.values():
                if type(element) is not str:
                    return False
        elif type(data) is not str:
            return False
        return True


class LogProcessor(DataProcessor):
    def process(self, data: str) -> str:
        return data

    def validate(self, data: str) -> bool:
        if type(data) is not str or ":" not in data:
            return False
        else:
            words = data.split(":")
            if not words[0].isupper():
                return False
        return True


def main():
    sentences = [
        "Initializing Numeric Processor...",
        "Initializing Text Processor...",
        "Initializing Log Processor..."
    ]
    checks = [
        "Numeric data verified",
        "Text data verified",
        "Log entry verified"
    ]
    test1 = [
        [1, 2, 3, 4, 5],
        "Je suis Bruno",
        "ERROR: Segfault!!"
    ]
    test2 = [
        {1: 5, 2: 48, 3: 4, 4: 78, 5: 89},
        ["Je", "suis", "bruno"],
        "INFO: All is good."
    ]
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    numeric_proc = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    procs: list[DataProcessor] = [numeric_proc, text_proc, log_proc]
    for proc, data, sentence, valid in zip(procs, test1, sentences, checks):
        print(sentence)
        print(f"Processing data: {data}")
        if proc.validate(data):
            print(f"Validation {valid}")
            result: str = proc.format_output(proc.process(data))
            print(result)
            print()
    print("=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...")
    i = 1
    for proc, data in zip(procs, test2):
        if proc.validate(data):
            result: str = proc.format_output(proc.process(data))
            print(f"Result {i}: {result}")
            i += 1
    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
