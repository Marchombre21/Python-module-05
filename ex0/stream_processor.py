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
    def __init__(self, details: str | None = None):
        message = f"Caught an error: {details}\n"
        super().__init__(message)


class NumericError(DataError):
    pass


class TextError(DataError):
    pass


class LogError(DataError):
    pass


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        """
        Docstring for process

        :param data: Values to be processed
        :type data: Any
        :return: String containing all results after data processing
        :rtype: str
        """
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """
        Docstring for validate

        :param data: Values to be processed
        :type data: Any
        :return: true or false depending on the type of data
        compared to the expected type
        :rtype: bool
        """
        pass

    def format_output(self, result: str) -> str:
        """
        Docstring for format_output

        :param result: String containing a summary of the process results
        :type result: str
        :return: Formated string depending on expected format
        :rtype: str
        """
        return f"Processed {result}"


class NumericProcessor(DataProcessor):
    def process(self, data: int | list[int] | dict[Any, int]) -> str:
        number: int = 0
        sum_val: int = 0
        if type(data) is list:
            for element in data:
                number += 1
                sum_val += element
        elif type(data) is dict:
            for element in data.values():
                number += 1
                sum_val += element
        else:
            number = 1
            sum_val = data
        return f"{number} numeric value(s), sum={sum_val},"\
               f" avg={(sum_val / number):.2f}"

    def validate(self, data: int | list[int] | dict[Any, int]) -> bool:
        if type(data) is list:
            if len(data) < 1:
                raise NumericError("List must have at least one value")
            for element in data:
                if type(element) is not int:
                    raise NumericError("The numeric data must be an int")
        elif type(data) is dict:
            if len(data) < 1:
                raise NumericError("Dict must have at least one value")
            for element in data.values():
                if type(element) is not int:
                    raise NumericError("The numeric data must be an int")
        elif type(data) is not int:
            raise NumericError("The numeric data must be an int")
        return True


class TextProcessor(DataProcessor):
    def process(self, data: str | list[str] | dict[Any, str]) -> str:
        char_nb: int = 0
        word_nb: int = 0
        if type(data) is list:
            for element in data:
                char_nb += len(element)
                word_nb += len(element.split())
        elif type(data) is dict:
            for element in data.values():
                char_nb += len(element)
                word_nb += len(element.split())
        else:
            char_nb = len(data)
            word_nb = len(data.split(" "))
        return f"text: {char_nb} character(s), {word_nb} words"

    def validate(self, data: str | list[str] | dict[Any, str]) -> bool:
        if type(data) is list:
            if len(data) < 1:
                raise TextError("List must contain at least one string")
            for element in data:
                if type(element) is not str:
                    raise TextError("The text data must be a string")
        elif type(data) is dict:
            if len(data) < 1:
                raise TextError("Dict must contain at least one string")
            for element in data.values():
                if type(element) is not str:
                    raise TextError("The text data must be a string")
        elif type(data) is not str:
            raise TextError("The text data must be a string")
        return True


class LogProcessor(DataProcessor):
    def process(self, data: str) -> str:
        return data

    def validate(self, data: str | list[str] | dict[Any, str]) -> bool:
        if type(data) is not str or ":" not in data:
            raise LogError("A log data must have"
                           " '<type error>: <error description>' format")
        else:
            words = data.split(":")
            if not words[0].isupper():
                raise LogError("The type error must be uppercase")
        return True

    def format_output(self, result: str) -> str:
        words = result.split(":")
        begining = ""
        result = words[1].strip()
        if words[0] == "ERROR":
            begining = "[ALERT] ERROR level detected:"
        else:
            begining = f"[{words[0]}] {words[0]} level detected:"
        return f"{begining} {result}"


def main():
    try:
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
        for proc, data, sentence, valid in zip(procs, test1, sentences,
                                               checks):
            print(sentence)
            print(f"Processing data: {data}")
            try:
                if proc.validate(data):
                    print(f"Validation {valid}")
                    result: str = proc.format_output(proc.process(data))
                    print(result)
                    print()
            except DataError as e:
                print(e)
        print("=== Polymorphic Processing Demo ===\n")
        print("Processing multiple data types through same interface...")
        i = 1
        for proc, data in zip(procs, test2):
            try:
                if proc.validate(data):
                    result: str = proc.format_output(proc.process(data))
                    print(f"Result {i}: {result}")
                    i += 1
            except DataError as e:
                print(e)
        print("\nFoundation systems online. Nexus ready for advanced streams.")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
