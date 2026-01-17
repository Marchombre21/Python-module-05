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
        print(result)


class NumericProcessor(DataProcessor):
    def process(self, data: int | list[int] | dict[str, int]) -> str:
        pass

    def validate(self, data: int) -> bool:
        pass


class TextProcessor(DataProcessor):
    def process(self, data: str) -> str:
        pass

    def validate(self, data: str) -> bool:
        pass


class LogProcessor(DataProcessor):
    def process(self, data: str) -> str:
        pass

    def validate(self, data: str) -> bool:
        pass


def main():
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    numeric_proc = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    print("Initializing Numeric Processor...")
    numeric_proc.process()

if __name__ == "__main__":
    main()
