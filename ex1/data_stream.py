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


class FinancialError(DataError):
    pass


class EventError(DataError):
    pass


class DataStream(ABC):
    @abstractmethod
    def process_batch(self, data_batch: list[Any]) -> str:
        """This function process all datas passed in the batch

        Args:
            data_batch (list[Any]): A list which contain datas of any types.

        Returns:
            str: Return a string which summarize the results
        """
        pass

    def filter_data(self, data_batch: list[Any],
                    criteria: str | None) -> list[Any]:
        """check if datas are in the wanted type and
        filter datas according to criteria passed

        Args:
            data_batch (list[Any]): A list which contain datas of any types.
            criteria (str | None): The criteria should be used to filter datas

        Returns:
            list[Any]: Return a list of the needed type
        """
        return data_batch

    def get_stats(self) -> dict[str, str | int | float]:
        """Get stats of the instance.

        Returns:
            dict[str, str | int | float]: Return a dict which contain datas of
            the object in keys
        """
        return {}


class SensorStream(DataStream):
    def __init__(self, id: str):
        self.__total_temp = 0
        self.__total_humidity = 0
        self.__total_pressure = 0
        self.__total_ope = 0
        self.id = id
        self.type = "Environmental Data"
        self.__errors = []

    def process_batch(self, data_batch: list[tuple]) -> str:
        total_temp = 0
        total_ope = 0
        print("\nInitializing Sensor Stream...")
        print(f"Stream ID: {self.id}, Type: Environmental Data")
        for tup in data_batch:
            t, h, p = tup
            print(f"Processing sensor batch: [temp: {t}, humidity: {h},"
                  f" pressure: {p}]")
            total_temp += t
            total_ope += 1
            self.__total_humidity += h
            self.__total_pressure += p
            self.__total_temp += t
            self.__total_ope += 1
        return f"Sensor analysis: {total_ope} readings processed,"\
               f" avg temp: {round(float(total_temp / total_ope), 2)}°C"

    def filter_data(self, data_batch: list[Any],
                    criteria: str | None) -> list[tuple]:
        clean_datas = []
        self.__errors = []
        for datas in data_batch:
            is_valid = True
            try:
                if not isinstance(datas, tuple):
                    is_valid = False
                    raise SensorError("All sensor datas must be tuples")
                for data in datas:
                    if not isinstance(data, float):
                        is_valid = False
                        raise SensorError("All sensor datas must be float")
            except SensorError as e:
                print(e)
            if is_valid:
                clean_datas.append(datas)
        if criteria and isinstance(criteria, str) and len(criteria) > 1:
            try:
                if criteria[0] == ">":
                    self.__errors.append([data for data in clean_datas
                                          if data[0] < float(criteria[1:])])
                    return [data for data in clean_datas if data[0] >
                            float(criteria[1:])]
                elif criteria[0] == "<":
                    self.__errors.append([data for data in clean_datas
                                          if data[0] > float(criteria[1:])])
                    return [data for data in clean_datas if data[0] <
                            float(criteria[1:])]
                else:
                    raise SensorError("Sensor filter must be in"
                                      " '<sup/inf> <float>' format")
            except (ValueError, DataError) as e:
                print(e)
        return super().filter_data(clean_datas, criteria)

    def get_stats(self) -> dict[str, float]:
        avg_temp = round(float(self.__total_temp / self.__total_ope), 2)
        avg_hum = round(float(self.__total_humidity / self.__total_ope), 2)
        avg_pre = round(float(self.__total_pressure / self.__total_ope), 2)
        return {
            "avg_temp": avg_temp,
            "avg_hum": avg_hum,
            "avg_pre": avg_pre,
            "total_operations": self.__total_ope,
            "errors": self.__errors
        }


class TransactionStream(DataStream):
    def __init__(self, id: str):
        self.__total_sell_value = 0
        self.__total_sell = 0
        self.__total_buy_value = 0
        self.__total_buy = 0
        self.__total_ope = 0
        self.id = id
        self.type = "Financial Data"
        self.__errors = []

    def process_batch(self, data_batch: list[int]) -> str:
        print("\nInitializing Transaction Stream...")
        print(f"Stream ID: {self.id}, Type: Financial Data")
        total_sell = 0
        total_buy = 0
        total_ope = 0
        sign = ""
        message = ""
        for ope in data_batch:
            self.__total_ope += 1
            total_ope += 1
            if ope < 0:
                if total_ope < 4:
                    message += f"buy:{ope}, "
                total_buy += ope
                self.__total_buy += ope
                self.__total_buy_value += 1
            if ope > 0:
                if total_ope < 4:
                    message += f"sell:{ope}, "
                total_sell += ope
                self.__total_sell += ope
                self.__total_sell_value += 1
        if total_sell - total_buy > 0:
            sign = "+"
        print(f"Processing transaction batch: [{message}...]")
        return f"Transaction analysis: {total_ope} operations,"\
               f" net flow: {sign}{total_sell + total_buy} units"

    def filter_data(self, data_batch: list[Any],
                    criteria: str | None) -> list[int]:
        clean_datas = []
        self.__errors = []
        for datas in data_batch:
            is_valid = True
            try:
                if not isinstance(datas, list):
                    is_valid = False
                    raise FinancialError("All financial datas must "
                                         "be lists of int")
                for data in datas:
                    if not isinstance(data, int):
                        is_valid = False
                        raise FinancialError("All financial data must be int")
            except DataError as e:
                print(e)
            if is_valid:
                clean_datas.append(datas)
        if criteria and isinstance(criteria, str) and len(criteria) > 1:
            try:
                if criteria[0] == ">":
                    self.__errors.append([data for datas in clean_datas for
                                         data in datas if data <
                                         int(criteria[1:])])
                    return [data for datas in clean_datas for data in datas
                            if data > int(criteria[1:])]
                elif criteria[0] == "<":
                    self.__errors.append([data for datas in clean_datas for
                                         data in datas if data >
                                         int(criteria[1:])])
                    return [data for datas in clean_datas for data in datas
                            if data < int(criteria[1:])]
                else:
                    raise FinancialError("Financial filter must be in"
                                         " '<sup/inf> <int>' format")
            except (ValueError, DataError) as e:
                print(e)
        return super().filter_data([data for datas in clean_datas for data
                                    in datas], criteria)

    def get_stats(self) -> dict[str, int]:
        avg_sell = round(self.__total_sell / self.__total_sell_value, 2) if\
                   self.__total_sell_value > 0 else 0
        avg_buy = round(self.__total_buy / self.__total_buy_value, 2) if\
            self.__total_buy_value > 0 else 0
        final_result = self.__total_sell + self.__total_buy
        return {
            "avg_sell": avg_sell,
            "avg_buy": avg_buy,
            "final_result": final_result,
            "total": self.__total_ope,
            "errors": self.__errors
        }


class EventStream(DataStream):
    def __init__(self, id: str):
        self.__total_login = 0
        self.__total_logout = 0
        self.__total_error = 0
        self.__total_ope = 0
        self.id = id
        self.__errors = []

    def process_batch(self, data_batch: list[str]) -> str:
        print("\nInitializing Event Stream...")
        print(f"Stream ID: {self.id}, Type: System Events")
        total_errors = 0
        total_ope = 0
        print(f"Processing event batch: {data_batch[:3]} ...")
        for ope in data_batch:
            self.__total_ope += 1
            total_ope += 1
            match ope:
                case "login":
                    self.__total_login += 1
                case "logout":
                    self.__total_logout += 1
                case "error":
                    self.__total_error += 1
                    total_errors += 1
        return f"Event analysis: {total_ope} event(s), {total_errors}"\
               "error(s) detected"

    def filter_data(self, data_batch: list[Any],
                    criteria: str | None) -> list[str]:
        clean_datas = []
        self.__errors = []
        for datas in data_batch:
            is_valid = True
            try:
                if not isinstance(datas, list):
                    is_valid = False
                    raise EventError("All event datas must "
                                     "be a list of strings")
                for data in datas:
                    if not isinstance(data, str):
                        is_valid = False
                        raise EventError("All event data must be a string")
            except DataError as e:
                print(e)
            if is_valid:
                clean_datas.append(datas)
        if criteria and isinstance(criteria, str):
            try:
                if criteria.lower() in ["error", "logout", "login"]:
                    self.__errors.append([data for datas in clean_datas for
                                         data in datas if data !=
                                         criteria.lower()])
                    return [data for datas in clean_datas for data in datas
                            if data == criteria.lower()]
                else:
                    raise EventError("Event filter must be in '<event>'"
                                     " format")
            except (ValueError, DataError) as e:
                print(e)
        return super().filter_data([data for datas in clean_datas for data
                                    in datas], criteria)

    def get_stats(self) -> dict[str, int]:
        total_errors = self.__total_error
        total_login = self.__total_login
        total_logout = self.__total_logout
        return {
            "total_errors": total_errors,
            "total_login": total_login,
            "total_logout": total_logout,
            "total": self.__total_ope,
            "errors": self.__errors
        }


class StreamProcessor:
    def __init__(self):
        self.__batch = 0
        self.sensor = SensorStream("SENSOR_001")
        self.trans = TransactionStream("TRANS_001")
        self.event = EventStream("EVENT_001")

    def dispatch_sensors(self, datas_batch: list, criteria_one: str | None,
                         criteria_two: str | None, criteria_three: str | None):
        sensor_list: list[tuple] = []
        trans_list: list[int] = []
        event_list: list[str] = []
        self.__batch += 1
        try:
            if not isinstance(datas_batch, list):
                raise DataError("Batchs must be a list")
            for datas in datas_batch:
                if isinstance(datas, tuple):
                    sensor_list.append(datas)
                elif isinstance(datas, list):
                    if isinstance(datas[0], int):
                        trans_list.append(datas)
                    elif isinstance(datas[0], str):
                        event_list.append(datas)
                else:
                    raise DataError("Batch's elements must be either tuple,"
                                    " list of int or list of string,"
                                    " nothing else!")

            list_lists: list[list] = [sensor_list, trans_list, event_list]
            criters_list: list[str] = [criteria_one, criteria_two,
                                       criteria_three]
            streams_lists: list[DataStream] = [self.sensor, self.trans,
                                               self.event]
            for arrays, streams, criter in zip(list_lists, streams_lists,
                                               criters_list):
                if len(arrays):
                    print(streams.process_batch(
                        streams.filter_data(arrays, criter)
                        ))
            #         ))
            dict_sensor = self.sensor.get_stats()
            dict_trans = self.trans.get_stats()
            dict_event = self.event.get_stats()
            print("\n=== Polymorphic Stream Processing ===\n")
            print("Processing mixed stream types through unified"
                  " interface...\n")
            print(f"Batch {self.__batch} Results:")
            print("\n===Sensor datas===\n")
            print(f"{dict_sensor['total_operations']} readings processed,"
                  f" avg temp: {dict_sensor['avg_temp']}°C, avg hum:"
                  f" {dict_sensor['avg_hum']}, avg pressure:"
                  f" {dict_sensor['avg_pre']}")
            print("\n===Transactions datas===\n")
            print(f"{dict_trans['total']} operations processed,"
                  f" avg sell: {dict_trans['avg_sell']}$, avg buy:"
                  f" {dict_trans['avg_buy']}$, final result:"
                  f" {dict_trans['final_result']}$")
            print("\n===Events datas===\n")
            print(f"{dict_event['total']} events processed,"
                  f" total login : {dict_event['total_login']}, total logout:"
                  f" {dict_event['total_logout']}, total errors:"
                  f" {dict_event['total_errors']}")
            print("\nStream filtering active: High-priority data only")
            height = ""
            if criteria_one and criteria_one[0] == "<":
                height = "large"
            elif criteria_one and criteria_one[0] == ">":
                height = "little"
            print(f"Filtered results: {len(dict_sensor['errors'])} critical"
                  f" sensor alerts, {len(dict_trans['errors'])} {height}"
                  f" transaction(s) and {len(dict_event['errors'])} event"
                  " alert(s)")
        except DataError as e:
            print(e)


def main():
    batch = [
        (22.5, 78.2, 37.4),
        [15, 150, 890],
        ["login", "logout", "login"],
        [69, 450, 935, -502],
        [752, 532, -2420, 632],
        ["error", "logout", "login", "error"],
        [75, 451, 325, -40],
        ["error", "logout", "login", "error"],
        (25.7, 67.3, 43.2),
        ["logout", "logout", "login", "error", "login"],
        (32.1, 70.1, 39.6)
    ]
    batch2 = [
        (27.5, 24.2, 43.4),
        [25, 1500, 90, 150],
        ["login", "logout", "error", "login"],
        [69, -450, 1935, -502, 32],
        [732, 832, -220, 432],
        ["login", "logout", "login", "error", "error"],
        [745, 41, 325, -40, 65],
        ["error", "logout", "login", "error"],
        (23.7, 89.3, 78.2),
        ["logout", "logout", "login", "error", "login"],
        (32.1, 70.1, 39.6)
    ]
    processor = StreamProcessor()
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")
    processor.dispatch_sensors(batch, "<30", "<900", None)
    processor.dispatch_sensors(batch2, None, None, None)


if __name__ == "__main__":
    main()
