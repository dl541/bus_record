import dataclasses
import time
import logging
import urllib3
from concurrent.futures import ThreadPoolExecutor
import random


@dataclasses.dataclass(frozen=True)
class Bus:
    route: str
    direction: str
    service_type: int


BUSES = (
    Bus("NA33", "I", "1"),
    Bus("A33X", "O", "1"),
    Bus("A33X", "O", "2"),
    Bus("A33X", "O", "4"),
    Bus("A36", "O", "1"),
    Bus("A36", "O", "2"),
    Bus("A36", "O", "3"),
    Bus("A36", "O", "5"),
)


def getRouteInfoURL(bus: Bus) -> str:
    return f"https://data.etabus.gov.hk/v1/transport/kmb/route/{bus.route}/{bus.direction}/{bus.service_type}"


def getStopInfoURL(stop_id: str) -> str:
    return f"https://data.etabus.gov.hk/v1/transport/kmb/stop/{stop_id}"


def getRouteETAURL(bus: Bus) -> str:
    return f"https://data.etabus.gov.hk/v1/transport/kmb/route-eta/{bus.route}/{bus.service_type}"


def getHistoryFileName(bus: Bus) -> str:
    return f"history/{bus.route}-{bus.service_type}-{getFormattedCurrentTime()}"


def getFormattedCurrentTime() -> str:
    return time.strftime("%Y-%m-%d-%X-%Z")


def setupLogging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(f"debug/debug-{getFormattedCurrentTime()}.log"),
            logging.StreamHandler(),
        ],
    )
    logFormatter = logging.Formatter(
        "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
    )


def scrapBusInfo(bus: Bus):
    while True:
        logging.info(f"Request bus info. {bus}")
        try:
            resp = urllib3.request("GET", getRouteETAURL(bus), retries=False)
            if resp.status != 200:
                logging.warning(f"Request failed. Body: {resp.data.decode()}")
                continue

            res: str = resp.data.decode()
            logging.info(f"Response received for {bus}")
            with open(getHistoryFileName(bus), "w") as f:
                f.write(res)
            logging.info(f"Wrote to file {getHistoryFileName(bus)}")

        except Exception as e:
            logging.error(f"Exception: {e}")

        time.sleep(random.randint(65, 75))  # in seconds


if __name__ == "__main__":
    setupLogging()
    with ThreadPoolExecutor(max_workers=len(BUSES)) as executor:
        executor.map(scrapBusInfo, BUSES)
